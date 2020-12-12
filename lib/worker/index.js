/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _events = require('../events');
const _history = require('../history');
const _peers = require('../peers');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const {extend: extendBlockchain} = require('./blockchain');
const logger = require('../logger');
const {merge} = require('./merge');
const {runGossipCycle, sendNotification} = require('./gossip');
const {BedrockError} = bedrock.util;
const EventWriter = require('./EventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

// load config defaults
require('../config');

// module API
const api = {};
module.exports = api;

// exposed for testing
api.EventWriter = EventWriter;
api.merge = merge;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('../temporaryLedgerNodeMeta');

api.scheduleWork = async ({session}) => {
  // start a consensus session for ledgers
  const maxAge =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  return session.start({fn: _guardedSync, maxAge});
};

// Note: exposed for testing
// this gives test worker approx 1 second per cycle
api._run = async ({ledgerNode, targetCycles = 1}) => {
  return _sync({
    session: {ledgerNode},
    halt() {
      // don't halt before pipeline is run once; this is safe when
      // `runPipelineOnlyOnce` is set and there is no ledger work session
      // scheduler being used that might concurrently schedule another session,
      // which is true in tests
      return false;
    },
    // force pipeline to run a certain number of times
    targetCycles
  });
};

async function _guardedSync(session) {
  // do not allow sync until `waitUntil` time
  const meta = await _ledgerNodeMeta.get(session.ledgerNode.id);
  const waitUntil = _.get(meta, 'consensus-continuity.waitUntil');
  if(waitUntil && waitUntil > Date.now()) {
    // do not run consensus yet
    logger.verbose('consensus job delaying until ' + new Date(waitUntil),
      {ledgerNodeId: session.ledgerNode.id, waitUntil});
    return;
  }
  // ready to run consensus
  return _sync({
    session,
    halt() {
      // expire session early, with 5 second buffer for overruns
      return session.timeRemaining() < 5000;
    }
  });
}

async function _sync({session, halt, targetCycles = -1}) {
  const {ledgerNode} = session;
  const ledgerNodeId = ledgerNode.id;

  logger.verbose('consensus job running', {ledgerNodeId});

  // listen for new local operation notifications
  let resume = () => {};
  const subscriber = new cache.Client().client;
  try {
    await subscriber.subscribe(`continuity2017|needsMerge|${ledgerNodeId}`);
    subscriber.on('message', () => resume());
  } catch(e) {
    subscriber.quit();
    logger.verbose(
      'Work session failed, could not subscribe to new pub/sub messages.',
      {session: session.id});
    return;
  }

  try {
    /* This work session is designed with the assumption that any attempt to
    write to the database or the cache will be successful or an error will
    be thrown canceling the work session. This allows for the cache and
    database to be brought into sync at the start of the work session, without
    having to worry about them getting out of sync in the middle of it. It
    also provides a more simple model for reasoning about correctness and
    potential errors. */

    // ensure cache and mongo are in sync
    const creatorId = (await _peers.get({ledgerNodeId})).id;
    await _validateCache({ledgerNode, creatorId});

    const peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
    const eventWriter = new EventWriter({ledgerNode});
    const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
    let needsGossip = false;

    // commit any previously cached events to the database that could
    // not be written before because gossip timed out; this step ensures
    // valid pending events are always written
    if(!halt()) {
      await eventWriter.write();
    }

    // run consensus/gossip/merge pipeline until work session expires
    // or until the pipeline is run at least once per request
    const savedState = {};
    let cycles = 0;
    if(targetCycles > 0) {
      const _halt = halt;
      halt = () => _halt() || cycles >= targetCycles;
    }
    while(!halt()) {
      // 1. extend blockchain until can't anymore
      const {blocks, priorityPeers, witnesses, blockHeight} =
        await extendBlockchain({ledgerNode, savedState, halt});

      // work session expired
      if(halt()) {
        break;
      }

      // if blocks were created, reset `needsGossip`; to be set again by
      // `merge`
      if(blocks > 0) {
        needsGossip = false;
      }

      // 2. run gossip cycle; the gossip cycle runs an internal loop against
      // selections of peers and it will loop:
      //   until >= 1 merge events received, if `needsGossip=true`;
      //   once, if `needsGossip=false`
      const {mergeEventsReceived} = await runGossipCycle({
        ledgerNode, priorityPeers, creatorId, peerSelector,
        needsGossip, witnesses, blockHeight, halt
      });

      // work session expired
      if(halt()) {
        break;
      }

      // 3. commit all cached events to mongo
      await eventWriter.write();

      // 4. merge if possible
      const {merged, status: mergeStatus} = await merge({
        ledgerNode, creatorId, priorityPeers, witnesses,
        basisBlockHeight: blockHeight - 1, halt
      });
      // keep track of whether a merge would happen if more peer events were
      // received via gossip
      needsGossip = mergeStatus.needsGossip;

      // determine if peers need to be notified of new events
      let notify;
      if(merged || mergeEventsReceived) {
        await _cache.gossip.notifyFlag({add: true, ledgerNodeId});
        notify = true;
      } else {
        notify = (await _cache.gossip.notifyFlag({ledgerNodeId})) !== null;
      }

      if(notify) {
        // notify peers of new/previous merge event(s)
        try {
          // FIXME: notify more than just the `priorityPeers`
          await sendNotification({creatorId, priorityPeers, peerSelector});
        } catch(e) {
          // just log the error, another attempt will be made on the next cycle
          logger.error(
            'An error occurred while attempting to send merge notification.',
            {error: e});
        }
      }

      // work session expired
      if(halt()) {
        break;
      }

      // if there are no outstanding operations (this includes configurations)
      // need to achieve consensus, then delay for cool down period or until a
      // peer notification or a local operation notification comes in
      if(!mergeStatus.hasOutstandingOperations) {
        await new Promise(resolve => {
          resume = resolve;
          setTimeout(resolve, coolDownPeriod);
        });
        // FIXME: if, after cool down, there is still nothing to do, should
        // we end the work session and let the scheduler take it from there?
      }

      // track pipeline runs
      cycles++;
    }
  } finally {
    // unsubscribe from new operation messages
    subscriber.quit();
    logger.verbose('Work session completed.', {session: session.id});
  }
}

async function _validateCache({ledgerNode, creatorId}) {
  /* Note: Ensure that childless events cache is proper, a previous work
  session may have terminated and failed to update the cache; this cache
  absolutely MUST NOT be corrupt in order for the work session operation
  to function properly, a corrupt cache here may result in loss of
  operations/regular events or invalidation as a properly operating peer.
  It should also be noted that the local regular event count in the cache
  may not be properly synced when this happens, but that key is only used
  for statistics gathering purposes and has a short expiration anyway. */
  await _cache.prime.primeChildlessEvents({ledgerNode});

  // ensure the cache head for this ledger node is in sync w/database
  const [cacheHead, mongoHead] = await Promise.all([
    _history.getHead({creatorId, ledgerNode}),
    _history.getHead({creatorId, ledgerNode, useCache: false})
  ]);
  if(_.isEqual(cacheHead, mongoHead)) {
    // success
    return;
  }
  // this should never happen and requires intervention to determine if
  // it can be repaired
  if((mongoHead.generation - cacheHead.generation) !== 1) {
    const ledgerNodeId = ledgerNode.id;
    throw new BedrockError(
      'Critical error: The cache is behind by more than one merge event.',
      'InvalidStateError',
      {cacheHead, mongoHead, ledgerNodeId});
  }
  const {eventHash} = mongoHead;
  await _events.repairCache({eventHash, ledgerNode});
}
