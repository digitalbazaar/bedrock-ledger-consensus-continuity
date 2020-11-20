/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _voters = require('../voters');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config, util: {callbackify}} = bedrock;
const logger = require('../logger');
const {extendBlockchain} = require('./consensus');
const {merge} = require('./merge');
const {runGossipCycle, sendNotification} = require('./gossip');
const EventWriter = require('./EventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

// load config defaults
require('../config');

// module API
const api = {};
module.exports = api;

// exposed for testing
api.EventWriter = EventWriter;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('../temporaryLedgerNodeMeta');

const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;

api.scheduleWork = ({session}) => {
  // start a consensus session for ledgers
  const maxTime =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  session.start(maxTime, _guardedSync)
    .catch(err => {
      logger.error('Error starting consensus job.', {error: err});
    });
};

// Note: exposed for testing
// this gives test worker approx 1 second per cycle
api._run = async ({ledgerNode}) => {
  return _sync({
    session: {ledgerNode},
    halt() {
      // don't halt before pipeline is run once; this is safe when
      // `runPipelineOnlyOnce` is set and there is no ledger work session
      // scheduler being used that might concurrently schedule another session,
      // which is true in tests
      return false;
    },
    // force pipeline to run only once in test mode
    runPipelineOnlyOnce: true
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

async function _sync({session, halt, runPipelineOnlyOnce = false}) {
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
    const creatorId = (await _voters.get({ledgerNodeId})).id;
    const peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
    const eventWriter = new EventWriter({ledgerNode});
    let needsGossip = true;

    // FIXME: determine if this extra event write is necessary to fix
    // stuck nodes that cannot complete gossip step and thus never write
    // cached events to disk
    // commit any previously cached events to mongo
    await eventWriter.write();

    // run consensus/gossip/merge pipeline until work session expires
    // or until the pipeline is run at least once per request
    const savedState = {};
    let hasRunOnce = false;
    if(runPipelineOnlyOnce) {
      const _halt = halt;
      halt = () => _halt() || hasRunOnce;
    }
    while(!halt()) {
      // 1. extend blockchain until can't anymore
      const {priorityPeers, mergePermits, halted} = await extendBlockchain(
        {ledgerNode, savedState, halt});

      // work session expired
      if(halted) {
        break;
      }

      // 2. gossip until `mergePermits - 1` permits are used, saving one for
      // a local merge event
      let {mergePermitsConsumed} = await runGossipCycle({
        ledgerNode, priorityPeers, creatorId, peerSelector,
        mergePermits: mergePermits - 1, needsGossip, halt
      });

      // work session expired
      if(halt()) {
        break;
      }

      // 3. commit all cached events to mongo
      await eventWriter.write();

      // 4. merge if possible
      const {merged, hasOutstandingOperations} = await merge(
        {ledgerNode, creatorId, priorityPeers, halt});
      needsGossip = hasOutstandingOperations;

      // determine if peers need to be notified
      let notify;
      if(merged) {
        mergePermitsConsumed++;
        await _cache.gossip.notifyFlag({add: true, ledgerNodeId});
        notify = true;
      } else {
        notify = (await _cache.gossip.notifyFlag({ledgerNodeId})) !== null;
      }

      if(!halt() && notify) {
        // notify peers of new/previous merge event
        try {
          await sendNotification({creatorId, priorityPeers, peerSelector});
        } catch(e) {
          // just log the error, another attempt will be made on the next cycle
          logger.error(
            'An error occurred while attempting to send merge notification.',
            {error: e});
        }
      }

      // if no merge permits were consumed and no gossip is needed, delay for
      // cool down period or until a peer notification or a local operation
      // notification comes in
      if(!halt() && mergePermitsConsumed === 0 && !needsGossip) {
        await new Promise(resolve => {
          resume = resolve;
          setTimeout(() => resolve(), coolDownPeriod);
        });
      }

      // track that a full cycle has run once
      hasRunOnce = true;
    }
  } finally {
    // unsubscribe from new operation messages
    subscriber.quit();
    logger.verbose('Work session completed.', {session: session.id});
  }
}
