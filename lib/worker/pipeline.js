/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const cache = require('bedrock-redis');
const {config} = require('bedrock');
const logger = require('../logger');

// load config defaults
require('../config');

// module API
const api = {};
module.exports = api;

api.run = async ({worker, targetCycles = -1} = {}) => {
  const {ledgerNode, session} = worker;
  let {halt} = worker;
  const ledgerNodeId = ledgerNode.id;

  logger.verbose('Ledger work session job running', {ledgerNodeId});

  // ensure the genesis block exists before running, if not, exit
  // immediately, there's nothing to do
  try {
    await ledgerNode.blocks.getGenesis();
  } catch(e) {
    if(e.name !== 'NotFoundError') {
      throw e;
    }
    logger.verbose(
      'Ledger work session exiting early; there is no genesis block ' +
      'for the ledger node.', {ledgerNodeId});
    return;
  }

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
    write to the database or other state will be successful or an error will
    be thrown canceling the work session. This allows for the database and
    any other state to be brought into sync at the start of the work session,
    without having to worry about them getting out of sync in the middle of
    it. It also provides a more simple model for reasoning about correctness
    and potential errors. Calling `_init` initializes the state based on
    what is in the database. */
    await worker.init();

    const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
    // FIXME: move `needsGossip` into worker state
    let needsGossip = false;

    // commit any previously cached events to the database that could
    // not be written before because gossip timed out; this step ensures
    // valid pending events are always written
    if(!halt()) {
      await worker.writeEvents();
    }

    // run consensus/gossip/merge pipeline until work session expires
    // or until the pipeline is run at least once per request
    let cycles = 0;
    if(targetCycles > 0) {
      const _halt = halt;
      worker.halt = halt = () => _halt() || cycles >= targetCycles;
    }
    while(!halt()) {
      // 1. extend blockchain until can't anymore
      const {blocks} = await worker.extendBlockchain();

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
      const {mergeEventsReceived} = await worker.runGossipCycle(
        {worker, needsGossip});

      // work session expired
      if(halt()) {
        break;
      }

      // 3. commit all cached events to mongo
      await worker.writeEvents();

      // FIXME: remove me, get directly within `worker.merge`
      const {nextBlockHeight: blockHeight, priorityPeers, witnesses} =
        worker.consensusState;

      // 4. merge if possible
      const {merged, status: mergeStatus} = await worker.merge({
        priorityPeers, witnesses, basisBlockHeight: blockHeight - 1
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
          await worker.sendNotification();
        } catch(e) {
          // just log the error, another attempt will be made on the next
          // cycle
          logger.error(
            'An error occurred while attempting to send merge notification.',
            {error: e});
        }
      }

      // work session expired
      if(halt()) {
        break;
      }

      // if there are no outstanding operations (this includes
      // configurations) need to achieve consensus, then delay for cool down
      // period or until a peer notification or a local operation
      // notification comes in
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
};
