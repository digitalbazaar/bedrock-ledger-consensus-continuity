/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const _cache = require('../cache');
const cache = require('bedrock-redis');
const {config} = require('bedrock');
const delay = require('delay');
const logger = require('../logger');

// load config defaults
require('../config');

// module API
const api = {};
module.exports = api;

let _runningPipelines = 0;

bedrock.events.on('bedrock.exit', async () => {
  // wait for all running pipelines to stop; `bedrock.exit` handler in
  // `Worker.js` will cause workers to halt
  while(_runningPipelines > 0) {
    await delay(100);
  }
});

api.run = async ({worker, targetCycles = -1} = {}) => {
  _runningPipelines++;
  try {
    await _runPipeline({worker, targetCycles});
  } finally {
    _runningPipelines--;
  }
};

async function _runPipeline({worker, targetCycles = -1} = {}) {
  const {ledgerNode, session} = worker;
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
    and potential errors. Calling `worker.init()` initializes the worker state
    based on what is in the database. */
    await worker.init();

    const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;

    // run consensus/gossip/merge pipeline until work session expires
    // or until the pipeline is run at least once per request; only used
    // during testing
    let cycles = 0;
    if(targetCycles > 0) {
      const {_customHalt} = worker;
      worker._customHalt = () => _customHalt() || cycles >= targetCycles;
    }

    // first, if there are any `pendingLocalRegularEventHashes`, ensuring
    // pending merge is completed (required to avoid generating invalid state,
    // such as bad `localEventNumber` assignment to events)
    if(worker.pendingLocalRegularEventHashes.size > 0) {
      const {merged} = await worker.merge();
      // notify peers if a merge occurred
      await _notify({worker, merged});
    }

    // next, always try to extend the blockchain in case we halted the
    // previous work session with events we haven't run through consensus yet
    await worker.extendBlockchain();

    while(!worker.halt()) {
      // 1. run gossip cycle; the gossip cycle runs an internal loop against
      // selections of peers and reports how many merge events were received
      // or if the network is too busy to get any gossip
      const {mergeEventsReceived, busy} = await worker.runGossipCycle();
      if(worker.halt()) {
        break;
      }

      // 2. extend blockchain until can't anymore
      await worker.extendBlockchain();
      if(worker.halt()) {
        break;
      }

      // 3. if not busy, merge if possible and send notification as needed
      let hasOutstandingOperations = false;
      if(!busy) {
        const {merged, status: mergeStatus} = await worker.merge();
        await _notify({worker, merged, mergeEventsReceived});
        if(worker.halt()) {
          break;
        }
        ({hasOutstandingOperations} = mergeStatus);
      }

      // if gossip has indicated the priority peers are too busy or there are
      // no outstanding operations (this includes configurations) that need to
      // achieve consensus, then delay for cool down period or until a peer
      // notification or a local operation notification comes in
      if(busy || !hasOutstandingOperations) {
        await new Promise(resolve => {
          resume = resolve;
          setTimeout(resolve, coolDownPeriod);
        });
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

async function _notify({worker, merged, mergeEventsReceived = 0}) {
  const {ledgerNodeId} = worker;

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
      await worker.notifyPeers();
    } catch(e) {
      // just log the error, another attempt will be made on the next
      // cycle
      logger.error(
        'An error occurred while attempting to send gossip notification.',
        {error: e});
    }
  }
}
