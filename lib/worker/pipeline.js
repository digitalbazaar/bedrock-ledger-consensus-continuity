/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
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

api.run = async ({worker, mergeOptions = {}, targetCycles = -1} = {}) => {
  _runningPipelines++;
  try {
    await _runPipeline({worker, mergeOptions, targetCycles});
  } finally {
    _runningPipelines--;
  }
};

async function _runPipeline({
  worker, mergeOptions = {}, targetCycles = -1
} = {}) {
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

  // FIXME: replace pub/sub operation notifications with a non-redis solution
  // or reengineer to eliminate its necessity

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

    // init gossip peer candidates concurrently with merge
    const {peerSelector} = worker;
    const initGossipPeersPromise = peerSelector.refreshCandidates();

    // first, if there are any `pendingLocalRegularEventHashes`, ensuring
    // pending merge is completed (required to avoid generating invalid state,
    // such as bad `localEventNumber` assignment to events)
    if(worker.pendingLocalRegularEventHashes.size > 0) {
      const [{merged}] = await Promise.all([
        worker.merge(),
        initGossipPeersPromise
      ]);
      // notify peers if a merge occurred
      await _notify({worker, merged});
    } else {
      // ensure gossip peers have been initialized
      await initGossipPeersPromise;
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

      // 3. refresh peer candidates to gossip with concurrently w/merge
      const refreshGossipPeersPromise = peerSelector.refreshCandidates();

      // 4. if not busy, merge if possible and send notification as needed
      let hasOutstandingOperations = false;
      if(!busy) {
        const [{merged, status: mergeStatus}] = await Promise.all([
          worker.merge(mergeOptions),
          refreshGossipPeersPromise
        ]);
        await _notify({worker, merged, mergeEventsReceived});
        if(worker.halt()) {
          break;
        }
        ({hasOutstandingOperations} = mergeStatus);
      } else {
        // ensure gossip peers have been refreshed
        await refreshGossipPeersPromise;
      }

      // if gossip has indicated that the network is too busy or there are
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
  const {lastLocalContributorConsensus} = worker;

  // Peers need to be notified when:
  // 1. If we've merged recently, OR
  // 2. We have received merge events to share, OR
  // 3. The last contributing merge event we created has not reached consensus.
  const notify = merged || mergeEventsReceived ||
    !lastLocalContributorConsensus;
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
