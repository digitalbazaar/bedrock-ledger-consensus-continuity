/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const _cache = require('../cache');
const _continuity = require('../continuity');
const _history = require('../history');
const logger = require('../logger');
const path = require('path');
const workerpool = require('workerpool');

// load config defaults
require('../config');

let consensusPool;
let consensusWorker;
bedrock.events.on('bedrock.start', async () => {
  const cfg = bedrock.config['ledger-consensus-continuity'];
  if(!cfg.consensus.workerpool.enabled) {
    return;
  }

  // start a worker pool for consensus calculations
  const maxWorkers = cfg.consensus.workerpool.maxWorkers;
  consensusPool = workerpool.pool(
    path.join(__dirname, 'continuityWorkerpoolWorker.js'), {maxWorkers});
  consensusWorker = await consensusPool.proxy();
});

// module API
const api = {};
module.exports = api;

/**
 * Determine if any new merge events have reached consensus via the given
 * worker.
 *
 * @param worker the worker with consensus state to use.
 *
 * @return a Promise that resolves to a result object with the following
 *   properties:
 *     consensus: `true` if consensus has been found, `false` if` not.
 *     eventHash: the hashes of all events that have reached
 *       consensus in order according to `Continuity2017`.
 *     ordering: the event hashes and both the block and gossip ordering
 *       information.
 *     consensusProofHash: the hashes of events endorsing `eventHash`,
 *       if single elector used.
 *     priorityPeers: if consensus is `false`, an array of peer (voter)
 *       IDs identifying the peers that may help achieve consensus most
 *       readily.
 *     creators: the witnesses that participated in events that reached
 *       consensus.
 *     witnesses: all witnesses for the current block.
 */
api.find = async ({worker}) => {
  // get recent history to run consensus algorithm on
  const history = worker.getRecentHistory();

  // get block height to use for consensus based on the "next" block height
  // for the block to be added next to the blockchain
  const {ledgerNode, consensusState} = worker;
  const {nextBlockHeight: blockHeight, continuityState: state, witnesses} =
    consensusState;

  logger.verbose('Start sync _runConsensusInPool, witnesses', {witnesses});
  const timer = new _cache.Timer();
  timer.start({name: 'findConsensus', ledgerNodeId: ledgerNode.id});
  let result;
  try {
    result = await _runConsensusInPool({
      ledgerNode, history, blockHeight, witnesses, state
    });
  } finally {
    const duration = await timer.stop();
    logger.verbose('End sync _runConsensusInPool', {duration});
  }

  // no consensus found
  if(!result.consensus) {
    return {...result, witnesses};
  }

  const eventHashes = await _history._addNonConsensusAncestorHashes({
    blockHeight, hashes: result.eventHashes, ledgerNode});
  const hashSet = new Set(eventHashes);
  const ordering = new Map();
  let blockOrder = 0;
  for(const eventHash of result.eventHashes.blockOrdering) {
    if(!hashSet.has(eventHash)) {
      continue;
    }
    ordering.set(eventHash, {
      eventHash,
      blockOrder: blockOrder++,
      gossipOrder: 0
    });
  }
  let gossipOrder = 0;
  for(const hash of result.eventHashes.gossipOrdering) {
    const entry = ordering.get(hash);
    if(entry) {
      entry.gossipOrder = gossipOrder++;
    }
  }

  // determine if any new forkers have been detected via the merge events
  // that have reached consensus
  const forkNumberMap = new Map();
  const forkerSet = new Set();
  const alreadyDetected = new Set();
  for(const eventHash of result.eventHashes.mergeEventHashes) {
    const eventSummary = worker.historyMap.get(eventHash);
    const {meta: {continuity2017: {
      creator, forkNumber, forkDetectedBlockHeight
    }}} = eventSummary;

    if(forkDetectedBlockHeight !== null) {
      // forker already detected in the past, but event was determined to be
      // valid because it was received by a peer that had not yet reached
      // `forkDetectedBlockHeight`
      alreadyDetected.add(creator);
      continue;
    }

    if(forkerSet.has(creator)) {
      // forker already detected in the block to be created
      continue;
    }

    // record fork number (even if `null`) for creator if not already recorded;
    // if recorded and different, a forker has been detected, if recorded and
    // the same, nothing to do, continue; we must store `null` too because a
    // `forkNumber` that is non-null vs. one that is `null` indicates a fork
    // just like two different `forkNumbers` that are non-null do
    const existing = forkNumberMap.get(creator);
    if(existing === undefined) {
      forkNumberMap.set(creator, forkNumber);
    } else if(existing !== forkNumber) {
      // different fork number detected, this means a forker has just
      // been detected
      forkerSet.add(creator);
      // do not need to check fork numbers anymore
      forkNumberMap.delete(creator);
    }
  }

  // find new forkers
  const newForkers = await _history._findNewForkers(
    {ledgerNode, forkNumberMap});
  for(const newForker of newForkers) {
    forkerSet.add(newForker);
  }

  return {
    consensus: true,
    consensusProofHash: result.consensusProofHashes,
    creators: result.creators,
    eventHash: [...ordering.keys()],
    forkerSet,
    ordering: [...ordering.values()],
    mergeEventHash: result.eventHashes.mergeEventHashes,
    witnesses
  };
};

async function _runConsensusInPool({
  ledgerNode, history, blockHeight, witnesses, state
}) {
  const cfg = bedrock.config['ledger-consensus-continuity'].consensus;
  if(!cfg.workerpool.enabled) {
    // run consensus directly
    return _continuity.findConsensus({
      ledgerNodeId: ledgerNode.id, history, blockHeight,
      witnesses, state, logger
    });
  }

  // run consensus in pool
  return consensusWorker.findConsensus({
    ledgerNodeId: ledgerNode.id,
    history: {
      events: history.events,
      localBranchHead: history.localBranchHead
    },
    blockHeight,
    witnesses
  });
}
