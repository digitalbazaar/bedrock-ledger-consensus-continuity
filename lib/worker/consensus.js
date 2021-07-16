/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _continuity = require('../continuity');
const _history = require('../history');
const logger = require('../logger');

// load config defaults
require('../config');

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
 *     priorityPeers: if consensus is `false`, an array of peer IDs
 *       identifying the peers that may help achieve consensus most
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

  logger.verbose('Start sync _continuity.findConsensus', {witnesses});
  const timer = new _cache.Timer();
  timer.start({name: 'findConsensus', ledgerNodeId: ledgerNode.id});
  let result;
  try {
    result = _continuity.findConsensus({
      ledgerNodeId: ledgerNode.id, history, blockHeight,
      witnesses, state, logger
    });
  } finally {
    const duration = await timer.stop();
    logger.verbose('End sync _continuity.findConsensus', {duration});
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
      blockOrder: blockOrder++
    });
  }

  // determine if any new replays have been detected via the merge events
  // that have reached consensus
  const replayNumberMap = new Map();
  const replayerSet = new Set();
  const alreadyDetected = new Set();
  for(const eventHash of result.eventHashes.mergeEventHashes) {
    const eventSummary = worker.historyMap.get(eventHash);
    const {meta: {continuity2017: {
      creator, localReplayNumber, replayDetectedBlockHeight
    }}} = eventSummary;

    if(replayDetectedBlockHeight !== -1) {
      // replayer already detected in the past, but event was determined to be
      // valid because it was received by a peer that had not yet reached
      // `replayDetectedBlockHeight`
      alreadyDetected.add(creator);
      continue;
    }

    if(replayerSet.has(creator)) {
      // replayer already detected in the block to be created
      continue;
    }

    // record replay number (even if `0`) for creator if not already recorded;
    // if recorded and different, a replayer has been detected, if recorded and
    // the same, nothing to do, continue; we must store `0` too because a
    // `localReplayNumber` that is non-zero vs. one that is `0` indicates a
    // replay even though `0` indicates no replay yet
    const existing = replayNumberMap.get(creator);
    if(existing === undefined) {
      replayNumberMap.set(creator, localReplayNumber);
    } else if(existing !== localReplayNumber) {
      // different replay number detected, this means a replayer has just
      // been detected
      replayerSet.add(creator);
      // do not need to check replay numbers anymore
      replayNumberMap.delete(creator);
    }
  }

  // find new replayers
  const {findNewReplayers} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  const newReplayers = await findNewReplayers({replayNumberMap});
  for(const newReplayer of newReplayers) {
    replayerSet.add(newReplayer);
  }

  return {
    consensus: true,
    creators: result.creators,
    eventHash: [...ordering.keys()],
    replayerSet,
    ordering: [...ordering.values()],
    mergeEventHash: result.eventHashes.mergeEventHashes,
    witnesses
  };
};
