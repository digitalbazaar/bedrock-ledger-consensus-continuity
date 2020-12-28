/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const _cache = require('./cache');
const logger = require('./logger');
const path = require('path');
const workerpool = require('workerpool');

// load config defaults
require('./config');

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

// exposed for testing
api._continuity = require('./continuity');
api._getAncestors = _getAncestors;

/**
 * Determine if any new merge events have reached consensus in the given
 * history summary of merge events w/o consensus.
 *
 * @param ledgerNode the local ledger node.
 * @param history recent history rooted at the ledger node's local branch
 *          including ONLY merge events, it must NOT include local regular
 *          events.
 * @param witnesses the current witnesses.
 * @param [state=null] an optional object for storing state information that
 *   can be reused for the same `blockHeight`.
 *
 * @return a Promise that resolves to a result object with the following
 *   properties:
 *     consensus: `true` if consensus has been found, `false` if` not.
 *     eventHash: the hashes of all events that have reached
 *       consensus in order according to `Continuity2017`.
 *     consensusProofHash: the hashes of events endorsing `eventHash`,
 *       if single elector used.
 *     priorityPeers: if consensus is `false`, an array of peer (voter)
 *       IDs identifying the peers that may help achieve consensus most
 *       readily.
 *     creators: the witnesses that participated in events that reached
 *       consensus.
 *     witnesses: all witnesses that could have participated.
 */
api.find = async ({
  ledgerNode, history, blockHeight, witnesses, state
}) => {
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

  const eventHashes = await _getAncestors({
    blockHeight, hashes: result.eventHashes, ledgerNode});
  const hashSet = new Set(eventHashes);
  const order = result.eventHashes.order.filter(h => hashSet.has(h));
  return {
    consensus: true,
    consensusProofHash: result.consensusProofHashes,
    creators: result.creators,
    eventHash: order,
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
    return api._continuity.findConsensus({
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
    blockHeight, witnesses
  });
}

// TODO: documentation
async function _getAncestors({blockHeight, hashes, ledgerNode}) {
  /* Note: `hashes.parentHashes` includes hashes for parents that were not
  included in the DAG set used to compute consensus. These hashes either refer
  to merge events that have already achieved consensus or they refer to regular
  events that are achieving consensus now. We must ensure we include these
  regular events in the next block, so we filter out those external parents
  that haven't acheived consensus yet here. */
  const {parentHashes} = hashes;

  // by now, all referenced events MUST exist because all events that were
  // fed into the consensus algorithm were validated, so go ahead and find
  // those that have not yet acheived consensus
  const nonConsensusHashes = await _filterHashes(
    {ledgerNode, consensus: false, eventHashes: parentHashes});

  // events may have been assigned to the current block during a prior
  // failed operation. All events must be included so that `blockOrder`
  // can be properly computed
  const nonConsensusSet = new Set(nonConsensusHashes);
  const notFound = parentHashes.filter(h => !nonConsensusSet.has(h));
  // FIXME: the test suite does not pass `blockHeight` into this API so we
  // have `blockHeight === undefined` here -- the test suite should be updated
  // so we can remove it
  if(notFound.length !== 0 && blockHeight !== undefined) {
    const hashes = await _filterHashes(
      {ledgerNode, blockHeight, eventHashes: notFound});
    nonConsensusHashes.push(...hashes);
  }
  // return all merge event hashes and the hashes for non-consensus regular
  // events that must be included in the block
  return hashes.mergeEventHashes.concat(nonConsensusHashes);
}

async function _filterHashes(
  {ledgerNode, blockHeight, consensus, eventHashes}) {
  // retrieve up to 1000 at a time to prevent hitting limits or starving
  // resources
  const batchSize = 1000;
  const filteredHashes = [];
  const chunks = _.chunk(eventHashes, batchSize);
  for(const chunk of chunks) {
    const filteredChunk = await ledgerNode.storage.events.filterHashes(
      {blockHeight, consensus, eventHash: chunk});
    filteredHashes.push(...filteredChunk);
  }
  return filteredHashes;
}
