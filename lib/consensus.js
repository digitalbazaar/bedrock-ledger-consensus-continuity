/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const _cache = require('./cache');
const {BedrockError} = bedrock.util;
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

  const eventHash = await _getAncestors({
    blockHeight, hashes: result.eventHashes, ledgerNode});
  const hashSet = new Set(eventHash);
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
  // must look up `hashes.parentHashes` to filter out only the ones that
  // have not reached consensus yet

  const {parentHashes} = hashes;
  const ledgerNodeId = ledgerNode.id;

  // first ensure that all the referenced events exist
  // hashes for regular events are extracted from merge events and
  // there is a possibility that those regular events do not exist
  const exists = await ledgerNode.storage.events.exists(parentHashes);
  if(!exists) {
    throw new BedrockError(
      'Some ancestors selected for consensus do not exist.',
      'InvalidStateError', {ledgerNodeId, parentHashes});
  }
  const nonConsensusHashes = await filterHashes(
    {consensus: false, eventHashes: parentHashes});

  // events may have been assigned to the current block during a prior
  // failed operation. All events must be included so that `blockOrder`
  // can be properly computed
  const nonConsensusSet = new Set(nonConsensusHashes);
  const notFound = parentHashes.filter(h => !nonConsensusSet.has(h));
  // the test suite does not pass `blockHeight` into this API
  if(notFound.length !== 0 && _.isInteger(blockHeight)) {
    const hashes = await filterHashes({blockHeight, eventHashes: notFound});
    nonConsensusHashes.push(...hashes);
  }
  return hashes.mergeEventHashes.concat(nonConsensusHashes);

  async function filterHashes({blockHeight, consensus, eventHashes}) {
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
}
