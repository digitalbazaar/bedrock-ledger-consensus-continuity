/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const _blocks = require('./blocks');
const _cache = require('./cache');
const {callbackify, BedrockError} = bedrock.util;
const brLedgerNode = require('bedrock-ledger-node');
const logger = require('./logger');
const path = require('path');
const workerpool = require('workerpool');

// load config defaults
require('./config');

// add simple elector selection method
require('./simpleElectorSelection');

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
    path.join(__dirname, 'consensus-worker.js'), {maxWorkers});
  consensusWorker = await consensusPool.proxy();
});

// module API
const api = {};
module.exports = api;

api._consensus = require('./consensus');
// exposed for testing
api._getElectorBranches = api._consensus._getElectorBranches;
api._getAncestors = _getAncestors;
api._findMergeEventProof = api._consensus._findMergeEventProof;

/**
 * Determine if any new merge events have reached consensus in the given
 * history summary of merge events w/o consensus.
 *
 * @param ledgerNode the local ledger node.
 * @param history recent history rooted at the ledger node's local branch
 *          including ONLY merge events, it must NOT include local regular
 *          events.
 * @param electors the current electors.
 *
 * @return a Promise that resolves to `null` if no consensus has been reached
 *    or an object if consensus has been reached, where:
 *      `eventHash` the hashes of all events that have reached
 *        consensus in order according to `Continuity2017`.
 *      `consensusProofHash` the hashes of all merge events proving consensus.
 */
api.findConsensus = callbackify(async (
  {ledgerNode, history, blockHeight, electors}) => {
  logger.verbose('Start sync _runConsensusInPool, electors', {electors});
  const timer = new _cache.Timer();
  timer.start({name: 'findConsensus', ledgerNodeId: ledgerNode.id});
  let consensus;
  try {
    consensus = await _runConsensusInPool(
      {ledgerNode, history, blockHeight, electors});
  } finally {
    const duration = await timer.stop();
    logger.verbose('End sync _runConsensusInPool', {duration});
  }

  // no consensus found
  if(!consensus) {
    return null;
  }

  const eventHash = await _getAncestors({
    blockHeight, hashes: consensus.eventHashes, ledgerNode});
  const hashSet = new Set(eventHash);
  const order = consensus.eventHashes.order.filter(h => hashSet.has(h));
  return {
    consensusProofHash: consensus.consensusProofHashes,
    creators: consensus.creators,
    eventHash: order,
    mergeEventHash: consensus.eventHashes.mergeEventHashes,
  };
});

/**
 * Get the electors for the given ledger node and block height.
 *
 * The electors will be passed to the given callback using the given
 * data structure:
 *
 * [{id: voter_id, sameAs: previous_voter_id}, ... ]
 *
 * @param ledgerNode the ledger node API to use.
 * @param blockHeight the height of the block.
 *
 * @return a Promise that resolves to the array of electors.
 */
api.getBlockElectors = callbackify(async ({ledgerNode, blockHeight}) => {
  // first check cache for electors
  const ledgerNodeId = ledgerNode.id;
  let electors = await _cache.consensus.getElectors(
    {blockHeight, ledgerNodeId});
  if(electors) {
    return electors;
  }

  // electors not in cache, will need to be computed
  const [ledgerConfiguration, latestBlockSummary] = await Promise.all([
    _getLatestConfig(ledgerNode),
    ledgerNode.storage.blocks.getLatestSummary(ledgerNode)
  ]);

  const {eventBlock} = latestBlockSummary;

  // FIXME: do we need to force this ... can we avoid this check?
  // ensure requested `blockHeight` matches next block
  const expectedBlockHeight = eventBlock.block.blockHeight + 1;
  if(expectedBlockHeight !== blockHeight) {
    throw new BedrockError(
      'Invalid `blockHeight` specified.', 'InvalidStateError', {
        blockHeight,
        expectedBlockHeight
      });
  }

  // use elector selection method plugin to get electors for block
  const electorSelectionMethod = _getElectorSelectionMethod(
    {ledgerConfiguration});
  if(electorSelectionMethod.type !== 'electorSelection') {
    throw new BedrockError(
      'Elector selection method is invalid.', 'InvalidStateError');
  }
  electors = await electorSelectionMethod.api.getBlockElectors(
    {ledgerNode, ledgerConfiguration, latestBlockSummary, blockHeight});

  // cache electors
  await _cache.consensus.setElectors({electors, blockHeight, ledgerNodeId});

  return electors;
});

/**
 * Determines if the given voter is in the passed voting population.
 *
 * @param voter the voter to check for.
 * @param electors the voting population.
 *
 * @return true if the voter is in the voting population, false if not.
 */
api.isBlockElector = (voter, electors) => {
  return electors.some(v => v.id === voter.id);
};

async function _runConsensusInPool(
  {ledgerNode, history, blockHeight, electors}) {
  const cfg = bedrock.config['ledger-consensus-continuity'].consensus;
  if(!cfg.workerpool.enabled) {
    // run consensus directly
    return api._consensus.findConsensus(
      {ledgerNodeId: ledgerNode.id, history, blockHeight, electors, logger});
  }

  // run consensus in pool
  return consensusWorker.findConsensus(
    {ledgerNodeId: ledgerNode.id, history: {
      // TODO: investigate if it's faster to send the events map or the array
      //   whichever is not sent must be rebuilt in the worker

      // do not include `eventsMap`; it must be recreated
      events: history.events,
      localBranchHead: history.localBranchHead
    }, blockHeight, electors});
}

// FIXME: documentation
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

async function _getLatestConfig(ledgerNode) {
  const result = await ledgerNode.storage.events.getLatestConfig();
  // `getLatestConfig` returns an empty object before genesis block is written
  if(_.isEmpty(result)) {
    return {};
  }
  const config = result.event.ledgerConfiguration;
  if(config.consensusMethod !== 'Continuity2017') {
    throw new BedrockError(
      'Consensus method must be "Continuity2017".', 'InvalidStateError', {
        consensusMethod: config.consensusMethod
      });
  }
  return config;
}

function _getElectorSelectionMethod({ledgerConfiguration}) {
  // FIXME: remove default `SimpleElectorSelection` and throw an error
  // no elector selection method has been set when using continuity -- i.e.
  // only temporarily allowing none to be specified for backwards compat
  const {
    electorSelectionMethod = {
      type: 'SimpleElectorSelection'
    }
  } = ledgerConfiguration.electorSelectionMethod || {};
  return brLedgerNode.use(electorSelectionMethod.type);
}
