/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const _cache = require('./cache');
const {callbackify, BedrockError} = bedrock.util;
const brLedgerNode = require('bedrock-ledger-node');
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
 * @param [recoveryElectors] optionally any recovery electors to use.
 * @param [recoveryGenerationThreshold] the "no progress" merge event
 *          generation threshold required to enter recovery mode; this
 *          must be given if `recoveryElectors` is given.
 * @param [mode='first'] an optional mode for selecting which events
 *          to find consensus on:
 *          // FIXME: change default to `first` in code
 *          first: create support sets from the first (aka "tail" or oldest)
 *            event from each participating elector and return the events that
 *            are in the consensus support set as having reached consensus
 *            (i.e., `eventHashes`) (mode is aka "Ys are Xs").
 *          firstWithConsensusProof: create support sets from the first
 *            endorsement event for the first (aka "tail" or oldest) event from
 *            each participating elector and return the consensus support set
 *            and all ancestors until the associated tail events as
 *            `consensusProofHashes` and the tails associated with the
 *            endorsement events as having reached consensus (i.e.,
 *            `eventHashes`) (mode is aka "Y is the endorsement of X").
 *          batch: the same as `firstWithConsensusProof` except all
 *            `consensusProofHashes` are instead also returned as having
 *            reached consensus in `eventHashes`.
 *
 * @return a Promise that resolves to a result object with the following
 *         properties:
 *           consensus: `true` if consensus has been found, `false` if` not.
 *           eventHash: the hashes of all events that have reached
 *             consensus in order according to `Continuity2017`.
 *           consensusProofHash: the hashes of events endorsing `eventHash`,
 *             if using mode=`firstWithConsensusProof`.
 *           needsMergeEvent: `true` if consensus is `false` and another merge
 *             event is needed from the node matching `ledgerNodeId`.
 *           creators: the electors that participated in events that reached
 *             consensus.
 *           electors: all electors that could have participated.
 *           recoveryElectors: all recovery electors that could have
 *             participated.
 */
api.findConsensus = callbackify(async ({
  ledgerNode, history, blockHeight, electors,
  recoveryElectors = [], recoveryGenerationThreshold,
  mode = 'firstWithConsensusProof'
}) => {
  logger.verbose('Start sync _runConsensusInPool, electors', {electors});
  const timer = new _cache.Timer();
  timer.start({name: 'findConsensus', ledgerNodeId: ledgerNode.id});
  let result;
  try {
    result = await _runConsensusInPool({
      ledgerNode, history, blockHeight, electors,
      recoveryElectors, recoveryGenerationThreshold, mode
    });
  } finally {
    const duration = await timer.stop();
    logger.verbose('End sync _runConsensusInPool', {duration});
  }

  // no consensus found
  if(!result.consensus) {
    return {...result, electors, recoveryElectors};
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
    electors,
    recoveryElectors
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
 * @param [recoveryMode] `true` if using recovery mode, `false` if not.
 *
 * @return a Promise that resolves to an object with properties:
 *         electors the array of electors.
 *         [recoveryElectors] the array of recovery electors.
 *         [recoveryGenerationThreshold] the maximum number of "no progress"
 *           merge events before attempting to trigger recovery mode.
 */
api.getBlockElectors = callbackify(async (
  {ledgerNode, blockHeight, recoveryMode = false}) => {
  const ledgerNodeId = ledgerNode.id;

  let electors = await _cache.consensus.getElectors(
    {blockHeight, ledgerNodeId, recoveryMode});

  // return cached electors
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
  const result = await electorSelectionMethod.api.getBlockElectors({
    ledgerNode, ledgerConfiguration, latestBlockSummary, blockHeight,
    recoveryMode
  });

  electors = result.electors;
  if(!('recoveryElectors' in result)) {
    result.recoveryElectors = [];
  }

  // TODO: potentially support recovery mode as an integer in the future
  // for N recovery attempts
  if(recoveryMode && result.recoveryElectors.length > 0) {
    throw new BedrockError(
      'Recovery electors must not be provided again once recovery mode has ' +
      'been entered.', 'InvalidStateError');
  }

  // validate that `electors` is either length `1` or forms a `3f+1` size set
  if(electors.length === 1) {
    // force recovery electors to none
    result.recoveryElectors = [];
  } else {
    const f = (electors.length - 1) / 3;
    if(!Number.isInteger(f)) {
      throw new BedrockError(
        'Electors do not form a set of size "3f+1".', 'InvalidStateError');
    }

    // validate that `recoveryElectors` is either empty or a subset of
    // `electors` and forms a `3r+1` size set
    const {recoveryElectors} = result;
    if(recoveryElectors.length > 0) {
      const r = (recoveryElectors.length - 1) / 3;
      if(!Number.isInteger(r)) {
        throw new BedrockError(
          'Recovery electors do not form a set of size "3r+1"; recovery ' +
          `electors count is ${recoveryElectors.length}, "r" is ${r}, and ' +
          '"f" is ${f}.`,
          'InvalidStateError');
      }

      // ensure recovery electors are a subset of electors
      const electorIds = new Set(electors.map(e => e.id));
      const recoveryIds = recoveryElectors.map(e => e.id);
      if(!recoveryIds.every(id => electorIds.has(id))) {
        throw new BedrockError(
          'Recovery electors must be a subset of all electors.',
          'InvalidStateError');
      }

      // validate generation threshold
      const {recoveryGenerationThreshold} = result;
      if(!Number.isInteger(recoveryGenerationThreshold)) {
        throw new BedrockError(
          '"recoveryGenerationThreshold" must be a number when ' +
          'recovery electors are specified.', 'InvalidStateError');
      }
    }
  }

  // cache electors
  await _cache.consensus.setElectors(
    {blockHeight, electors: result, ledgerNodeId, recoveryMode});

  return result;
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

async function _runConsensusInPool({
  ledgerNode, history, blockHeight, electors,
  recoveryElectors, recoveryGenerationThreshold, mode
}) {
  const cfg = bedrock.config['ledger-consensus-continuity'].consensus;
  if(!cfg.workerpool.enabled) {
    // run consensus directly
    return api._consensus.findConsensus({
      ledgerNodeId: ledgerNode.id, history, blockHeight, electors,
      recoveryElectors, recoveryGenerationThreshold, mode, logger
    });
  }

  // run consensus in pool
  return consensusWorker.findConsensus({
    ledgerNodeId: ledgerNode.id,
    history: {
      // TODO: investigate if it's faster to send the events map or the array
      //   whichever is not sent must be rebuilt in the worker

      // do not include `eventsMap`; it must be recreated
      events: history.events,
      localBranchHead: history.localBranchHead
    },
    blockHeight, electors, recoveryElectors,
    recoveryGenerationThreshold, mode
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
  const {electorSelectionMethod} = ledgerConfiguration;
  if(!electorSelectionMethod) {
    throw new TypeError(
      '`ledgerConfiguration.electorSelectionMethod` is required.');
  }
  return brLedgerNode.use(electorSelectionMethod.type);
}
