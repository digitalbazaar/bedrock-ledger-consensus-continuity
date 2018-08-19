/*!
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const _blocks = require('./blocks');
const cache = require('bedrock-redis');
const jsigs = require('jsonld-signatures')();
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const logger = require('./logger');
const path = require('path');
const workerpool = require('workerpool');
const BedrockError = bedrock.util.BedrockError;

// load config defaults
require('./config');

// add simple elector selection method
require('./simpleElectorSelection');

jsigs.use('jsonld', brDidClient.jsonld);

let consensusPool;
let consensusWorker;
bedrock.events.on('bedrock.start', (callback) => {
  const cfg = bedrock.config['ledger-consensus-continuity'];
  if(!cfg.consensus.workerpool.enabled) {
    return callback();
  }

  // start a worker pool for consensus calculations
  const maxWorkers = cfg.consensus.workerpool.maxWorkers;
  consensusPool = workerpool.pool(
    path.join(__dirname, 'consensus-worker.js'), {maxWorkers});
  // FIXME: dump node 6.x and use `await`
  consensusPool.proxy()
    .then(worker => {
      consensusWorker = worker;
      callback();
    }, callback);
});

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._hasher = brLedgerNode.consensus._hasher;
api._storage = require('./storage');
api._voters = require('./voters');
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
 * @param callback(err, result) called once the operation completes where
 *          `result` is null if no consensus has been reached or where it
 *          is an object if it has, where:
 *          `result.eventHash` the hashes of all events that have reached
 *            consensus in order according to `Continuity2017`.
 *          `result.consensusProofHash` the hashes of all merge events
 *            proving consensus.
 */
api.findConsensus = (
  {ledgerNode, history, blockHeight, electors}, callback) => {
  logger.verbose('Start sync _runConsensusInPool, electors', {electors});
  const startTime = Date.now();
  _runConsensusInPool(
    {ledgerNode, history, blockHeight, electors}, (err, consensus) => {
      const duration = Date.now() - startTime;
      cache.client.set(`findConsensus|${ledgerNode.id}`, duration);
      logger.verbose('End sync _runConsensusInPool', {duration});
      if(err || !consensus) {
        return callback(err, null);
      }
      _getAncestors({
        blockHeight, hashes: consensus.eventHashes, ledgerNode
      }, (err, eventHash) => {
        if(err) {
          return callback(err);
        }
        const hashSet = new Set(eventHash);
        const order = consensus.eventHashes.order.filter(h => hashSet.has(h));
        callback(null, {
          consensusProofHash: consensus.consensusProofHashes,
          creators: consensus.creators,
          eventHash: order,
          mergeEventHash: consensus.eventHashes.mergeEventHashes,
        });
      });
    });
};

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
 * @param callback(err, electors) called once the operation completes.
 */
api.getBlockElectors = (ledgerNode, blockHeight, callback) => {
  async.auto({
    config: callback => _getLatestConfig(ledgerNode, callback),
    // NOTE: events *must* be expanded here
    latestBlock: callback => _blocks.getLatest(ledgerNode, (err, result) => {
    // latestBlock: callback => ledgerNode.storage.blocks.getLatest(
      // (err, result) => {
      if(err) {
        return callback(err);
      }
      const expectedBlockHeight = result.eventBlock.block.blockHeight + 1;
      if(expectedBlockHeight !== blockHeight) {
        return callback(new BedrockError(
          'Invalid `blockHeight` specified.', 'InvalidStateError', {
            blockHeight,
            expectedBlockHeight
          }));
      }
      callback(null, result);
    }),
    electorSelectionMethod: ['config', (results, callback) => {
      _getElectorSelectionMethod({config: results.config}, callback);
    }],
    electors: ['latestBlock', 'electorSelectionMethod', (results, callback) => {
      const {electorSelectionMethod} = results;
      if(electorSelectionMethod.type !== 'electorSelection') {
        return callback(new BedrockError(
          'Elector selection method is invalid.', 'InvalidStateError'));
      }
      results.electorSelectionMethod.api.getBlockElectors(
        {ledgerNode, ledgerConfiguration: results.config, blockHeight},
        callback);
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.electors));
};

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

function _runConsensusInPool(
  {ledgerNode, history, blockHeight, electors}, callback) {
  const cfg = bedrock.config['ledger-consensus-continuity'].consensus;
  if(!cfg.workerpool.enabled) {
    // run consensus directly
    const consensus = api._consensus.findConsensus(
      {ledgerNodeId: ledgerNode.id, history, blockHeight, electors, logger});
    return callback(null, consensus);
  }

  // run consensus in pool
  consensusWorker.findConsensus(
    {ledgerNodeId: ledgerNode.id, history: {
      // TODO: investigate if it's faster to send the events map or the array
      //   whichever is not sent must be rebuilt in the worker

      // do not include `eventsMap`; it must be recreated
      events: history.events,
      localBranchHead: history.localBranchHead
    }, blockHeight, electors})
    .then(consensus => callback(null, consensus), callback);
}

// FIXME: documentation
function _getAncestors({blockHeight, hashes, ledgerNode}, callback) {
  // must look up `hashes.parentHashes` to filter out only the ones that
  // have not reached consensus yet

  // retrieve up to 1000 at a time to prevent hitting limits or starving
  // resources
  const batchSize = 1000;
  const nonConsensusHashes = [];
  let start = 0;
  const {parentHashes} = hashes;
  let remaining = parentHashes.length;
  const collection = ledgerNode.storage.events.collection;
  const projection = {_id: 0, 'meta.eventHash': 1};
  const query = {
    'meta.eventHash': {},
    $or: [
      {'meta.consensus': {$exists: false}},
      // events may have been assigned to the current block during a prior
      // failed operation. All events must be included so that `blockOrder`
      // can be properly computed
      {'meta.blockHeight': blockHeight}
    ]
  };
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    // ensure that all the referenced events exist
    // hashes for regular events are extracted from merge events and
    // there is a possibility that those regular events do not exist
    exists: callback => ledgerNode.storage.events.exists(
      parentHashes, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return callback(new BedrockError(
            'Some ancestors selected for consensus do not exist.',
            'InvalidStateError', {ledgerNodeId, parentHashes}));
        }
        callback();
      }),
    nonConsensus: ['exists', (results, callback) => {
      async.whilst(() => remaining > 0, callback => {
        query['meta.eventHash'].$in =
          parentHashes.slice(start, start + batchSize);
        start += batchSize;
        remaining -= query['meta.eventHash'].$in.length;
        collection.find(query, projection)
          .forEach(r => nonConsensusHashes.push(r.meta.eventHash), callback);
      }, callback);
    }]
  }, err => err ? callback(err) :
    callback(null, hashes.mergeEventHashes.concat(nonConsensusHashes)));
}

function _getLatestConfig(ledgerNode, callback) {
  ledgerNode.storage.events.getLatestConfig((err, result) => {
    if(err) {
      return callback(err);
    }
    // `getLatestConfig` returns an empty object before genesis block is written
    if(_.isEmpty(result)) {
      return callback(null, {});
    }
    const config = result.event.ledgerConfiguration;
    if(config.consensusMethod !== 'Continuity2017') {
      return callback(new BedrockError(
        'Consensus method must be "Continuity2017".', 'InvalidStateError', {
          consensusMethod: config.consensusMethod
        }));
    }
    callback(null, config);
  });
}

function _getElectorSelectionMethod({config}, callback) {
  // FIXME: remove default `SimpleElectorSelection` and throw an error
  // no elector selection method has been set when using continuity -- i.e.
  // only temporarily allowing none to be specified for backwards compat
  const {
    electorSelectionMethod = {
      type: 'SimpleElectorSelection'
    }
  } = config.electorSelectionMethod || {};
  _use(electorSelectionMethod.type, callback);
}

function _use(plugin, callback) {
  let p;
  try {
    p = brLedgerNode.use(plugin);
  } catch(e) {
    return callback(e);
  }
  callback(null, p);
}
