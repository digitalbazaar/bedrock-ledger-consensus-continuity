/*
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const _blocks = require('./blocks');
const crypto = require('crypto');
const jsonld = bedrock.jsonld;
const jsigs = require('jsonld-signatures')();
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const logger = require('./logger');
const path = require('path');
const workerpool = require('workerpool');
const BedrockError = bedrock.util.BedrockError;

// load config defaults
require('./config');

jsigs.use('jsonld', brDidClient.jsonld);

let consensusPool;
let consensusWorker;
bedrock.events.on('bedrock.ready', (callback) => {
  const cfg = bedrock.config['ledger-consensus-continuity'];
  if(!cfg.consensus.workerpool.enabled) {
    return callback();
  }

  // start a worker pool for consensus calculations
  const maxWorkers = cfg.consensus.workerpool.maxWorkers;
  consensusPool = workerpool.pool(
    path.join(__dirname, 'consensus-worker.js'), {maxWorkers});
  // FIXME: dump node 6.x and use `await`
  consensusWorker = consensusPool.proxy()
    .then(worker => {
      consensusWorker = worker;
      callback();
    }, callback);
});

// maximum number of electors if not specified in the ledger configuration
const MAX_ELECTOR_COUNT = 10;

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
 *          result.event the merge events that have reached consensus.
 *          result.consensusProof the merge events proving consensus.
 */
api.findConsensus = (
  {ledgerNode, history, blockHeight, electors}, callback) => {
  logger.debug('Start sync _runConsensusInPool, electors', {electors});
  const startTime = Date.now();
  _runConsensusInPool(
    {ledgerNode, history, blockHeight, electors}, (err, consensus) => {
    logger.debug('End sync _runConsensusInPool', {
      duration: Date.now() - startTime
    });
    if(err || !consensus) {
      return callback(err, null);
    }

    _getAncestors(
      {ledgerNode, hashes: consensus.eventHashes}, (err, eventHash) => {
      if(err) {
        return callback(err);
      }
      callback(null, {
        consensusProofHash: consensus.consensusProofHashes,
        eventHash,
        creators: consensus.creators
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
// FIXME: remove hack
let _firstElector = null;
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
    electors: ['config', 'latestBlock', (results, callback) => {
      // get previous consensus events
      const previousEvents = results.latestBlock.eventBlock.block.event;

      // FIXME: this uses the key ID for the elector ID ... which has been
      //   made to be the same as the voter ID ... need to make sure this
      //   has no unintended negative consequences

      // aggregate recommended electors
      let electors = [];
      previousEvents.forEach(event => {
        if(!jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
          // regular event
          return;
        }
        // TODO: is `e.signature.creator` check robust enough? Can it assume
        //   a single signature and that it's by the voter? (merge events are
        //   only meant to be signed by the voter)
        electors.push(event.signature.creator);
        // TODO: support recommended electors?
        /*const recommended = jsonld.getValues(event, 'recommendedElector');
        // only accept a recommendation if there is exactly 1
        if(recommended.length === 1) {
          // TODO: recommended elector needs to be validated -- only
          //   previous participants (those that have generated signed merge
          //   events) can be recommended
          electors.push(recommended[0]);
        }*/
      });

      // TODO: we should be able to reasily remove previously detected
      // byzantine nodes (e.g. those that forked at least) from the electors

      // TODO: simply count consensus event signers once and proof signers
      //   twice for now -- add comprehensive elector selection and
      //   recommended elector vote aggregating algorithm in v2
      const aggregate = {};
      electors = _.uniq(electors).forEach(
        e => aggregate[e] = {id: e, weight: 1});
      // TODO: weight previous electors more heavily to encourage continuity
      const consensusProof =
        results.latestBlock.eventBlock.block.consensusProof;
      _.uniq(consensusProof.map(e => e.signature.creator))
        .forEach(id => {
          if(id in aggregate) {
            aggregate[id].weight = 3;
          } else {
            aggregate[id] = {id, weight: 2};
          }
        });
      electors = Object.keys(aggregate).map(k => aggregate[k]);

      // get elector count, defaulting to MAX_ELECTOR_COUNT if not set
      // (hardcoded, all nodes must do the same thing -- but ideally this would
      // *always* be set)
      const electorCount = results.config.electorCount || MAX_ELECTOR_COUNT;

      // TODO: could optimize by only sorting tied electors if helpful
      /*
      // fill positions
      let idx = -1;
      for(let i = 0; i < electorCount; ++i) {
        if(electors[i].weight > electors[i + 1].weight) {
          idx = i;
        }
      }
      // fill positions with non-tied electors
      const positions = electors.slice(0, idx + 1);
      if(positions.length < electorCount) {
        // get tied electors
        const tied = electors.filter(
          e => e.weight === electors[idx + 1].weight);
        // TODO: sort tied electors
      }
      }*/

      // break ties via sorting
      electors.sort((a, b) => {
        // 1. sort descending by weight
        if(a.weight !== b.weight) {
          // FIXME: with current weights, this prevents elector cycling
          //   if commented out, will force elector cycling, needs adjustment
          return b.weight - a.weight;
        }

        // generate and cache hashes
        // the hash of the previous block is combined with the elector id to
        // prevent any elector from *always* being sorted to the top
        a.hash = a.hash || _sha256(
          results.latestBlock.eventBlock.meta.blockHash + _sha256(a.id));
        b.hash = b.hash || _sha256(
          results.latestBlock.eventBlock.meta.blockHash + _sha256(b.id));

        // 2. sort by hash
        return a.hash.localeCompare(b.hash);
      });

      // select first `electorCount` electors
      electors = electors.slice(0, electorCount);

      // TODO: if there were no electors chosen or insufficient electors,
      // add electors from config

      electors.map(e => {
        // only include `id` and `sameAs`
        const elector = {id: e.id};
        if(e.sameAs) {
          elector.sameAs = e.sameAs;
        }
        return elector;
      });

      // reduce electors to highest multiple of `3f + 1`, i.e.
      // `electors.length % 3 === 1` or electors < 4 ... electors MUST be a
      // multiple of `3f + 1` for BFT or 1 for trivial dictator case
      while(electors.length > 1 && (electors.length % 3 !== 1)) {
        electors.pop();
      }
      // FIXME: remove me
      if(electors.length === 1) {
        if(!_firstElector) {
          _firstElector = consensusProof[0].signature.creator;
        }
        electors[0].id = _firstElector;
      }
      /*if(blockHeight > 10 && electors.length < 4) {
        console.log('electors dropped below 4');
      }*/

      logger.verbose(
        'Continuity2017 electors for ledger node ' + ledgerNode.id +
        ' at block height ' + blockHeight,
        {ledgerNode: ledgerNode.id, blockHeight, electors});

      callback(null, electors);
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
function _getAncestors({hashes, ledgerNode}, callback) {
  // must look up `hashes.parentHashes` to filter out only the ones that
  // have not reached consensus yet

  // retrieve up to 1000 at a time to prevent hitting limits or starving
  // resources
  const batchSize = 1000;
  const nonConsensusHashes = [];
  let start = 0;
  let remaining = hashes.parentHashes.length;
  const collection = ledgerNode.storage.events.collection;
  const projection = {_id: 0, eventHash: 1};
  const query = {
    eventHash: {},
    'meta.consensus': {$exists: false}
  };
  async.whilst(() => remaining > 0, callback => {
    query.eventHash.$in = hashes.parentHashes.slice(start, start + batchSize);
    start += batchSize;
    remaining -= query.eventHash.$in.length;
    collection.find(query, projection).toArray((err, result) => {
      if(err) {
        return callback(err);
      }
      result.forEach(r => nonConsensusHashes.push(r.eventHash));
      callback();
    });
  }, err => err ?
    callback(err) :
    callback(null, hashes.mergeEventHashes.concat(nonConsensusHashes)));
}

/**
 * Gets peer voters from an event, based on its signatures. If the event
 * has no signature from a peer voter, then an empty array will be returned in
 * the callback.
 *
 * @param ledgerNode the ledger node.
 * @param event the event to check.
 * @param callback(err, peers) called once the operation completes.
 */
function _getEventPeers(ledgerNode, event, callback) {
  // TODO: optimize
  const owners = [];
  jsigs.verify(event, {
    checkKeyOwner: (owner, key, options, callback) => {
      if(jsonld.hasValue(
        owner, 'type', 'https://w3id.org/wl#Continuity2017Peer')) {
        owners.push({id: owner.id});
      }
      callback(null, true);
    }
  }, err => {
    if(err) {
      // ignore bad or missing signature; no event peer can be found
      // TODO: revert to verbose
      logger.debug('Non-critical error in _getEventPeers.', err);
      //logger.verbose('Non-critical error in _getEventPeers.', err);
      return callback(null, []);
    }
    // TODO: do a more robust check to ensure that the peer is up-to-date
    // with the current blockHeight (block status phase is `consensus`)

    // FIXME: skipping this check because there are restriction on getting
    // blockHeight = 0.  See api.getBlockElectors(L#107) above
    // const blockHeight = 0;
    // async.filter(_.uniq(owners), (owner, callback) =>
    //   api._client.getBlockStatus(blockHeight, owner.id, (err, status) => {
    //     console.log('EEEEEEEEE', err);
    //     console.log('SSSSSSSSSSS', status);
    //     callback(null, !err && status.ledger === ledgerNode.ledger);
    //   }), callback);
    callback(null, _.uniq(owners));
  });
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

function _sha256(x) {
  return crypto.createHash('sha256').update(x).digest('hex');
}

// TODO: remove this or use it where?
function _validateEvents(ledgerNode, hashes, blockHeight, callback) {
  async.auto({
    getEvents: callback => async.map(hashes, (eventHash, callback) =>
      ledgerNode.storage.events.get(eventHash, callback), callback),
    getConfig: ['getEvents', (results, callback) => {
      if(blockHeight > 0) {
        return ledgerNode.storage.events.getLatestConfig(callback);
      }
      // genesis block
      callback(null, results.getEvents.filter(e =>
        e.event.type === 'WebLedgerConfigurationEvent')[0]);
    }],
    validate: ['getConfig', 'getEvents', (results, callback) => {
      const configEvent = results.getConfig.event.ledgerConfiguration;
      if(!(configEvent.eventValidator &&
        configEvent.eventValidator.length > 0)) {
        // no validators for this ledger, pass all events
        return callback(null, results.getEvents);
      }
      const requireEventValidation =
        configEvent.requireEventValidation || false;
      async.filter(results.getEvents, (e, callback) =>
        brLedgerNode.consensus._validateEvent(
          e.event, configEvent.eventValidator, {requireEventValidation},
          err => {
            if(err) {
              // TODO: the event did not pass validation, should the event
              // be retried? marked for deletion?
              // failed events will forever be candidates for inclusion in
              // future blocks until this TODO is addressed
              return callback(null, false);
            }
            callback(null, true);
          }
        ), callback);
    }]
  }, (err, results) => err ? callback(err) : callback(null, {
    hashes: results.validate.map(e => e.meta.eventHash),
    events: results.validate.map(e => e.event)
  }));
}
