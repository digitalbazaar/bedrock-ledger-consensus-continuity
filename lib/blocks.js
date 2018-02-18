/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _storage = require('./storage');
const _util = require('./util');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
// const logger = require('./logger');
const {BedrockError} = bedrock.util;

const api = {};
module.exports = api;

api.getLatest = (ledgerNode, callback) => {
  async.auto({
    latest: callback => ledgerNode.storage.blocks.getLatest(callback),
    // mutates block
    expand: ['latest', (results, callback) => _expandConsensusProofEvents(
      {block: results.latest.eventBlock.block, ledgerNode}, callback)]
  }, (err, results) => err ? callback(err) : callback(null, results.latest));
};

/**
 * Gets the latest consensus block and returns the new proposed block height
 * for the ledger (i.e. the current `blockHeight + 1`) and the latest block
 * hash as what would become the next `previousBlockHash`.
 *
 * @param ledgerNode the ledger node to get the latest block for.
 * @param callback(err, {blockHeight, previousBlockHash}) called once the
 *          operation completes.
 */
api.getNextBlockInfo = (ledgerNode, callback) => {
  // Note: This consensus method assumes that `blockHeight` will always exist
  // on the previous block because it cannot be used on a blockchain that
  // does not have that information. There has presently been no mechanism
  // devised for switching consensus methods between hashgraph-like blocks
  // and typical blockchains with block heights.
  ledgerNode.storage.blocks.getLatestSummary((err, block) => {
    if(err) {
      return callback(err);
    }
    const previousBlockHash = _.get(block, 'eventBlock.meta.blockHash');
    const last = _.get(block, 'eventBlock.block.blockHeight');
    if(last === undefined) {
      return callback(new BedrockError(
        'blockHeight is missing from latest block.', 'NotFoundError', {
          block
        }));
    }
    callback(null, {
      blockHeight: last + 1,
      previousBlockHash,
      previousBlockId: _.get(block, 'eventBlock.block.id')
    });
  });
};

// TODO: document
// consensusResult = {event: [event records], consensusProof: [event records]}
api.write = ({ledgerNode, state, consensusResult}, callback) => {
  async.auto({
    config: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) => {
        if(err) {
          return callback(err);
        }
        const config = result.event.ledgerConfiguration;
        if(config.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'Consensus method must be "Continuity2017".',
            'InvalidStateError', {
              consensusMethod: config.consensusMethod
            }));
        }
        callback(null, config);
      }),
    keys: callback => {
      const signatureCreators = consensusResult.creators;
      async.map(
        signatureCreators, (publicKeyId, callback) =>
          _storage.keys.getPublicKey(
            {ledgerNodeId: ledgerNode.id, publicKeyId}, callback),
        (err, result) => {
          if(err) {
            return callback(err);
          }
          const publicKey = result.map(key => key.seeAlso ?
            {id: key.id, seeAlso: key.seeAlso} : {
              id: key.id,
              type: key.type,
              owner: key.owner,
              publicKeyBase58: key.publicKeyBase58
            });
          callback(null, publicKey);
        });
    },
    createBlock: ['config', 'keys', (results, callback) => {
      const blockHeight = state.blockHeight;
      const block = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        id: results.config.ledger + '/blocks/' + blockHeight,
        blockHeight,
        consensusMethod: 'Continuity2017',
        type: 'WebLedgerEventBlock',
        eventHash: consensusResult.eventHash,
        consensusProofHash: consensusResult.consensusProofHash,
        previousBlock: state.previousBlockId,
        previousBlockHash: state.previousBlockHash,
        publicKey: results.keys
      };
      _util.hasher(block, (err, blockHash) => {
        if(err) {
          return callback(err);
        }
        // convert `eventHash` predicate to `event`
        // TODO: make internal storage use `eventHash` and convert it to
        //   `event` using consensus plugin
        block.event = block.eventHash;
        delete block.eventHash;

        // TODO: ensure storage supports `consensusProof` event hash lookup

        callback(null, {
          block: block,
          meta: {blockHash}
        });
      });
    }],
    store: ['createBlock', (results, callback) =>
      ledgerNode.storage.blocks.add(
        results.createBlock.block, results.createBlock.meta, callback)],
    postWrite: ['store', (results, callback) => _postWriteBlock({
      ledgerNode,
      // voter,
      state,
      blockId: results.createBlock.block.id,
      blockHash: results.createBlock.meta.blockHash,
      keys: results.keys,
      consensusResult
    }, callback)]
  }, err => callback(err, err ? false : true));
};

function _expandConsensusProofEvents({block, ledgerNode}, callback) {
  if(block.consensusProof) {
    // block already has `consensusProof` set
    return callback();
  }

  // find all events that must be fetched
  const events = block.consensusProofHash || [];
  const eventsToFetch = {};
  for(let i = 0; i < events.length; ++i) {
    const event = events[i];
    // TODO: Determine if we want to expand other hash types
    const isEventHash = typeof(event) === 'string' &&
      event.startsWith('ni:///');
    if(isEventHash) {
      eventsToFetch[event] = i;
    }
  }
  const hashes = Object.keys(eventsToFetch);
  if(hashes.length === 0) {
    // no event hashes to fetch
    return callback();
  }

  // get all event hashes from event collection
  const query = {
    eventHash: {
      $in: hashes
    }
  };
  const projection = {
    event: 1,
    'meta.eventHash': 1,
    _id: 0
  };
  block.consensusProof = [];
  ledgerNode.storage.events.collection.find(query, projection).forEach(e => {
    block.consensusProof[eventsToFetch[e.meta.eventHash]] = e.event;
  }, err => {
    if(err) {
      return callback(err);
    }
    delete block.consensusProofHash;
    callback();
  });
}

api.finishIncompleteBlock = ({ledgerNode, state}, callback) => {
  async.auto({
    // check to see if a write block was previously interrupted
    incompleteBlock: callback =>
      ledgerNode.storage.blocks.getSummaryByHeight(
        state.blockHeight, {consensus: false, eventHash: true},
        (err, summary) => {
          if(err && err.name === 'NotFoundError') {
            err = null;
          }
          // FIXME: remove
          if(summary) {
            throw new Error('INCOMPLETE BLOCK');
          }
          callback(err, summary);
        }),
    consensusResult: ['incompleteBlock', (results, callback) => {
      if(!results.incompleteBlock) {
        return callback();
      }
      // TODO: ensure block is a continuity block? (handle corner-case where
      //   consenus mechanisms have changed over time and a really stale
      //   non-consensus fork from an alternative PoW method is found)

      // rebuild consensus result
      const consensusResult = {};
      consensusResult.event = results.incompleteBlock.block.eventHash.map(
        hash => ({eventHash: hash}));
      const proofEvents = results.incompleteBlock.block.consensusProof;

      // TODO: support getting consensus proof hashes from storage in summary

      // compute hashes for consensus proof
      consensusResult.consensusProof = [];
      async.eachSeries(proofEvents, (event, callback) =>
        _util.hasher(event, (err, hash) => {
          if(err) {
            return callback(err);
          }
          consensusResult.consensusProof.push({eventHash: hash});
          callback();
        }), err => callback(err, consensusResult));
    }],
    finishBlock: ['consensusResult', (results, callback) => {
      if(!results.incompleteBlock) {
        return callback();
      }
      // finish post block write
      _postWriteBlock({
        ledgerNode,
        blockId: results.incompleteBlock.block.id,
        blockHash: results.incompleteBlock.meta.blockHash,
        consensusResult: results.consensusResult,
        incompleteBlock: results.incompleteBlock.block
      }, callback);
    }]
  }, callback);
};

function _postWriteBlock(
  {ledgerNode, blockId, blockHash,
    keys, consensusResult, incompleteBlock},
  callback) {
  async.auto({
    keys: callback => {
      if(keys) {
        return callback(null, keys);
      }
      // lookup keys in incomplete block
      const blockKeys = incompleteBlock.publicKey.forEach(key => key.id);
      async.map(blockKeys, (keyId, callback) => _storage.keys.getPublicKey(
        ledgerNode.id, {id: keyId}, callback), (err, result) => {
        if(err) {
          return callback(err);
        }
        const publicKey = result.map(key => key.seeAlso ?
          {id: key.id, seeAlso: key.seeAlso} : {
            id: key.id,
            type: key.type,
            owner: key.owner,
            publicKeyBase58: key.publicKeyBase58
          });
        callback(null, publicKey);
      });
    },
    updateKey: ['keys', (results, callback) => {
      const toUpdate = results.keys
        .filter(key => !key.seeAlso)
        .map(key => {
          key.seeAlso = blockId;
          return key;
        });
      async.each(toUpdate, (key, callback) => _storage.keys.updatePublicKey(
        ledgerNode.id, key, callback), callback);
    }],
    /* Note: Update events must occur *after* creating the non-consensus block.
    Once events are marked as having achieved consensus, the consenus algorithm
    will build a different recent history to work off of. Therefore, writing
    the non-consensus block first allows it to act as a guard against a
    potential inconsistency. We always look (above) for this non-consensus
    block before running the consensus algorithm as a check for whether or not
    a previous write block operation did not complete fully for any reason
    (e.g. a mongo timeout). */
    updateEvents: ['updateKey', (results, callback) =>
      async.each(consensusResult.eventHash, (hash, callback) => {
        const now = Date.now();
        ledgerNode.storage.events.update(hash, [{
          op: 'unset',
          changes: {
            meta: {
              pending: true
            }
          }
        }, {
          op: 'set',
          changes: {
            meta: {
              consensus: true,
              consensusDate: now,
              updated: now
            }
          }
        }], callback);
      }, callback)],
    cache: ['updateEvents', (results, callback) => {
      const ledgerNodeId = ledgerNode.id;
      const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
      const eventKeys = consensusResult.eventHash
        .map(eventHash => _cacheKey.event({eventHash, ledgerNodeId}));
      cache.client.multi()
        .srem(outstandingMergeKey, eventKeys)
        .del(eventKeys)
        .exec(callback);
    }],
    updateBlock: ['cache', (results, callback) => {
      const patch = [{
        op: 'set',
        changes: {meta: {consensus: true, consensusDate: Date.now()}}
      }];
      ledgerNode.storage.blocks.update(blockHash, patch, callback);
    }],
    // updateState: ['updateBlock', (results, callback) => {
    //   _updateState({ledgerNode, state, voter}, callback);
    // }]
    // FIXME: the events must be checked in the block -- if any of them is
    //   a configuration event, then further processing in the worker should
    //   halt, allowing for a different worker code path to run after the
    //   new configuration has been validated and accepted (or rejected)
  }, err => callback(err, err ? false : true));
}
