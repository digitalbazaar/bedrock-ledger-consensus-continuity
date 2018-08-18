/*!
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
const hasher = require('bedrock-ledger-node').consensus._hasher;
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
    const lastBlockHeight = _.get(block, 'eventBlock.block.blockHeight');
    if(lastBlockHeight === undefined) {
      return callback(new BedrockError(
        'blockHeight is missing from latest block.', 'NotFoundError',
        {block}));
    }
    const previousBlockHash = _.get(block, 'eventBlock.meta.blockHash');
    const previousBlockId = _.get(block, 'eventBlock.block.id');
    const blockHeight = lastBlockHeight + 1;
    callback(null, {blockHeight, previousBlockHash, previousBlockId});
  });
};

// TODO: document
// find hashes for merge events in last block and remove them from the cache,
//   incr blockHeight
api.repairCache = ({blockHeight, ledgerNode}, callback) => {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    eventHash: callback => {
      const {collection} = ledgerNode.storage.events;
      const query = {
        'meta.blockHeight': blockHeight,
        'meta.continuity2017.type': 'm'
      };
      const projection = {_id: 0, 'meta.eventHash': 1};
      collection.find(query, projection).toArray((err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.length === 0) {
          return callback(new BedrockError(
            'No merge events were found associated with the given blockHeight.',
            'NotFoundError', {blockHeight, ledgerNodeId}));
        }
        callback(null, result.map(e => e.meta.eventHash));
      });
    },
    cache: ['eventHash', (results, callback) => {
      const {eventHash: hashes} = results;
      _updateCache({hashes, ledgerNodeId}, callback);
    }]
  }, callback);
};

// TODO: document
// consensusResult = {event: [event records], consensusProof: [event records]}
api.write = ({ledgerNode, state, consensusResult}, callback) => {
  const {blockHeight, previousBlockId: previousBlock, previousBlockHash} =
    state;
  const ledgerNodeId = ledgerNode.id;
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
    keys: ['config', (results, callback) => {
      const {ledger: ledgerId} = results.config;
      const blockId = _generateBlockId({blockHeight, ledgerId});
      const signatureCreators = consensusResult.creators;
      async.map(
        signatureCreators, (publicKeyId, callback) =>
          _storage.keys.getPublicKey({ledgerNodeId, publicKeyId}, callback),
        (err, result) => {
          if(err) {
            return callback(err);
          }
          // `seeAlso` may have been updated during a prior failed attempt to
          // write the block
          const publicKey = result.map(key =>
            (key.seeAlso && key.seeAlso !== blockId) ?
              {id: key.id, seeAlso: key.seeAlso} : {
                id: key.id,
                type: key.type,
                owner: key.owner,
                publicKeyBase58: key.publicKeyBase58
              });
          callback(null, publicKey);
        });
    }],
    events: ['config', (results, callback) => {
      const hashes = consensusResult.eventHash;
      const now = Date.now();
      async.timesLimit(hashes.length, 100, (i, callback) => {
        const eventHash = hashes[i];
        ledgerNode.storage.events.update({
          eventHash,
          patch: [{
            op: 'set',
            changes: {
              meta: {
                blockHeight,
                blockOrder: i,
                consensus: true,
                consensusDate: now,
                updated: now
              }
            }
          }]
        }, callback);
      }, callback);
    }],
    createBlock: ['config', 'events', 'keys', (results, callback) => {
      const {ledger: ledgerId} = results.config;
      const block = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        id: _generateBlockId({blockHeight, ledgerId}),
        blockHeight,
        consensusMethod: 'Continuity2017',
        type: 'WebLedgerEventBlock',
        eventHash: consensusResult.eventHash,
        consensusProofHash: consensusResult.consensusProofHash,
        previousBlock,
        previousBlockHash,
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
        const now = Date.now();
        callback(null, {
          block: block,
          meta: {blockHash, consensus: true, consensusDate: now}
        });
      });
    }],
    updateKey: ['createBlock', (results, callback) => {
      const {id: blockId} = results.createBlock.block;
      // clone to prevent updated key documents from being saved into the block
      const toUpdate = bedrock.util.clone(results.keys)
        .filter(key => !key.seeAlso)
        .map(key => {
          key.seeAlso = blockId;
          return key;
        });
      async.each(toUpdate, (key, callback) => _storage.keys.updatePublicKey(
        ledgerNodeId, key, callback), callback);
    }],
    // FIXME: the events must be checked in the block -- if any of them is
    //   a configuration event, then further processing in the worker should
    //   halt, allowing for a different worker code path to run after the
    //   new configuration has been validated and accepted (or rejected)
    store: ['updateKey', (results, callback) =>
      ledgerNode.storage.blocks.add({
        block: results.createBlock.block, meta: results.createBlock.meta
      }, callback)],
    cache: ['store', (results, callback) => {
      const hashes = consensusResult.mergeEventHash;
      _updateCache({hashes, ledgerNodeId}, callback);
    }],
  }, err => callback(err, err ? false : true));
};

api.writeGenesis = ({block, ledgerNode}, callback) => async.auto({
  hashBlock: callback => hasher(block, callback),
  writeBlock: ['hashBlock', (results, callback) => {
    const meta = {
      blockHash: results.hashBlock,
      consensus: true,
      consensusDate: Date.now()
    };
    ledgerNode.storage.blocks.add({block, meta}, callback);
  }],
  cache: ['writeBlock', (results, callback) => {
    const blockHeightKey = _cacheKey.blockHeight(ledgerNode.id);
    cache.client.set(blockHeightKey, 0, callback);
  }]
}, callback);

function _generateBlockId({blockHeight, ledgerId}) {
  return `${ledgerId}/blocks/${blockHeight}`;
}

// TODO: proofs should be stored and retrieved in the same fashion as events
function _expandConsensusProofEvents({block, ledgerNode}, callback) {
  if(block.consensusProof) {
    // block already has `consensusProof` set
    return callback();
  }

  // find all events that must be fetched
  const events = block.consensusProofHash || [];
  const eventsToFetch = {};
  for(let i = 0; i < events.length; ++i) {
    eventsToFetch[events[i]] = i;
  }
  const hashes = Object.keys(eventsToFetch);
  if(hashes.length === 0) {
    // no event hashes to fetch
    return callback();
  }

  // get all event hashes from event collection
  const query = {
    'meta.eventHash': {
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

function _updateCache({hashes, ledgerNodeId}, callback) {
  const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const eventKeys = hashes.map(eventHash => _cacheKey.event(
    {eventHash, ledgerNodeId}));
  cache.client.multi()
    .srem(outstandingMergeKey, eventKeys)
    .del(eventKeys)
    .incr(blockHeightKey)
    .exec(callback);
}
