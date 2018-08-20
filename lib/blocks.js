/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _storage = require('./storage');
const _util = require('./util');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {callbackify, BedrockError} = bedrock.util;
const {config} = bedrock;
const pLimit = require('p-limit');
const {promisify} = require('util');

const api = {};
module.exports = api;

// TODO: document
api.getLatest = callbackify(async (ledgerNode) => {
  const latest = await ledgerNode.storage.blocks.getLatest();
  // mutates block
  await _expandConsensusProofEvents(
    {block: latest.eventBlock.block, ledgerNode});
  return latest;
});

/**
 * Gets the latest consensus block and returns the new proposed block height
 * for the ledger (i.e. the current `blockHeight + 1`) and the latest block
 * hash as what would become the next `previousBlockHash`.
 *
 * @param ledgerNode the ledger node to get the latest block for.
 *
 * @return a Promise that resolves to {blockHeight, previousBlockHash}.
 */
api.getNextBlockInfo = callbackify(async (ledgerNode) => {
  // Note: This consensus method assumes that `blockHeight` will always exist
  // on the previous block because it cannot be used on a blockchain that
  // does not have that information. There has presently been no mechanism
  // devised for switching consensus methods between hashgraph-like blocks
  // and typical blockchains with block heights.
  const block = await ledgerNode.storage.blocks.getLatestSummary();
  const lastBlockHeight = _.get(block, 'eventBlock.block.blockHeight');
  if(lastBlockHeight === undefined) {
    throw new BedrockError(
      'blockHeight is missing from latest block.', 'NotFoundError',
      {block});
  }
  const previousBlockHash = _.get(block, 'eventBlock.meta.blockHash');
  const previousBlockId = _.get(block, 'eventBlock.block.id');
  const blockHeight = lastBlockHeight + 1;
  return {blockHeight, previousBlockHash, previousBlockId};
});

// TODO: document
// find hashes for merge events in last block and remove them from the cache,
//   incr blockHeight
api.repairCache = callbackify(async ({blockHeight, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const {collection} = ledgerNode.storage.events;
  const query = {
    'meta.blockHeight': blockHeight,
    'meta.continuity2017.type': 'm'
  };
  const projection = {_id: 0, 'meta.eventHash': 1};
  const records = await collection.find(query, projection).toArray();
  if(records.length === 0) {
    throw new BedrockError(
      'No merge events were found associated with the given blockHeight.',
      'NotFoundError', {blockHeight, ledgerNodeId});
  }
  // FIXME: does the return value of this function make sense or is it
  // just leaking the results from an old `async.auto`?
  const eventHash = records.map(e => e.meta.eventHash);
  const cache = await _updateCache({hashes: eventHash, ledgerNodeId});
  return {eventHash, cache};
});

// TODO: document
// consensusResult = {event: [event records], consensusProof: [event records]}
api.write = callbackify(async ({ledgerNode, state, consensusResult}) => {
  const {blockHeight, previousBlockId: previousBlock, previousBlockHash} =
    state;
  const ledgerNodeId = ledgerNode.id;
  const {event: {ledgerConfiguration: ledgerConfig}} =
    await ledgerNode.storage.events.getLatestConfig();
  if(ledgerConfig.consensusMethod !== 'Continuity2017') {
    throw new BedrockError(
      'Consensus method must be "Continuity2017".',
      'InvalidStateError', {
        consensusMethod: ledgerConfig.consensusMethod
      });
  }

  // FIXME: remove keys from blocks; unnecessary
  const {ledger: ledgerId} = ledgerConfig;
  const blockId = _generateBlockId({blockHeight, ledgerId});
  const signatureCreators = consensusResult.creators;
  const keys = await Promise.all(
    signatureCreators.map(publicKeyId =>
      _storage.keys.getPublicKey({ledgerNodeId, publicKeyId})));
  // `seeAlso` may have been updated during a prior failed attempt to
  // write the block
  const publicKey = keys.map(key =>
    (key.seeAlso && key.seeAlso !== blockId) ?
      {id: key.id, seeAlso: key.seeAlso} : {
        id: key.id,
        type: key.type,
        owner: key.owner,
        publicKeyBase58: key.publicKeyBase58
      });

  // update events with block information 100 at a time
  const hashes = consensusResult.eventHash;
  let now = Date.now();
  const limit = pLimit(100);
  await Promise.all(hashes.map((eventHash, index) => {
    limit(() => ledgerNode.storage.events.update({
      eventHash,
      patch: [{
        op: 'set',
        changes: {
          meta: {
            blockHeight,
            blockOrder: index,
            consensus: true,
            consensusDate: now,
            updated: now
          }
        }
      }]
    }));
  }));

  const block = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    id: blockId,
    blockHeight,
    consensusMethod: 'Continuity2017',
    type: 'WebLedgerEventBlock',
    eventHash: consensusResult.eventHash,
    consensusProofHash: consensusResult.consensusProofHash,
    previousBlock,
    previousBlockHash,
    publicKey
  };
  const blockHash = await _util.hasher(block);

  // convert `eventHash` predicate to `event`
  // TODO: make internal storage use `eventHash` and convert it to
  //   `event` using consensus plugin
  block.event = block.eventHash;
  delete block.eventHash;

  // TODO: ensure storage supports `consensusProof` event hash lookup
  now = Date.now();
  const blockRecord = {
    block,
    meta: {blockHash, consensus: true, consensusDate: now}
  };

  // FIXME: remove during public key simplification

  // update keys
  // clone to prevent updated key documents from being saved into the block
  const toUpdate = bedrock.util.clone(publicKey)
    .filter(key => !key.seeAlso)
    .map(key => {
      key.seeAlso = blockId;
      return key;
    });
  await Promise.all(toUpdate.map(key => _storage.keys.updatePublicKey(
    ledgerNodeId, key)));

  // FIXME: the events must be checked in the block -- if any of them is
  //   a configuration event, then further processing in the worker should
  //   halt, allowing for a different worker code path to run after the
  //   new configuration has been validated and accepted (or rejected)
  await ledgerNode.storage.blocks.add(blockRecord);

  await _updateCache({hashes: consensusResult.mergeEventHash, ledgerNodeId});

  // FIXME: is returning a boolean necessary?
  return true;
});

api.writeGenesis = callbackify(async ({block, ledgerNode}) => {
  const meta = {
    blockHash: await _util.hasher(block),
    consensus: true,
    consensusDate: Date.now()
  };
  await ledgerNode.storage.blocks.add({block, meta});

  // add block height to cache
  const blockHeightKey = _cacheKey.blockHeight(ledgerNode.id);
  await cache.client.set(blockHeightKey, 0);
});

function _generateBlockId({blockHeight, ledgerId}) {
  return `${ledgerId}/blocks/${blockHeight}`;
}

// FIXME: proofs should be stored and retrieved in the same fashion as events
async function _expandConsensusProofEvents({block, ledgerNode}) {
  if(block.consensusProof) {
    // block already has `consensusProof` set
    return;
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
    return;
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

  // TODO: update driver to get `promise` from `.forEach`
  const cursor = await ledgerNode.storage.events.collection
    .find(query, projection);
  const forEach = promisify(cursor.forEach.bind(cursor));
  await forEach(e => {
    block.consensusProof[eventsToFetch[e.meta.eventHash]] = e.event;
  });
  delete block.consensusProofHash;
}

function _updateCache({hashes, ledgerNodeId}) {
  const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const eventKeys = hashes.map(eventHash => _cacheKey.event(
    {eventHash, ledgerNodeId}));
  return cache.client.multi()
    .srem(outstandingMergeKey, eventKeys)
    .del(eventKeys)
    .incr(blockHeightKey)
    .exec();
}
