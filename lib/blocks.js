/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('./cache');
const _storage = require('./storage');
const _util = require('./util');
const bedrock = require('bedrock');
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
// ensure block is committed to cache
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
  const cache = await _cache.blocks.commitBlock(
    {eventHashes: eventHash, ledgerNodeId});
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

  // update events with block information up to 100 at a time
  const hashes = consensusResult.eventHash;
  let now = Date.now();
  const limit = pLimit(100);
  await Promise.all(hashes.map((eventHash, index) =>
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
    }))));

  // create new block ID
  const {ledger: ledgerId} = ledgerConfig;
  const blockId = _generateBlockId({blockHeight, ledgerId});

  const block = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    id: blockId,
    blockHeight,
    consensusMethod: 'Continuity2017',
    type: 'WebLedgerEventBlock',
    eventHash: consensusResult.eventHash,
    consensusProofHash: consensusResult.consensusProofHash,
    previousBlock,
    previousBlockHash
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

  // FIXME: the events must be checked in the block -- if any of them is
  //   a configuration event, then further processing in the worker should
  //   halt, allowing for a different worker code path to run after the
  //   new configuration has been validated and accepted (or rejected)
  await ledgerNode.storage.blocks.add(blockRecord);

  await _cache.blocks.commitBlock(
    {eventHashes: consensusResult.mergeEventHash, ledgerNodeId});

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

  // add initial block height to cache
  await _cache.blocks.setBlockHeight(
    {blockHeight: 0, ledgerNodeId: ledgerNode.id});
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
