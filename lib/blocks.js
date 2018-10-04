/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('./cache');
const _util = require('./util');
const bedrock = require('bedrock');
const {callbackify, BedrockError} = bedrock.util;
const {config} = bedrock;
const pLimit = require('p-limit');

const api = {};
module.exports = api;

/**
 * Get all peers that participated in the block at the given `blockHeight`.
 *
 * @param blockHeight the block height to get the participants for.
 * @param ledgerNode the ledger node to check.
 *
 * @return a Promise that resolves to an object with:
 *           consensusProofPeers - peers that participated in consensus proving.
 *           mergeEventPeers - peers that participated with merge events.
 */
api.getParticipants = callbackify(async ({blockHeight, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  // special case genesis block -- only need to get mergeEventPeers, there is
  // only one and it is the same as the consensusProofPeers
  const {getMergeEventPeers} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  if(blockHeight === 0) {
    const mergeEventPeers = await getMergeEventPeers({blockHeight});
    const consensusProofPeers = mergeEventPeers.slice();
    return {consensusProofPeers, mergeEventPeers};
  }

  // any other block
  // return cached particants if available
  const cachedParticipants = await _cache.blocks.getParticipants(
    {blockHeight, ledgerNodeId});
  if(cachedParticipants) {
    return cachedParticipants;
  }
  // cache miss
  const {getConsensusProofPeers} = ledgerNode.storage.blocks
    .plugins['continuity-storage'];
  const [consensusProofPeers, mergeEventPeers] = await Promise.all([
    getConsensusProofPeers({blockHeight}),
    getMergeEventPeers({blockHeight})
  ]);

  // update the cache
  await _cache.blocks.setParticipants(
    {blockHeight, consensusProofPeers, ledgerNodeId, mergeEventPeers});

  return {consensusProofPeers, mergeEventPeers};
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
  const {getMergeEventHashes} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  const eventHashes = await getMergeEventHashes({blockHeight});
  if(eventHashes.length === 0) {
    throw new BedrockError(
      'No merge events were found associated with the given blockHeight.',
      'NotFoundError', {blockHeight, ledgerNodeId});
  }
  const cache = await _cache.blocks.commitBlock({eventHashes, ledgerNodeId});
  // the return values are used *only* in unit tests
  return {cache, eventHashes};
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

  await ledgerNode.storage.blocks.add(blockRecord);

  await _cache.blocks.commitBlock(
    {eventHashes: consensusResult.mergeEventHash, ledgerNodeId});

  const hasLedgerConfigEvent = await ledgerNode.storage.events.hasEvent(
    {blockHeight, type: 'WebLedgerConfigurationEvent'});

  return {blockHeight, hasLedgerConfigEvent};
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
