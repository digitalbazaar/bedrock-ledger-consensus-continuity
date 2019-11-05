/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');
const {util: {BedrockError}} = require('bedrock');

/**
 * Get the latest block height.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Number>} The latest block height as an integer.
 */
exports.blockHeight = async ledgerNodeId => {
  const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
  const result = await cache.client.get(blockHeightKey);
  if(result === null) {
    throw new BedrockError(
      'Block height key is missing from the cache.',
      'InvalidStateError', {ledgerNodeId});
  }
  return parseInt(result, 10);
};

/**
 * Commits a block:
 * - remove events that have acheived consensus from the set of outstanding
 *   merge events
 * - remove cached merge event records
 * - increment the cached blockHeight
 *
 * @param eventHashes  {string[]} - The hashes of the merge events in the block.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.commitBlock = async ({eventHashes, ledgerNodeId}) => {
  const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const eventKeys = eventHashes.map(
    eventHash => _cacheKey.event({eventHash, ledgerNodeId}));
  return cache.client.multi()
    .srem(outstandingMergeKey, eventKeys)
    .del(eventKeys)
    .incr(blockHeightKey)
    .exec();
};

/**
 * Get block participation information.
 *
 * @param blockHeight {Number} - The block height to get.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} includes consensusProofPeers and mergeEventPeers.
 */
exports.getParticipants = async ({blockHeight, ledgerNodeId}) => {
  const key = _cacheKey.blockParticipants(ledgerNodeId);
  const json = await cache.client.get(key);
  if(!json) {
    return null;
  }
  const {blockHeight: _blockHeight, consensusProofPeers, mergeEventPeers} =
    JSON.parse(json);

  // ensure that the cache contains data pertaining to the proper blockHeight
  if(blockHeight !== _blockHeight) {
    return null;
  }

  return {consensusProofPeers, mergeEventPeers};
};

/**
 * Sets the latest block height in the cache.
 *
 * @param blockHeight {Number} - The block height to set.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setBlockHeight = async ({blockHeight, ledgerNodeId}) => {
  const key = _cacheKey.blockHeight(ledgerNodeId);
  return cache.client.set(key, blockHeight);
};

/**
 * Store block participation information.
 *
 * @param blockHeight {Number} - The block height to set.
 * @param consensusProofPeers {string[]} - A list of peers.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param mergeEventPeers {string[]} - A list of peers.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setParticipants = async (
  {blockHeight, consensusProofPeers, ledgerNodeId, mergeEventPeers}) => {
  const key = _cacheKey.blockParticipants(ledgerNodeId);
  const json = JSON.stringify(
    {blockHeight, consensusProofPeers, mergeEventPeers});
  return cache.client.set(key, json);
};
