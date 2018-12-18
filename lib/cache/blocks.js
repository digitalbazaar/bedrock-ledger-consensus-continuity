/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');
const {util: {BedrockError}} = require('bedrock');

const api = {};
module.exports = api;

api.blockHeight = async ledgerNodeId => {
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
 * Commits a block by removing outstanding merge events from the event cache
 * and updating the block height after a successful merge.
 *
 * @param eventHashes the hashes of the merge events from the block to commit.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return {Promise} resolves once the operation completes.
 */
api.commitBlock = async ({eventHashes, ledgerNodeId}) => {
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
 * Stores block participation information for the given blockHeight.
 *
 * @param blockHeight the block height to set.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return {Promise} includes consensusProofPeers and mergeEventPeers.
 */
api.getParticipants = async ({blockHeight, ledgerNodeId}) => {
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
 * Sets the current block height in the cache for a ledger node.
 *
 * @param blockHeight the block height to set.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return {Promise} resolves once the operation completes.
 */
api.setBlockHeight = async ({blockHeight, ledgerNodeId}) => {
  const key = _cacheKey.blockHeight(ledgerNodeId);
  return cache.client.set(key, blockHeight);
};

/**
 * Stores block participation information for the given blockHeight.
 *
 * @param blockHeight the block height to set.
 * @param consensusProofPeers {string[]} a list of peers.
 * @param ledgerNodeId the ID of the ledger node to update.
 * @param mergeEventPeers {string[]} a list of peers.
 *
 * @return {Promise} resolves once the operation completes.
 */
api.setParticipants = async (
  {blockHeight, consensusProofPeers, ledgerNodeId, mergeEventPeers}) => {
  const key = _cacheKey.blockParticipants(ledgerNodeId);
  const json = JSON.stringify(
    {blockHeight, consensusProofPeers, mergeEventPeers});
  return cache.client.set(key, json);
};
