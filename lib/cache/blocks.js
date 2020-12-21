/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cacheKey');

// FIXME: consider removing participants related code

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
