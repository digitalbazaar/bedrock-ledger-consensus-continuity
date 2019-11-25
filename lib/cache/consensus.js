/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

/**
 * Adds the electors for the given block height to the cache for a ledger
 * node. Only one set of electors is ever cached at a time for a ledger node;
 * it includes the latest block height and the electors to use.
 *
 * @param electors {Object} - The electors.
 * @param blockHeight {Number} - The block height.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setElectors = async ({
  electors, blockHeight, ledgerNodeId, recoveryMode
}) => {
  const key = _cacheKey.electors(ledgerNodeId);
  return cache.client.set(key, JSON.stringify(
    {blockHeight, electors, recoveryMode}));
};

/**
 * Get the electors from the cache.
 *
 * @param blockHeight {Number} - The block height.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves to the array of electors or to `null` if
 *          the electors for the blockHeight were not found in the cache.
 */
exports.getElectors = async ({blockHeight, ledgerNodeId, recoveryMode}) => {
  const key = _cacheKey.electors(ledgerNodeId);
  const json = await cache.client.get(key);
  if(!json) {
    return null;
  }
  const {electors, blockHeight: _blockHeight, recoveryMode: _recoveryMode} =
    JSON.parse(json);
  if(blockHeight === _blockHeight && recoveryMode === _recoveryMode) {
    // cache hit
    return electors;
  }
  // cache miss
  return null;
};

/**
 * Gets the current priority peers for achieving consensus. Only one set of
 * priority peers is ever cached at a time for a ledger node.
 *
 * @param priorityPeers {Array} - The priority peer IDs.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setPriorityPeers = async ({priorityPeers, ledgerNodeId}) => {
  const key = _cacheKey.priorityPeers(ledgerNodeId);
  const multi = cache.client.multi();
  multi.del(key);
  multi.sadd(key, priorityPeers);
  return multi.exec();
};

/**
 * Gets the current priority peers for achieving consensus. Only one set of
 * priority peers is ever cached at a time for a ledger node.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves to the array of priority peers which will be
 *          empty if the priority peers were not found in the cache.
 */
exports.getPriorityPeers = async ({ledgerNodeId}) => {
  const key = _cacheKey.priorityPeers(ledgerNodeId);
  return cache.client.smembers(key);
};
