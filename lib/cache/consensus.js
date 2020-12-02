/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
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
  electors, blockHeight, ledgerNodeId
}) => {
  const key = _cacheKey.electors(ledgerNodeId);
  return cache.client.set(key, JSON.stringify({blockHeight, electors}));
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
exports.getElectors = async ({blockHeight, ledgerNodeId}) => {
  const key = _cacheKey.electors(ledgerNodeId);
  const json = await cache.client.get(key);
  if(!json) {
    return null;
  }
  const {electors, blockHeight: _blockHeight} = JSON.parse(json);
  if(blockHeight === _blockHeight) {
    // cache hit
    return electors;
  }
  // cache miss
  return null;
};
