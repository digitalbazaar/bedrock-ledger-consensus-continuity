/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

const api = {};
module.exports = api;

/**
 * Adds the electors for the given block height to the cache for a ledger
 * node. Only one set of electors is ever cached at a time for a ledger node;
 * it includes the latest block height and the electors to use.
 *
 * @param electors the electors.
 * @param blockHeight the block height.
 * @param ledgerNodeId the ID of the ledger node.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.setElectors = async (
  {electors, blockHeight, ledgerNodeId, recoveryMode}) => {
  const key = _cacheKey.electors(ledgerNodeId);
  return cache.client.set(key, JSON.stringify(
    {blockHeight, electors, recoveryMode}));
};

/**
 * Attempts to get the electors for the given blockHeight from the cache.
 *
 * @param blockHeight the block height.
 * @param ledgerNodeId the ID of the ledger node.
 *
 * @return a Promise that resolves to the array of electors or to `null` if
 *         the electors for the blockHeight were not found in the cache.
 */
api.getElectors = async ({blockHeight, ledgerNodeId, recoveryMode}) => {
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
