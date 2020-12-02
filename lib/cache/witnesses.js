/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

/**
 * Adds the witnesses for the given block height to the cache for a ledger
 * node. Only one set of witnesses is ever cached at a time for a ledger node;
 * it includes the latest block height and the witnesses to use.
 *
 * @param witnesses {Object} - The witnesses.
 * @param blockHeight {Number} - The block height.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setWitnesses = async ({
  witnesses, blockHeight, ledgerNodeId
}) => {
  const key = _cacheKey.witnesses(ledgerNodeId);
  return cache.client.set(key, JSON.stringify({blockHeight, witnesses}));
};

/**
 * Get the witnesses from the cache.
 *
 * @param blockHeight {Number} - The block height.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves to the array of witnesses or to `null` if
 *   the witnesses for the blockHeight were not found in the cache.
 */
exports.getWitnesses = async ({blockHeight, ledgerNodeId}) => {
  const key = _cacheKey.witnesses(ledgerNodeId);
  const json = await cache.client.get(key);
  if(!json) {
    return null;
  }
  const {witnesses, blockHeight: _blockHeight} = JSON.parse(json);
  if(blockHeight === _blockHeight) {
    // cache hit
    return witnesses;
  }
  // cache miss
  return null;
};
