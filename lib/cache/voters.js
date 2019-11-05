/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

/**
 * Adds a voter ID to the cache.
 *
 * @param voterId {string} - The voter ID.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.add = async ({voterId, ledgerNodeId}) => {
  const key = _cacheKey.voter(ledgerNodeId);
  return cache.client.set(key, voterId);
};

/**
 * Gets a voter ID from the cache.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<string|null>} The voter ID or `null` if not found.
 */
exports.get = async ({ledgerNodeId}) => {
  const key = _cacheKey.voter(ledgerNodeId);
  return cache.client.get(key);
};

/**
 * Sets the ledger node ID for a voter ID.
 *
 * @param voterId {string} - The voter ID.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setLedgerNodeId = async ({voterId, ledgerNodeId}) => {
  const key = _cacheKey.ledgerNode(voterId);
  return cache.client.set(key, ledgerNodeId);
};

/**
 * Gets the ledger node ID for a voter ID.
 *
 * @param voterId - The voter ID.
 *
 * @returns {Promise<string|null>} The ledger node ID or `null` if not found.
 */
exports.getLedgerNodeId = async ({voterId}) => {
  const key = _cacheKey.ledgerNode(voterId);
  return cache.client.get(key);
};
