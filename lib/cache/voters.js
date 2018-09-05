/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

const api = {};
module.exports = api;

/**
 * Adds a voter ID to the cache for a given ledger node.
 *
 * @param voterId the voter ID to add to the cache.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.add = async ({voterId, ledgerNodeId}) => {
  const key = _cacheKey.voter(ledgerNodeId);
  return cache.client.set(key, voterId);
};

/**
 * Gets a voter ID from the cache for a given ledger node.
 *
 * @param ledgerNodeId the ID of the ledger node to get the voter ID for.
 *
 * @return a Promise that resolves to the voter ID or `null` if not found.
 */
api.get = async ({ledgerNodeId}) => {
  const key = _cacheKey.voter(ledgerNodeId);
  return cache.client.get(key);
};

/**
 * Sets the ledger node ID for a voter ID.
 *
 * @param voterId the voter ID.
 * @param ledgerNodeId the ID of the ledger node.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.setLedgerNodeId = async ({voterId, ledgerNodeId}) => {
  const key = _cacheKey.ledgerNode(voterId);
  return cache.client.set(key, ledgerNodeId);
};

/**
 * Gets the ledger node ID for a voter ID.
 *
 * @param voterId the ID of the voter to get the ledger node ID for.
 *
 * @return a Promise that resolves to the ledger node ID or `null` if not found.
 */
api.getLedgerNodeId = async ({voterId}) => {
  const key = _cacheKey.ledgerNode(voterId);
  return cache.client.get(key);
};
