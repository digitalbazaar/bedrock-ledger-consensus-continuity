/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

const operationsConfig = require('bedrock')
  .config['ledger-consensus-continuity'].operations;

const api = {};
module.exports = api;

/**
 * Adds an operation to the cache.
 *
 * @param operation the operation to cache.
 * @param operationHash the hash for the operation.
 * @param recordId the `database.hash` of the ID of the record for the
 *          operation.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return {Promise} resolves once the operation completes.
 */
api.add = async ({ledgerNodeId, operation, meta}) => {
  const {basisBlockHeight, operationHash} = meta;
  const opHashKey = _cacheKey.operationHash({ledgerNodeId, operationHash});
  const opKey = _cacheKey.operation(
    {basisBlockHeight, ledgerNodeId, operationHash});
  const opListKey = _cacheKey.operationList(ledgerNodeId);
  const opCountKey = _cacheKey.opCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  return cache.client.multi()
    .incr(opCountKey)
    .expire(opCountKey, operationsConfig.counter.ttl)
    .set(opKey, JSON.stringify({meta, operation}))
    .set(opHashKey, '')
    .rpush(opListKey, opKey)
    .publish(`continuity2017|operation|${ledgerNodeId}`, 'add')
    .exec();
};

api.exists = async ({ledgerNodeId, operationHash}) => {
  const opHashKey = _cacheKey.operationHash({ledgerNodeId, operationHash});
  // watch the opKey whether it exists or not
  await cache.client.watch(opHashKey);
  // the `exists` API returns 0 or 1
  const exists = !!(await cache.client.exists(opHashKey));
  if(exists) {
    // the key already exists remove the watch since it is no longer needed
    // no key is specified in the unwatch API
    await cache.client.unwatch();
  }
  return exists;
};
