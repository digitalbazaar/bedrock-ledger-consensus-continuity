/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');
const uuid = require('uuid/v4');

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
  // using uuid instead of hash because duplicate operations are allowed
  const {basisBlockHeight} = meta;
  const opKey = _cacheKey.operation(
    {basisBlockHeight, ledgerNodeId, opId: uuid()});
  const opListKey = _cacheKey.operationList(ledgerNodeId);
  const opCountKey = _cacheKey.opCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  return cache.client.multi()
    .incr(opCountKey)
    .expire(opCountKey, operationsConfig.counter.ttl)
    .set(opKey, JSON.stringify({meta, operation}))
    .rpush(opListKey, opKey)
    .publish(`continuity2017|operation|${ledgerNodeId}`, 'add')
    .exec();
};
