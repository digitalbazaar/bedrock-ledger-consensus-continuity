/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _util = require('./util');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {callbackify} = require('bedrock').util;
const {config} = bedrock;
const uuid = require('uuid/v4');

const opConfig = config['ledger-consensus-continuity'].operations;

const api = {};
module.exports = api;

/**
 * Adds a new operation. Operations first pass through the LedgerNode API
 * where they are validated using the `operationValidator` defined in the
 * ledger configuration.
 *
 * @param operation the operation to add.
 * @param ledgerNode the node that is tracking this operation.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.add = callbackify(async ({operation, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  // using uuid instead of hash because duplicate operations are allowed
  const opKey = _cacheKey.operation({ledgerNodeId, opId: uuid()});
  const opListKey = _cacheKey.operationList(ledgerNodeId);
  const opCountKey = _cacheKey.opCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  const recordId = _util.generateRecordId({ledgerNode, operation});
  const operationHash = await _util.hasher(operation);
  // TODO: move to redis/cache file/module
  await cache.client.multi()
    .incr(opCountKey)
    .expire(opCountKey, opConfig.counter.ttl)
    .set(opKey, JSON.stringify(
      {operation, meta: {operationHash}, recordId}))
    .rpush(opListKey, opKey)
    .publish(`continuity2017|operation|${ledgerNodeId}`, 'add')
    .exec();
  return {operation, meta: {operationHash}, recordId};
});

api.write = callbackify(async ({eventHash, ledgerNode, operations}) => {
  const records = [];
  for(let i = 0; i < operations.length; ++i) {
    const record = operations[i];
    const meta = bedrock.util.clone(record.meta);
    _.assign(meta, {
      eventOrder: i,
      eventHash
    });
    const {operation, recordId} = record;
    records.push({meta, operation, recordId});
  }
  return ledgerNode.storage.operations.addMany({operations: records});
});
