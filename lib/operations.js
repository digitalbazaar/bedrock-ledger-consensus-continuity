/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _util = require('./util');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
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
 * @param callback(err, record) called once the operation completes.
 */
api.add = ({operation, ledgerNode}, callback) => {
  const ledgerNodeId = ledgerNode.id;
  // using uuid instead of hash because duplicate operations are allowed
  const opKey = _cacheKey.operation({ledgerNodeId, opId: uuid()});
  const opListKey = _cacheKey.operationList(ledgerNodeId);
  const opCountKey = _cacheKey.opCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  const recordId = _util.generateRecordId({ledgerNode, operation});
  // NOTE: some tests are inspecting the return value
  async.auto({
    operationHash: callback => _util.hasher(operation, callback),
    cache: ['operationHash', (results, callback) => {
      const {operationHash} = results;
      cache.client.multi()
        .incr(opCountKey)
        .expire(opCountKey, opConfig.counter.ttl)
        .set(opKey, JSON.stringify(
          {operation, meta: {operationHash}, recordId}))
        .rpush(opListKey, opKey)
        .publish(`continuity2017|operation|${ledgerNodeId}`, 'add')
        .exec(callback);
    }]
  }, callback);
};

api.write = ({eventHash, ledgerNode, operations}, callback) => {
  const records = [];
  for(let i = 0; i < operations.length; ++i) {
    const meta = bedrock.util.clone(operations[i].meta);
    _.assign(meta, {
      eventOrder: i,
      eventHash: eventHash
    });
    const {operation, recordId} = operations[i];
    records.push({meta, operation, recordId});
  }
  ledgerNode.storage.operations.addMany({operations: records}, callback);
};
