/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

// const bedrock = require('bedrock');
const _cacheKey = require('./cache-key');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
// const {config} = bedrock;

const api = {};
module.exports = api;

/**
 * Adds a new operation.
 *
 * @param operation the operation to add.
 * @param ledgerNode the node that is tracking this operation.
 * @param callback(err, record) called once the operation completes.
 */
api.add = ({operation, ledgerNode}, callback) => {
  // TODO: add an operation queue so N operations can be bundled every
  //   period of time T into a single event to reduce overhead
  // const event = {
  //   '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
  //   type: 'WebLedgerOperationEvent',
  //   operation: [operation]
  // };
  const ledgerNodeId = ledgerNode.id;
  const opHash = database.hash(JSON.stringify(operation));
  const opKey = _cacheKey.operation({ledgerNodeId, opHash});
  const opSetKey = _cacheKey.operationSet(ledgerNodeId);
  const opCountKey = _cacheKey.opCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  // FIXME: some tests are inspecting the return value
  cache.client.multi()
    .incr(opCountKey)
    .set(opKey, JSON.stringify({operation, opHash}))
    .sadd(opSetKey, opKey)
    .publish('continuity2017.operation', 'add')
    .exec(err => callback(err));
};
