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
  console.log('9999999999999999');
  const ledgerNodeId = ledgerNode.id;
  const opHash = database.hash(operation);
  const opKey = _cacheKey.operation({ledgerNodeId, opHash});
  const opSetKey = _cacheKey.operationSet(ledgerNodeId);
  // FIXME: some tests are inspecting the return value
  cache.client.multi()
    .set(opKey, JSON.stringify({operation, opHash}))
    .sadd(opSetKey, opKey)
    .exec(err => callback(err));
};
