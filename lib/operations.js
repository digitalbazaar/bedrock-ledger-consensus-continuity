/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const _util = require('./util');
const async = require('async');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');

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
  const ledgerNodeId = ledgerNode.id;
  const opHash = database.hash(JSON.stringify(operation));
  const opKey = _cacheKey.operation({ledgerNodeId, opHash});
  const opSetKey = _cacheKey.operationSet(ledgerNodeId);
  const opCountKey = _cacheKey.opCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  // FIXME: some tests are inspecting the return value
  async.auto({
    opHash: callback => _util.hasher(operation, callback),
    cache: ['opHash', (results, callback) => cache.client.multi()
      .incr(opCountKey)
      .set(opKey, JSON.stringify({operation, opHash: results.opHash}))
      .sadd(opSetKey, opKey)
      .publish('continuity2017.operation', 'add')
      .exec(callback)
    ]
  }, callback);

};
