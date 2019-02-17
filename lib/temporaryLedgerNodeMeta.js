/*!
 * Temporary hack to provide ledger node meta API. Replace once implemented
 * in bedrock-ledger-node.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const database = require('bedrock-mongodb');
const {promisify} = require('util');
const {BedrockError} = bedrock.util;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await promisify(database.openCollections)(['ledgerNode']);
});

api.get = async ledgerNodeId => {
  const record = await database.collections.ledgerNode.findOne({
    id: database.hash(ledgerNodeId)
  }, {_id: 0, meta: 1});
  if(!record) {
    throw new BedrockError(
      'Ledger node not found.',
      'NotFound',
      {httpStatusCode: 404, ledger: ledgerNodeId, public: true});
  }
  return record.meta;
};

api.setWaitUntil = async (ledgerNodeId, waitUntil) => {
  // TODO: actual API is likely to use patch-based API
  await database.collections.ledgerNode.update({
    id: database.hash(ledgerNodeId)
  }, {
    $set: {
      'meta.consensus-continuity.waitUntil': waitUntil,
      'meta.updated': Date.now()
    }
  }, database.writeOptions);
};
