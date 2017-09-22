/*
 * Temporary hack to provide ledger node meta API. Replace once implemented
 * in bedrock-ledger-node.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const database = require('bedrock-mongodb');
const BedrockError = bedrock.util.BedrockError;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-mongodb.ready', callback => async.auto({
  openCollections: callback =>
    database.openCollections(['ledgerNode'], callback)
}, err => callback(err)));

api.get = (ledgerNodeId, callback) => {
  database.collections.ledgerNode.findOne({
    id: database.hash(ledgerNodeId)
  }, {meta: 1}, (err, record) => {
    if(err) {
      return callback();
    }
    if(!record) {
      return callback(new BedrockError(
        'Ledger node not found.',
        'NotFound',
        {httpStatusCode: 404, ledger: ledgerNodeId, public: true}));
    }
    callback(null, record.meta);
  });
};

api.setWaitUntil = (ledgerNodeId, waitUntil, callback) => {
  // TODO: actual API is likely to use patch-based API
  database.collections.ledgerNode.update({
    id: database.hash(ledgerNodeId)
  }, {
    $set: {
      'meta.consensus-continuity.waitUntil': waitUntil
    }
  }, database.writeOptions, callback);
};
