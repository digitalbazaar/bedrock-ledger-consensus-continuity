/*!
 * Temporary hack to provide ledger node meta API. Replace once implemented
 * in bedrock-ledger-node.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

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

api.claimState = (ledgerNodeId, state, callback) => {
  // TODO: actual API is likely to use patch-based API
  database.collections.ledgerNode.update({
    id: database.hash(ledgerNodeId)
  }, {
    $set: {
      'meta.consensus-continuity.state': state,
      'meta.updated': Date.now()
    }
  }, database.writeOptions, callback);
};

api.updateState = (ledgerNodeId, state, callback) => {
  // TODO: actual API is likely to use patch-based API
  // Note: only updates if session ID matches
  database.collections.ledgerNode.update({
    id: database.hash(ledgerNodeId),
    'meta.consensus-continuity.state.sessionId': state.sessionId
  }, {
    $set: {
      'meta.consensus-continuity.state': state,
      'meta.updated': Date.now()
    }
  }, database.writeOptions, callback);
};

api.setWaitUntil = (ledgerNodeId, waitUntil, callback) => {
  // TODO: actual API is likely to use patch-based API
  database.collections.ledgerNode.update({
    id: database.hash(ledgerNodeId)
  }, {
    $set: {
      'meta.consensus-continuity.waitUntil': waitUntil,
      'meta.updated': Date.now()
    }
  }, database.writeOptions, callback);
};
