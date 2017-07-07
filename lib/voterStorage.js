/*
 * Voter storage for Continuity2017 consensus method.
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

// get logger
const logger = bedrock.loggers.get('app');

bedrock.events.on('bedrock-mongodb.ready', callback => async.auto({
  openCollections: callback =>
    database.openCollections(['continuity2017_voter'], callback),
  createIndexes: ['openCollections', (results, callback) =>
    database.createIndexes([{
      collection: 'continuity2017_voter',
      fields: {ledgerNodeId: 1, 'voter.id': 1},
      options: {unique: true, background: false}
    }], callback)
  ]
}, err => callback(err)));

/**
 * Adds a new voter. Each individual ledger node can be a "voter" and
 * needs voter information including an identifier, a cryptographic key,
 * and a ledger agent. The ledger agent URL is not stored in the database,
 * but computed from the configuration using the ledger agent ID.
 *
 * FIXME: move private key storage to HSM/SSM.
 *
 * @param ledgerNodeId the ID of the ledger node.
 * @param voter the ledger node's voter information:
 *          id the ID of the voter.
 *          ledgerAgentId the ID of the ledger agent for the node.
 *          publicKey the voter's public key.
 *          privateKey the voter's private key.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (ledgerNodeId, voter, callback) => {
  // create the record
  const now = Date.now();
  const record = {
    ledgerNodeId: ledgerNodeId,
    meta: {
      created: now,
      updated: now
    },
    voter: voter
  };

  logger.verbose('adding voter', voter.id);

  const collection = database.collections.continuity2017_voter;
  collection.insert(
    record, database.writeOptions, (err, result) => {
      if(err) {
        if(database.isDuplicateError(err)) {
          return callback(new BedrockError(
            'The voter information for the given ledger node already exists.',
            'DuplicateError', {ledgerNodeId: ledgerNodeId}, err));
        }
        return callback(err);
      }
      callback(null, result.ops[0]);
    });
};

/**
 * Gets the voter information for the given ledger node ID.
 *
 * @param ledgerNodeId the ID of the ledger node.
 * @param callback(err, voter) called when the operation completes.
 */
api.get = (ledgerNodeId, callback) => {
  const query = {
    ledgerNodeId: ledgerNodeId,
    'meta.deleted': {
      $exists: false
    }
  };
  const collection = database.collections.continuity2017_voter;
  collection.findOne(query, {voter: 1}, (err, record) => {
    if(err) {
      return callback(err);
    }

    if(!record) {
      return callback(new BedrockError(
        'Voter information not found for the given ledger node.',
        'NotFound', {ledgerNodeId: ledgerNodeId}));
    }

    callback(null, record.voter);
  });
};
