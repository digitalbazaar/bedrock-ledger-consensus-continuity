/*!
 * Voter storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const continuityStorage = require('./storage');
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
      fields: {'voter.id': 1},
      options: {unique: true, background: false}
    }, {
      collection: 'continuity2017_voter',
      fields: {'voter.ledgerNodeId': 1},
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
 * @param voter the ledger node's voter information:
 *          id the ID of the voter.
 *          ledgerNodeId the ID of the ledger node.
 *          publicKey the voter's public key.
 *          privateKey the voter's private key.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (voter, callback) => {
  // create the record
  const now = Date.now();
  const record = {
    meta: {
      created: now,
      updated: now
    },
    voter: voter
  };

  logger.verbose('adding voter', voter.id);

  const collection = database.collections.continuity2017_voter;
  async.auto({
    // NOTE: storing key because it should be in cache.
    key: callback => {
      const publicKey = bedrock.util.clone(record.voter.publicKey);
      publicKey.owner = record.voter.id;
      publicKey['@context'] = config.constants.WEB_LEDGER_CONTEXT_V1_URL;
      delete publicKey.privateKey;
      continuityStorage.keys.addPublicKey(
        voter.ledgerNodeId, publicKey, err => {
          if(err && database.isDuplicateError(err)) {
            return callback();
          }
          callback();
        });
    },
    voter: ['key', (results, callback) => collection.insert(
      record, database.writeOptions, (err, result) => {
        if(err) {
          if(database.isDuplicateError(err)) {
            // TODO: how to handle key rotation?
            return callback(new BedrockError(
              'The voter information for the given ledger node already exists.',
              'DuplicateError', {ledgerNodeId: voter.ledgerNodeId}, err));
          }
          return callback(err);
        }
        callback(null, result.ops[0]);
      })]
  }, (err, results) => err ? callback(err) : callback(null, results.voter));
};

/**
 * Gets the voter information for the given ledger node ID or voter ID.
 *
 * @param options the options to use:
 *          [ledgerNodeId] the ID of the ledger node.
 *          [voterId] the ID of the voter.
 * @param callback(err, voter) called when the operation completes.
 */
api.get = ({
  ledgerNodeId, privateKey = false, publicKey = false, voterId
}, callback) => {
  if(!(ledgerNodeId || voterId)) {
    throw new Error('`ledgerNodeId` or `voterId` must be set.');
  }

  const query = {'meta.deleted': {$exists: false}};
  if(ledgerNodeId) {
    query['voter.ledgerNodeId'] = ledgerNodeId;
  } else {
    query['voter.id'] = voterId;
  }

  const collection = database.collections.continuity2017_voter;
  const projection = {
    _id: 0,
    'voter.id': 1,
    'voter.ledgerNodeId': 1,
  };
  if(publicKey) {
    _.assign(projection, {
      'voter.publicKey.id': 1,
      'voter.publicKey.publicKeyBase58': 1
    });
  }
  if(privateKey) {
    // the private key and the associated `id` resides in `publicKey`
    _.assign(projection, {
      'voter.publicKey.id': 1,
      'voter.publicKey.privateKey.privateKeyBase58': 1
    });
  }
  collection.findOne(query, projection, (err, record) => {
    if(err) {
      return callback(err);
    }

    if(!record) {
      const details = {
        httpStatusCode: 400,
        public: true
      };
      if(ledgerNodeId) {
        details.ledgerNodeId = ledgerNodeId;
      } else {
        details.voterId = voterId;
      }

      return callback(new BedrockError(
        'Voter information not found for the given ledger node.',
        'NotFoundError', details));
    }

    callback(null, record.voter);
  });
};
