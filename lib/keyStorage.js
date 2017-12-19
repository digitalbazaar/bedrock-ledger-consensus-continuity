/*
 * Key storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const database = require('bedrock-mongodb');
const logger = require('./logger');
const BedrockError = bedrock.util.BedrockError;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-mongodb.ready', callback => async.auto({
  openCollections: callback => database.openCollections(
    ['continuity2017_key'], callback),
  createIndexes: ['openCollections', (results, callback) =>
    database.createIndexes([{
      collection: 'continuity2017_key',
      fields: {ledgerNodeId: 1, id: 1},
      options: {unique: true, background: false}
    }], callback)]
}, err => callback(err)));

/**
 * Adds a new PublicKey.
 *
 * @param ledgerNodeId the ID of the node that is tracking this publicKey.
 * @param publicKey the publicKey to add, with no ID yet set.
 * @param callback(err, record) called once the operation completes.
 */
api.addPublicKey = (ledgerNodeId, publicKey, callback) => {
  if(!('id' in publicKey)) {
    return callback(new BedrockError(
      '`id` is a required property.', 'DataError', {
        ledgerNodeId,
        publicKey,
      }));
  }

  // if no type given add it
  if(!('type' in publicKey)) {
    publicKey.type = 'CryptographicKey';
  }

  // log prior to adding private key
  logger.debug('adding public key', publicKey);

  // insert the publc key
  const now = Date.now();
  const record = {
    id: database.hash(publicKey.id),
    ledgerNodeId,
    owner: database.hash(publicKey.owner),
    meta: {
      created: now,
      updated: now
    },
    publicKey: publicKey
  };
  database.collections['continuity2017_key'].insert(
    record, database.writeOptions, (err, result) => {
      if(err && database.isDuplicateError(err)) {
        err = new BedrockError(
          'Duplicate key.', 'DuplicateError', {publicKey: record});
      }
      if(err) {
        return callback(err);
      }
      callback(null, result.ops[0]);
    });
};

/**
 * Retrieves a PublicKey.
 *
 * @param ledgerNodeId the ID of the node that is tracking this publicKey.
 * @param publicKey the PublicKey with 'id' set.
 * @param callback(err, publicKey) called once the operation completes.
 */
api.getPublicKey = (ledgerNodeId, publicKey, callback) => {
  const query = {
    id: database.hash(publicKey.id),
    ledgerNodeId
  };
  database.collections['continuity2017_key']
    .findOne(query, {}, (err, result) => {
      if(err) {
        return callback(err);
      }
      if(!result) {
        return callback(new BedrockError(
          'PublicKey not found.',
          'NotFoundError',
          {httpStatusCode: 404, key: publicKey, ledgerNodeId, public: true}
        ));
      }
      callback(null, result.publicKey);
    });
};

/**
 * Updates descriptive data for a PublicKey.
 *
 * @param ledgerNodeId the ID of the node that is tracking this publicKey.
 * @param publicKey the publicKey to update.
 * @param callback(err) called once the operation completes.
 */
api.updatePublicKey = (ledgerNodeId, publicKey, callback) => {
  async.auto({
    // exclude restricted fields
    update: callback => database.collections['continuity2017_key'].update(
      {id: database.hash(publicKey.id), ledgerNodeId},
      {$set: database.buildUpdate(
        publicKey, 'publicKey', {exclude: [
          'publicKey.publicKeyPem', 'publicKey.owner']})},
      database.writeOptions,
      callback),
    checkUpdate: ['update', (results, callback) => {
      if(results.update.result.n === 0) {
        return callback(new BedrockError(
          'Could not update public key. Public key not found.',
          'NotFoundError',
          {httpStatusCode: 404, key: publicKey, public: true}
        ));
      }
      callback();
    }]
  }, callback);
};
