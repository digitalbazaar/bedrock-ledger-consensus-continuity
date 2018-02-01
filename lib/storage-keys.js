/*
 * Key storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
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
    publicKey.type = 'Ed25519VerificationKey2018';
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

api.getPeerIds = ({creatorId, ledgerNodeId}, callback) => {
  // exclude creatorId
  const query = {ledgerNodeId, 'publicKey.owner': {$ne: creatorId}};
  const projection = {_id: 0, 'publicKey.owner': 1};
  database.collections['continuity2017_key'].find(query, projection)
    .toArray((err, result) => {
      if(err) {
        return callback(err);
      }
      return callback(null, result.map(k => k.publicKey.owner));
    });
};

/**
 * Retrieves a PublicKey.
 *
 * @param callback(err, publicKey) called once the operation completes.
 */
api.getPublicKey = ({ledgerNodeId, publicKeyId}, callback) => {
  const publicKeyCacheKey = _cacheKey.publicKey({ledgerNodeId, publicKeyId});
  // TTL is extended by 8 hours on each access
  const cacheTtl = 28800; // 8 hours
  async.auto({
    cache: callback => cache.client.multi()
      .get(publicKeyCacheKey)
      .expire(publicKeyCacheKey, cacheTtl)
      .exec((err, result) => {
        if(err) {
          return callback(err);
        }
        if(result[0] === null) {
          return callback();
        }
        callback(null, JSON.parse(result[0]));
      }),
    publicKey: ['cache', (results, callback) => {
      if(results.cache) {
        // cache hit
        return callback(null, results.cache);
      }
      const query = {id: database.hash(publicKeyId), ledgerNodeId};
      const projection = {_id: 0, publicKey: 1};
      database.collections['continuity2017_key']
        .findOne(query, projection, (err, result) => {
          if(err) {
            return callback(err);
          }
          if(!result) {
            return callback(new BedrockError(
              'PublicKey not found.',
              'NotFoundError',
              {httpStatusCode: 404, ledgerNodeId, publicKeyId, public: true}
            ));
          }
          callback(null, result.publicKey);
        });
    }],
    cacheInsert: ['publicKey', (results, callback) => {
      if(results.cache) {
        // cache hit
        return callback();
      }
      cache.client.set(
        publicKeyCacheKey, JSON.stringify(results.publicKey), 'EX', cacheTtl,
        callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, results.publicKey);
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
  const publicKeyCacheKey = _cacheKey.publicKey(
    {ledgerNodeId, publicKeyId: publicKey.id});
  async.auto({
    // evict the key from the cache
    evict: callback => cache.client.del(publicKeyCacheKey, callback),
    // exclude restricted fields
    update: callback => database.collections['continuity2017_key'].update(
      {id: database.hash(publicKey.id), ledgerNodeId},
      {$set: database.buildUpdate(
        publicKey, 'publicKey', {exclude: [
          'publicKey.publicKeyBase58', 'publicKey.owner']})},
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
