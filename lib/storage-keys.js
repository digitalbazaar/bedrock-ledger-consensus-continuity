/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {callbackify, BedrockError} = bedrock.util;
const database = require('bedrock-mongodb');
const logger = require('./logger');
const {promisify} = require('util');

// module API
const api = {};
module.exports = api;

// FIXME: key storage is no longer required... most of this API will change
// or be removed

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await promisify(database.openCollections)(['continuity2017_key']);
  await promisify(database.createIndexes)([{
    collection: 'continuity2017_key',
    fields: {ledgerNodeId: 1, id: 1},
    options: {unique: true, background: false}
  }]);
});

/**
 * Adds a new PublicKey.
 *
 * @param ledgerNodeId the ID of the node that is tracking this publicKey.
 * @param publicKey the publicKey to add, with no ID yet set.
 * @param callback(err, record) called once the operation completes.
 */
api.addPublicKey = callbackify(async (ledgerNodeId, publicKey) => {
  if(!('id' in publicKey)) {
    throw new BedrockError(
      '`id` is a required property.', 'DataError', {
        ledgerNodeId,
        publicKey,
      });
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
  try {
    return (await database.collections['continuity2017_key'].insert(
      record, database.writeOptions)).ops[0];
  } catch(e) {
    if(database.isDuplicateError(e)) {
      throw new BedrockError(
        'Duplicate key.', 'DuplicateError', {publicKey: record});
    }
    throw e;
  }
});

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
api.getPublicKey = callbackify(async ({ledgerNodeId, publicKeyId}) => {
  // try to get key from cache
  const publicKeyCacheKey = _cacheKey.publicKey({ledgerNodeId, publicKeyId});
  // TTL is extended by 8 hours on each access
  const cacheTtl = 28800; // 8 hours
  const result = await cache.client.multi()
    .get(publicKeyCacheKey)
    .expire(publicKeyCacheKey, cacheTtl)
    .exec();
  if(result[0] !== null) {
    // cache hit
    return JSON.parse(result[0]);
  }

  // key not in cache, get from database
  const query = {id: database.hash(publicKeyId), ledgerNodeId};
  const projection = {_id: 0, publicKey: 1};
  const record = await database.collections['continuity2017_key']
    .findOne(query, projection);
  if(!record) {
    throw new BedrockError(
      'PublicKey not found.',
      'NotFoundError',
      {httpStatusCode: 404, ledgerNodeId, publicKeyId, public: true});
  }

  // insert key into cache
  const {publicKey} = record;
  await cache.client.set(
    publicKeyCacheKey, JSON.stringify(publicKey), 'EX', cacheTtl);

  return publicKey;
});

/**
 * Updates descriptive data for a PublicKey.
 *
 * @param ledgerNodeId the ID of the node that is tracking this publicKey.
 * @param publicKey the publicKey to update.
 * @param callback(err) called once the operation completes.
 */
// FIXME: remove method when simplifying public keys
api.updatePublicKey = callbackify(async (ledgerNodeId, publicKey) => {
  const publicKeyCacheKey = _cacheKey.publicKey(
    {ledgerNodeId, publicKeyId: publicKey.id});
  // exclude restricted fields
  const result = await database.collections['continuity2017_key'].update(
    {id: database.hash(publicKey.id), ledgerNodeId},
    {$set: database.buildUpdate(
      publicKey, 'publicKey', {exclude: [
        'publicKey.publicKeyBase58', 'publicKey.owner']})},
    database.writeOptions);
  if(result.result.n === 0) {
    throw new BedrockError(
      'Could not update public key. Public key not found.',
      'NotFoundError',
      {httpStatusCode: 404, key: publicKey, public: true});
  }

  // evict the key from the cache
  await cache.client.del(publicKeyCacheKey);
});
