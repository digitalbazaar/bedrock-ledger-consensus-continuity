/*
 * Manifest storage for Continuity2017 consensus method.
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
    database.openCollections(['continuity2017_manifest'], callback),
  createIndexes: ['openCollections', (results, callback) =>
    database.createIndexes([{
      collection: 'continuity2017_manifest',
      fields: {'manifest.id': 1},
      options: {unique: true, background: false}
    }], callback)
  ]
}, err => callback(err)));

/**
 * Adds a new manifest. A manifest is a proposed set of events for a
 * future block.
 *
 * Manifest storage may be safely shared across ledger nodes.
 *
 * @param manifest the manifest to store:
 *          id the ID (hash) of the manifest.
 *          block the ID of the block the manifest is for.
 *          events an array of all of the event hashes in the manifest.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (manifest, callback) => {
  // create the record
  const now = Date.now();
  const record = {
    meta: {
      created: now,
      updated: now
    },
    manifest: manifest
  };

  logger.verbose(`adding manifest ${manifest.id}`);

  const collection = database.collections.continuity2017_manifest;
  collection.insert(record, database.writeOptions, (err, result) => {
    if(err) {
      if(database.isDuplicateError(err)) {
        return callback(new BedrockError(
          'The manifest already exists.', 'DuplicateError', {
            manifest: manifest.id
          }, err));
      }
      return callback(err);
    }
    callback(null, result.ops[0]);
  });
};

/**
 * Gets a manifest by the given manifest ID (hash).
 *
 * @param manifestHash the ID of the manifest.
 * @param callback(err, manifest) called when the operation completes.
 */
api.get = (manifestHash, callback) => {
  const query = {
    'manifest.id': manifestHash,
    'meta.deleted': {
      $exists: false
    }
  };
  const collection = database.collections.continuity2017_manifest;
  collection.findOne(query, {manifest: 1}, (err, record) => {
    if(err) {
      return callback(err);
    }

    if(!record) {
      return callback(new BedrockError(
        'Manifest not found.', 'NotFound', {
          manifest: manifestHash
        }));
    }

    callback(null, record.manifest);
  });
};
