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
      fields: {'ledgerNodeId': 1, 'manifest.id': 1},
      options: {unique: true, background: false}
    }, {
      collection: 'continuity2017_manifest',
      fields: {
        'ledgerNodeId': 1,
        'manifest.type': 1,
        'manifest.blockHeight': 1,
        'itemCount': 1
      },
      options: {unique: true, background: false}
    }], callback)
  ]
}, err => callback(err)));

/**
 * Adds a new manifest. A manifest is a proposed set of items, for example,
 * a set of events for a block or a set of electors that voted for a particular
 * set of events.
 *
 * Manifest storage may be safely shared across ledger nodes.
 *
 * @param ledgerNodeId the ID of the node that is tracking this manifest.
 * @param manifest the manifest to store:
 *          id the ID (hash) of the manifest.
 *          blockHeight the height of the block the manifest is for.
 *          type 'Events' or 'RollCall'.
 *          item an array of all of the item hashes in the manifest.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (ledgerNodeId, manifest, callback) => {
  // create the record
  const now = Date.now();
  const record = {
    ledgerNodeId: database.hash(ledgerNodeId),
    itemCount: manifest.item.length,
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
 * @param ledgerNodeId the ID of the node that is tracking this manifest.
 * @param manifestHash the ID of the manifest.
 * @param callback(err, manifest) called when the operation completes.
 */
api.get = (ledgerNodeId, manifestHash, callback) => {
  const query = {
    ledgerNodeId: database.hash(ledgerNodeId),
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
        'Manifest not found.', 'NotFoundError', {
          manifest: manifestHash,
          httpStatusCode: 404,
          public: true
        }));
    }

    callback(null, record.manifest);
  });
};

/**
 * Gets all manifests for the given block height, sorted by their length.
 *
 * The result will have this structure:
 *
 * [{manifestHash: <manifestHash>, length: <number of items>}, ...]
 *
 * @param ledgerNodeId the ID of the node that is tracking the manifests.
 * @param blockHeight the height of the block.
 * @param electionTopic the topic of the election ('Event' or 'RollCall').
 * @param callback(err, results) called when the operation completes.
 */
api.getAllByLength = (ledgerNodeId, blockHeight, electionTopic, callback) => {
  const query = {
    ledgerNodeId: database.hash(ledgerNodeId),
    'manifest.type': electionTopic,
    'manifest.blockHeight': blockHeight,
    'meta.deleted': {$exists: false}
  };
  const collection = database.collections.continuity2017_manifest;
  collection.find(query, {'manifest.id': 1, itemCount: 1})
    .sort({'itemCount': -1})
    .toArray((err, records) => {
    if(err) {
      return callback(err);
    }
    callback(null, records.map(r => ({
      manifestHash: r.manifest.id,
      length: r.itemCount
    })));
  });
};

/**
 * Get a count for all items in a set of manifests.
 *
 * The result has this structure:
 *
 * [{id: <item>, count: count}]
 *
 * The items will be sorted by count, in descending order (highest count first).
 *
 * @param ledgerNodeId the ID of the node that is tracking the manifests.
 * @param manifestHashes the set of manifest hashes.
 * @param callback(err, results) called once the operation completes.
 */
api.getItemCount = (ledgerNodeId, manifestHashes, callback) => {
  // TODO: can use `$sortByCount` instead in mongo 3.4+

  // create aggregation pipeline
  const pipeline = [
    // only match given manifests
    {$match: {
      ledgerNodeId: database.hash(ledgerNodeId),
      'manifest.id': {$in: manifestHashes},
      'meta.deleted': {$exists: false}
    }},
    // group by manifest and produce a count for each manifest
    {$unwind: '$manifest.item'},
    {$group: {_id: '$manifest.item', count: {$sum: 1}}},
    // sort by count (descending, items with highest counts first)
    {$sort: {count: -1}}
  ];
  const collection = database.collections.continuity2017_manifest;
  collection.aggregate(pipeline).toArray((err, items) => {
    if(err) {
      return callback(err);
    }
    callback(null, items.map(item => ({
      id: item._id,
      count: item.count
    })));
  });
};
