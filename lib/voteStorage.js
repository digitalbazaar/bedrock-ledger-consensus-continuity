/*
 * Vote storage for Continuity2017 consensus method.
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
    database.openCollections(['continuity2017_vote'], callback),
  createIndexes: ['openCollections', (results, callback) =>
    database.createIndexes([{
      collection: 'continuity2017_vote',
      fields: {
        blockId: 1,
        'vote.round': 1,
        'vote.manifest': 1,
        'vote.voter': 1
      },
      options: {unique: true, background: false}
    }], callback)
  ]
}, err => callback(err)));

/**
 * Adds a new vote. A vote contains the ID of the block the vote pertains
 * to, the manifest voted for, who voted for it, and during which round.
 *
 * @param vote the vote information:
 *          block the ID of the block the vote is for.
 *          manifest the hash of the manifest that was voted for.
 *          round the round during which the vote occurred.
 *          voter the ID of the voter.
 *          signature the signature on the vote.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (vote, callback) => {
  // create the record
  const now = Date.now();
  const record = {
    blockId: database.hash(vote.block),
    meta: {
      created: now,
      updated: now
    },
    vote: vote
  };

  logger.verbose('adding vote', vote);

  const collection = database.collections.continuity2017_vote;
  collection.insert(record, database.writeOptions, (err, result) => {
    if(err) {
      if(database.isDuplicateError(err)) {
        return callback(new BedrockError(
          'The vote already exists.', 'DuplicateError', {vote: vote}, err));
      }
      return callback(err);
    }
    callback(null, result.ops[0]);
  });
};

/**
 * Gets the current election results for a particular block.
 *
 * @param blockId the ID of the block.
 * @param callback(err, votes) called when the operation completes.
 */
api.get = (blockId, callback) => {
  const query = {
    blockId: database.hash(blockId),
    'meta.deleted': {$exists: false}
  };
  const collection = database.collections.continuity2017_vote;
  collection.find(query).toArray((err, records) => {
    if(err) {
      return callback(err);
    }
    callback(null, records.filter(record => record.vote));
  });
};

/**
 * Gets the vote count for all manifests for a round for a particular block.
 *
 * @param blockId the ID of the block.
 * @param round the round.
 * @param callback(err, votes) called when the operation completes.
 */
api.tally = (blockId, round, callback) => {
  // TODO: can use `$sortByCount` instead in mongo 3.4+

  // create aggregation pipeline
  const pipeline = [
    // only match votes that have `blockId` and `round`
    {$match: {
      blockId: database.hash(blockId),
      round,
      'meta.deleted': {$exists: false}
    }},
    // group by manifest and produce a count for each manifest
    {$group: {_id: '$vote.manifest', count: {$sum: 1}}},
    // sort by count (descending, most voted manifest first)
    {$sort: {count: -1}}
  ];
  const collection = database.collections.continuity2017_vote;
  collection.aggregation(pipeline).toArray((err, items) => {
    if(err) {
      return callback(err);
    }
    callback(null, items.filter(item => ({
      manifest: item._id,
      count: item.count
    })));
  });
};
