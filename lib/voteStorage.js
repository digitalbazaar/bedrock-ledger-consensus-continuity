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
      fields: {ledgerNodeId: 1, electionTopic: 1, 'meta.voteHash': 1},
      options: {unique: true, background: false}
    }, {
      collection: 'continuity2017_vote',
      fields: {
        ledgerNodeId: 1,
        electionTopic: 1,
        'vote.blockHeight': 1,
        'vote.voteRound': 1,
        'vote.manifestHash': 1,
        'vote.voter': 1
      },
      options: {
        unique: true,
        background: false,
        name: 'bedrock_ledger_continuity_test.continuity2017_vote.aggregation'
      }
    }, {
      collection: 'continuity2017_vote',
      fields: {
        ledgerNodeId: 1,
        electionTopic: 1,
        'vote.blockHeight': 1,
        'vote.voter': 1,
        'vote.voteRound': 1,
        'vote.manifestHash': 1
      },
      options: {
        unique: true,
        background: false,
        name: 'bedrock_ledger_continuity_test.continuity2017_vote.by_voter'
      }
    }, {
      collection: 'continuity2017_vote',
      fields: {
        ledgerNodeId: 1,
        'meta.voteHash': 1,
        'meta.deleted': 1,
      },
      options: {
        unique: true,
        background: false,
        name: 'bedrock_ledger_continuity_test.continuity2017_vote.by_voteHash'
      }
    }], callback)
  ]
}, err => callback(err)));

/**
 * Adds a new vote. A vote contains the height of the block the vote pertains
 * to, the type of election, the manifest voted for, who voted for it, and
 * during which round.
 *
 * @param ledgerNodeId the ID of the node that is tracking this vote, which
 *         is *not* necessarily the same as the node that cast the vote.
 * @param vote the vote information:
 *          blockHeight the height of the block the vote is for.
 *          electionTopic the topic of the election ('Events' or 'RollCall').
 *          manifest the hash of the manifest that was voted for.
 *          round the round during which the vote occurred.
 *          voter the voter information.
 *          signature the signature on the vote.
 *          [recommendedElectors] a list of recommended electors for future
 *            elections; each elector should be a voter with `id` and
 *            optionally a `sameAs` with another voter `id`.
 * @param options the options for the vote.
 *          voteHash the hash for the vote.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (ledgerNodeId, electionTopic, vote, options, callback) => {
  if(!(options && typeof options === 'object' && options.meta &&
    typeof options.meta === 'object' && options.meta.voteHash &&
    typeof options.meta.voteHash === 'string')) {
    throw new TypeError('`options.meta.voteHash` must be a non-empty string.');
  }

  // create the record
  const now = Date.now();
  const record = {
    ledgerNodeId: database.hash(ledgerNodeId),
    electionTopic: electionTopic,
    meta: Object.assign({
      created: now,
      updated: now
    }, options.meta),
    vote: vote
  };

  logger.verbose(`adding vote for ${electionTopic}`, vote);

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
 * Determine if a vote with a given hash exists.
 *
 * @param ledgerNodeId the ID of the node that is tracking this vote
 * @param voteHash the hash or array of hashes of the vote(s)
 * @param callback(err, result) called once the operation completes.
 */
api.exists = (ledgerNodeId, voteHash, callback) => {
  const hashes = [].concat(voteHash);
  const query = {
    ledgerNodeId: database.hash(ledgerNodeId),
    'meta.deleted': {$exists: false},
    'meta.voteHash': {$in: hashes},
  };
  const collection = database.collections.continuity2017_vote;
  collection.find(query).count((err, result) => {
    if(err) {
      return callback(err);
    }
    if(hashes.length !== result) {
      return callback(null, false);
    }
    return callback(null, true);
  });
};

// TODO: add documentation
api.difference = (ledgerNodeId, voteHash, callback) => {
  const hashes = [].concat(voteHash);
  const query = {
    ledgerNodeId: database.hash(ledgerNodeId),
    'meta.deleted': {$exists: false},
    'meta.voteHash': {$in: hashes},
  };
  const collection = database.collections.continuity2017_vote;
  collection.find(query, {'meta.voteHash': 1}).toArray((err, result) => {
    if(err) {
      return callback(err);
    }
    const localVotes = result.map(v => v.meta.voteHash);
    callback(null, hashes.filter(v => !localVotes.includes(v)));
  });
};

/**
 * Gets the current election results for a particular block from the
 * perspective of a particular node.
 *
 * @param ledgerNodeId the ID of the node that is tracking the votes.
 * @param blockHeight the height of the block.
 * @param electionTopic the topic of the election ('Event' or 'RollCall').
 * @param callback(err, votes) called when the operation completes.
 */
api.get = (ledgerNodeId, blockHeight, electionTopic, callback) => {
  const query = {
    ledgerNodeId: database.hash(ledgerNodeId),
    electionTopic: electionTopic,
    'vote.blockHeight': blockHeight,
    'meta.deleted': {$exists: false}
  };
  const collection = database.collections.continuity2017_vote;
  collection.find(query, {vote: 1, meta: 1}).toArray((err, records) => {
    if(err) {
      return callback(err);
    }
    callback(null, records);
  });
};

// TODO: add documentation
api.getByHash = (ledgerNodeId, voteHash, callback) => {
  const query = {
    ledgerNodeId: database.hash(ledgerNodeId),
    'meta.voteHash': voteHash,
    'meta.deleted': {$exists: false}
  };
  const collection = database.collections.continuity2017_vote;
  collection.findOne(query, {vote: 1, meta: 1}, (err, result) =>
    err ? callback(err) : callback(null, result));
};

/**
 * Get the last vote cast for a particular block by a particular voter. If
 * there is no such vote, `null` will be passed to the callback.
 *
 * @param ledgerNodeId the ID of the node that is tracking the votes.
 * @param blockHeight the height of the block.
 * @param electionTopic the topic of the election ('Event' or 'RollCall').
 * @param voterId the ID of the voter.
 * @param callback(err, vote) called when the operation completes.
 */
api.getLast = (ledgerNodeId, blockHeight, electionTopic, voterId, callback) => {
  const query = {
    ledgerNodeId: database.hash(ledgerNodeId),
    electionTopic: electionTopic,
    'vote.blockHeight': blockHeight,
    'vote.voter': voterId,
    'meta.deleted': {$exists: false}
  };
  const collection = database.collections.continuity2017_vote;
  collection.find(query, {})
    .sort({'vote.voteRound': -1})
    .limit(1)
    .toArray((err, records) => {
      if(err) {
        return callback(err);
      }
      if(records.length === 0) {
        return callback(null, null);
      }
      callback(null, records[0].vote);
    });
};

/**
 * Gets the vote count for all manifests for a round for a particular block
 * from the perspective of a particular node.
 *
 * The result will be sorted by vote count and have this structure:
 *
 * [{manifestHash: <manifestHash>, count: <number of votes>}, ...]
 *
 * @param ledgerNodeId the ID of the node that is tracking the votes.
 * @param blockHeight the height of the block.
 * @param electionTopic the topic of the election ('Event' or 'RollCall').
 * @param round the round.
 * @param callback(err, results) called when the operation completes.
 */
api.tally = (ledgerNodeId, blockHeight, electionTopic, round, callback) => {
  // TODO: can use `$sortByCount` instead in mongo 3.4+

  // create aggregation pipeline
  const pipeline = [
    // only match votes that have `blockHeight` and `round`
    {$match: {
      ledgerNodeId: database.hash(ledgerNodeId),
      electionTopic: electionTopic,
      'vote.blockHeight': blockHeight,
      'vote.voteRound': round,
      'meta.deleted': {$exists: false}
    }},
    // group by manifest and produce a count for each manifest
    {$group: {_id: '$vote.manifestHash', count: {$sum: 1}}},
    // sort by count (descending, most voted manifest first)
    {$sort: {count: -1}}
  ];
  const collection = database.collections.continuity2017_vote;
  collection.aggregate(pipeline).toArray((err, items) => {
    if(err) {
      return callback(err);
    }
    callback(null, items.map(item => ({
      manifestHash: item._id,
      count: item.count
    })));
  });
};
