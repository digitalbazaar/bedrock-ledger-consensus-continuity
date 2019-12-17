/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache/cache-key');
const _events = require('./events');
const _voters = require('./voters');
const cache = require('bedrock-redis');
const {util: {BedrockError}} = require('bedrock');

/**
 * Get non-consensus ledger history.
 *
 * @param {Object} options - The options to use.
 * @param {Array<string>} options.creatorIds - The creator IDs of interest.
 * @param {Object} options.ledgerNode - The node that is tracking the history.
 * @param {Number} options.maxDepth - The maximum number of generations
 *   to include in the history for each creator.
 *
 * @returns {Promise} that resolves once the operation completes.
 */
// FIXME: defaulting maxDepth until callers are updated
exports.getRecentHistory = async ({creatorIds, ledgerNode, maxDepth = 100}) => {

  // get ids for all the creators known to the node
  // FIXME: this is problematic... we can't possibly return ALL creators
  // ever known to the node when the network (and its history) gets quite
  // large.
  // localCreators is an array of strings which are peerIds (URLs)
  const localCreators = await _voters.getPeerIds({ledgerNode});

  // FIXME defaulting this until the caller passes it in
  creatorIds = creatorIds || localCreators;

  const consensusHeads = await _getConsensusHeads({ledgerNode, localCreators});

  const {aggregateHistory, getHead} = ledgerNode.storage.events
    .plugins['continuity-storage'];

  const creatorHeads = {};
  await Promise.all(localCreators.map(async creatorId => {
    creatorHeads[creatorId] = await _events.getHead({creatorId, ledgerNode});
  }));

  const {creatorRestriction, hasMore, startParentHash} =
    await _computeAggregateParams({
      consensusHeads, creatorHeads, creatorIds, getHead, maxDepth
    });

  const events = [];
  const eventMap = {};
  if(startParentHash.length === 0) {
    return {events, eventMap};
  }
  const eventHistory = await aggregateHistory({
    creatorRestriction,
    eventTypeFilter: 'ContinuityMergeEvent',
    startParentHash
  });
  if(eventHistory.length === 0) {
    return {events, eventMap};
  }
  const ledgerNodeId = ledgerNode.id;
  const outstandingMergeEventKeys = eventHistory.map(({meta: {eventHash}}) =>
    _cacheKey.outstandingMergeEvent({eventHash, ledgerNodeId}));
  const result = await cache.client.mget(outstandingMergeEventKeys);
  // sanity check to ensure all merge events could be retrieved... if
  // `null` is in the array then at least one is missing
  if(result.includes(null)) {
    // FIXME: this is a signal that the cache recovery would need to be run.
    throw new BedrockError(
      'One or more events are missing from the cache.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }

  for(const eventSummaryJson of result) {
    const parsed = JSON.parse(eventSummaryJson);
    const {eventHash} = parsed.meta;
    const {parentHash, treeHash, type} = parsed.event;
    const {creator} = parsed.meta.continuity2017;
    const doc = {
      _children: [],
      _parents: [],
      eventHash,
      event: {parentHash, treeHash, type},
      meta: {continuity2017: {creator}}
    };
    events.push(doc);
    eventMap[eventHash] = doc;
  }

  return {events, eventMap, hasMore};
};

async function _computeAggregateParams({
  consensusHeads, creatorHeads, creatorIds, getHead, maxDepth
}) {
  let hasMore = false;
  const startParentHash = [];
  // creator restriction defines the base of the branches which are the
  // most recent consensus merge events for each branch
  const creatorRestriction = [];
  for(const creatorId of creatorIds) {
    const consensusHead = consensusHeads.get(creatorId);
    // there may not be a consensusHead for the creator if it's a new node
    // or when the ledger is starting up etc.
    const consensusGeneration = consensusHead ? consensusHead.generation : 0;
    creatorRestriction.push(
      {creator: creatorId, generation: consensusGeneration});

    const maxGeneration = consensusGeneration + maxDepth;
    const {
      generation: currentGeneration,
      eventHash: currentGenerationEventHash
    } = creatorHeads[creatorId];
    if(currentGeneration <= maxGeneration) {
      // the current head is within range, use it
      startParentHash.push(currentGenerationEventHash);
    } else {
      hasMore = true;
      const [{meta: {eventHash}}] = await getHead(
        {creatorId, generation: maxGeneration});
      startParentHash.push(eventHash);
    }
  }
  return {creatorRestriction, hasMore, startParentHash};
}

async function _getConsensusHeads({ledgerNode, localCreators}) {
  // get the latest merge event for peer that has consensus
  const result = new Map();
  const query = {
    'meta.continuity2017.type': 'm',
    'meta.continuity2017.creator': {$in: localCreators},
    'meta.consensus': true,
  };
  (await ledgerNode.storage.events.collection.aggregate([
    {$match: query},
    {$sort: {'meta.continuity2017.generation': -1}},
    {$group: {
      _id: '$meta.continuity2017.creator',
      generation: {$first: '$meta.continuity2017.generation'},
    }},
  ]).toArray()).forEach(r => result.set(r._id, {generation: r.generation}));

  return result;
}
