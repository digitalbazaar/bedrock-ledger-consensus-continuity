/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('./logger');
const uuid = require('uuid/v4');
const {BedrockError} = bedrock.util;

const eventsConfig = config['ledger-consensus-continuity'].events;
const operationsConfig = config['ledger-consensus-continuity'].operations;

const api = {};
module.exports = api;

// expose cache key API for testing
api.cacheKey = _cacheKey;

api.Timer = require('./Timer');
api.OperationQueue = require('./OperationQueue');

api.blocks = {};

/**
 * Sets the current block height in the cache for a ledger node.
 *
 * @param blockHeight the block height to set.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.blocks.setBlockHeight = async ({blockHeight, ledgerNodeId}) => {
  const key = _cacheKey.blockHeight(ledgerNodeId);
  await cache.client.set(key, blockHeight);
};

/**
 * Commits a block by removing outstanding merge events from the event cache
 * and updating the block height after a successful merge.
 *
 * @param eventHashes the hashes of the merge events from the block to commit.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.blocks.commitBlock = async ({eventHashes, ledgerNodeId}) => {
  const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const eventKeys = eventHashes.map(
    eventHash => _cacheKey.event({eventHash, ledgerNodeId}));
  return cache.client.multi()
    .srem(outstandingMergeKey, eventKeys)
    .del(eventKeys)
    .incr(blockHeightKey)
    .exec();
};

api.events = {};

/**
 * Adds an event received from a peer to the cache for later inserting into
 * persistent storage.
 *
 * @param event the event to cache.
 * @param meta the meta data for the event.
 * @param ledgerNodeId the ID of the ledger node to cache it for.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.events.addPeerEvent = async ({event, meta, ledgerNodeId}) => {
  const {eventHash} = meta;
  const {creator: creatorId, generation, type} = meta.continuity2017;
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  const eventJson = JSON.stringify({event, eventHash, meta});

  // TODO: it would be great to find some common abstractions between
  // adding peer, merge, and local events to help with maintanence and
  // correctness... see `addLocalMergeEvent` and `addLocalRegularEvent`

  // perform update in a single atomic transaction
  const txn = cache.client.multi();
  if(type === 'r') {
    // Note: peer regular events have `operationRecords` not `operation` at
    // this point
    const opCountKey = _cacheKey.opCountPeer(
      {ledgerNodeId, second: Math.round(Date.now() / 1000)});
    txn.incrby(opCountKey, event.operationRecords.length);
    txn.expire(opCountKey, operationsConfig.counter.ttl);
  }
  if(type === 'm') {
    const headGenerationKey = _cacheKey.headGeneration(
      {eventHash, ledgerNodeId});
    const latestPeerHeadKey = _cacheKey.latestPeerHead(
      {creatorId, ledgerNodeId});
    // expire the key in an hour, in case the peer/creator goes dark
    txn.hmset(latestPeerHeadKey, 'h', eventHash, 'g', generation);
    txn.expire(latestPeerHeadKey, 3600);
    // this key is set to expire in the event-writer
    txn.set(headGenerationKey, generation);
  }
  // add the hash to the set used to check for dups and ancestors
  txn.sadd(eventQueueSetKey, eventHash);
  // create a key that contains the event and meta
  txn.set(eventKey, eventJson);
  // push to the list that is handled in the event-writer
  txn.rpush(eventQueueKey, eventKey);
  txn.publish(`continuity2017|peerEvent|${ledgerNodeId}`, 'new');
  return txn.exec();
  // TODO: abstract `publish` into some notify/check API on this file to keep
  // it isolated and easier to maintain
};

/**
 * Adds the summary information for a local merge event to the cache for later
 * processing by the consensus algorithm. Local merge events are already
 * present in storage before this method is called; this merely adds a summary
 * of their information to the cache so it can be pulled down with the rest
 * of "recent history" to be processed by the consensus algorithm.
 *
 * @param event the event to cache.
 * @param meta the meta data for the event.
 * @param ledgerNodeId the ID of the ledger node to cache it for.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.events.addLocalMergeEvent = async ({event, meta, ledgerNodeId}) => {
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const {creator: creatorId, generation} = meta.continuity2017;
  const {eventHash} = meta;
  const headKey = _cacheKey.head({creatorId, ledgerNodeId});
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const {parentHash, treeHash, type} = event;
  const parentHashes = parentHash.filter(h => h !== treeHash);
  // no need to set a headGeneration key here, those are only used for
  // processing peer merge events
  // TODO: `creator` is quite a long URL, can a substitution be made?

  // for local merge events we only cache a summary of the event; it does not
  // include its operations and it puts `eventHash` at the top level to
  // assist the consensus algorithm (it is not put in `meta` like it is
  // for persistent ledger storage (e.g. mongo)
  const eventSummary = JSON.stringify({
    eventHash,
    event: {parentHash, treeHash, type},
    meta: {continuity2017: {creator: creatorId}}
  });
  try {
    const result = cache.client.multi()
      .srem(childlessKey, parentHashes)
      .set(eventKey, eventSummary)
      .sadd(outstandingMergeKey, eventKey)
      .hmset(headKey, 'h', eventHash, 'g', generation)
      .publish(`continuity2017|event|${ledgerNodeId}`, 'merge')
      .exec();
    // result is inspected in unit tests
    return result;
  } catch(e) {
    // FIXME: fail gracefully
    // failure here means head information would be corrupt which
    // cannot be allowed
    logger.error('Could not set head.', {
      creatorId,
      // FIXME: fix when logger.error works properly
      err1: e,
      generation,
      headKey,
      ledgerNodeId,
    });
    throw e;
  }
};

/**
 * Tracks that a new local regular event has been added that needs merging.
 *
 * @param eventHash the hash of the new local regular event.
 * @param ledgerNodeId the ID of the ledger node the event was added to.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.events.addLocalRegularEvent = async ({eventHash, ledgerNodeId}) => {
  // new local events are `childless` meaning that they have no events
  // that descend from them; they must be merged
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const localRegularEventCountKey = _cacheKey.eventCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  return cache.client.multi()
    .sadd(childlessKey, eventHash)
    .incr(localRegularEventCountKey)
    .expire(localRegularEventCountKey, eventsConfig.counter.ttl)
    // inform ConsensusAgent and other listeners about new regular events
    // so that they can be merged
    .publish(`continuity2017|event|${ledgerNodeId}`, 'regular')
    .exec();
};

/**
 * Get the current merge status information for the ledger node identified
 * by the given ID. This status information includes whether or not the
 * ledger node is lagging/behind in gossip (indicating that it should not
 * merge any events until caught up) and the hashes of any childless
 * events (targets for merging ... to become the parents of the next
 * potential merge event).
 *
 * @param ledgerNodeId the ID of the ledger node to get the merge status for.
 *
 * @return a Promise that resolves to the merge status info.
 */
api.events.getMergeStatus = async ({ledgerNodeId}) => {
  // see if there are any childless events to be merged and if the node
  // is too far behind in gossip to merge
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const gossipBehindKey = _cacheKey.gossipBehind(ledgerNodeId);
  const [gossipBehind, childlessHashes] = await cache.client.multi()
    .get(gossipBehindKey)
    .smembers(childlessKey)
    .exec();
  // TODO: `gossipBehind` seems like it could have a more clear name
  return {gossipBehind: gossipBehind !== null, childlessHashes};
};

/**
 * Sets the current head for a creator ID in the cache. This head is stable,
 * meaning it refers to an event that is in storage.
 *
 * @param creatorId the ID of the creator to set the head for.
 * @param ledgerNodeId the ID of the ledger node to use.
 * @param eventHash the event hash for the head.
 * @param generation the generation for the head.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.events.setHead = async (
  {creatorId, ledgerNodeId, eventHash, generation}) => {
  const key = _cacheKey.head({creatorId, ledgerNodeId});
  await _setHead({key, eventHash, generation});
};

/**
 * Gets the current head from the cache. This head is stable, meaning it refers
 * to an event that is in storage.
 *
 * @param creatorId the ID of the creator to get the head for.
 * @param ledgerNodeId the ID of the ledger node to check.
 *
 * @return a Promise that resolves to the head or `null` if none found.
 */
api.events.getHead = async ({creatorId, ledgerNodeId}) => {
  const key = _cacheKey.head({creatorId, ledgerNodeId});
  return _getHead({key});
};

/**
 * Sets the genesis head from the cache.
 *
 * @param eventHash the event hash for the genesis merge event.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.events.setGenesisHead = async ({eventHash, ledgerNodeId}) => {
  const key = _cacheKey.genesis(ledgerNodeId);
  await cache.client.set(key, eventHash);
};

/**
 * Gets the genesis head from the cache.
 *
 * @param ledgerNodeId the ID of the ledger node to check.
 *
 * @return a Promise that resolves to the head or `null` if none found.
 */
api.events.getGenesisHead = async ({ledgerNodeId}) => {
  const key = _cacheKey.genesis(ledgerNodeId);
  const eventHash = await cache.client.get(key);
  if(eventHash) {
    return {eventHash, generation: 0};
  }
  return null;
};

/**
 * Gets the *very* latest head in the event cache that may not have been
 * written to storage yet, i.e. it can be *more recent* than just getting
 * the head for a node. This head is useful during gossip, but not for merging
 * events because it is not stable enough.
 *
 * @param creatorId the ID of the creator to get the latest head for.
 * @param ledgerNodeId the ID of the ledger node to check.
 *
 * @return a Promise that resolves to the latest head or `null` if none found.
 */
api.events.getUncommittedHead = async ({creatorId, ledgerNodeId}) => {
  const key = _cacheKey.latestPeerHead({creatorId, ledgerNodeId});
  return _getHead({key});
};

/**
 * Gets the generation for a single event identified by the given event hash
 * and stored by the given ledger node. The generation indicates the order of
 * an event relative to its creator node. A generation of `0` is the genesis
 * event and events count up from there, scoped to each node.
 *
 * @param eventHash the hash of the event to get the generation for.
 * @param ledgerNodeId the ID of the ledger node to use.
 *
 * @return a Promise that resolves to the generation for the event.
 */
api.events.getGeneration = async ({eventHash, ledgerNodeId}) => {
  // first check cache
  const key = _cacheKey.headGeneration({eventHash, ledgerNodeId});
  const generation = await cache.client.get(key);
  if(generation !== null) {
    return parseInt(generation, 10);
  }
  // no generation found for the eventHash
  return generation;
};

/**
 * Bulk sets the generation for every event in the given Map.
 *
 * @param generationMap a Map of eventHash => generation to set.
 * @param ledgerNodeId the ID of the ledger node to use.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.events.setGenerations = async ({generationMap, ledgerNodeId}) => {
  if(generationMap.size === 0) {
    // nothing to do
    return;
  }
  // use a single atomic transaction to set all generations
  const txn = cache.client.multi();
  for(const [eventHash, generation] of generationMap) {
    const key = _cacheKey.headGeneration({eventHash, ledgerNodeId});
    txn.set(key, generation, 'EX', 36000);
  }
  return txn.exec();
};

/**
 * Bulk gets the generations for all events identified by the given hashes.
 *
 * @param eventHashes the hashes of the events to get the generations for.
 * @param ledgerNodeId the ID of the ledger node to use.
 *
 * @return a Promise that resolves to an object with:
 *         generationMap: a Map of eventHash => generation where the entries
 *           will preserve the order of `eventHashes`; any missing generation
 *           will be `null`.
 *         notFound: an array of eventHashes that were not found.
 */
api.events.getGenerations = async ({eventHashes, ledgerNodeId}) => {
  // create keys in order (ensures event hash order is preserved)
  const keys = eventHashes.map(eventHash =>
    _cacheKey.headGeneration({eventHash, ledgerNodeId}));
  const generations = await cache.client.mget(keys);

  // insert entries into generation map in order
  const generationMap = new Map();
  const notFound = [];
  let index = 0;
  for(const eventHash in eventHashes) {
    const generation = generations[index++];
    if(generation === null) {
      notFound.push(eventHash);
      generationMap.set(eventHash, null);
    } else {
      generationMap.set(eventHash, parseInt(generation, 10));
    }
  }
  return {generationMap, notFound};
};

/**
 * Compute the difference between the given event hashes and those that are
 * in the event cache, i.e. return which event hashes are NOT in the cache.
 *
 * @param eventHashes the hashes of events to look for.
 * @param ledgerNodeId the ID of the ledger node to check.
 *
 * @return a Promise that resolves to an array with the events that are
 *         not in the cache.
 */
api.events.difference = async ({eventHashes, ledgerNodeId}) => {
  if(eventHashes.length === 0) {
    return [];
  }
  // get a random key to use for the diff operation
  const diffKey = `d|${uuid()}`;
  // get key for event queue
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);

  // TODO: this could be implemented as smembers as well and diff the hashes
  // as an array, if the eventQueueSetKey contains a large set, then the
  // existing implementation is good

  // Note: Here we use an atomic transaction that adds an entry to redis
  //   just to perform a diff and then removes it.
  // 1. Add `diffKey` with `eventHashes` array as the value.
  // 2. Run `sdiff` to diff that value with what is in the event queue
  //    for the ledger node (using key `eventQueueSetKey`).
  // 3. Delete the `diffKey` once we're done running the diff.
  //
  // the results of `sadd` is in result[0], `sdiff` is in result[1], so
  // we destructure result[1] into `notFound` (i.e. events not in the queue)
  const [, notFound] = await cache.client.multi()
    .sadd(diffKey, eventHashes)
    .sdiff(diffKey, eventQueueSetKey)
    .del(diffKey)
    .exec();
  return notFound;
};

/**
 * Track duplicate events for statistics analysis.
 *
 * @param count the number of additional duplicate events detected.
 */
api.events.trackDuplicates = async ({count}) => {
  if(count === 0) {
    // nothing to do
    return;
  }
  // for statistics purposes only, ignore errors
  cache.client.incrby(`dup-${Math.round(Date.now() / 60000)}`, count)
    .catch(() => {});
};

/**
 * Gets the reset history of merge events from the cache. This history includes
 * all merge events in the cache that have not achieved consensus and a map of
 * each event's hash to each event. The event data model is also modified
 * with custom fields to assist in the consensus algorithm. Each event
 * looks like: `{eventHash, event, meta, _children, _parents}`. The
 * `_children` and `_parents` arrays will be populated by the consensus
 * algorithm. The `event` value only has summary info like tree and parent
 * hashes and does not include operations.
 *
 * @param ledgerNodeId the ID of the ledger node to check.
 *
 * @return a Promise that resolves to an object with:
 *           events - an array of events with a data model customized for
 *             the consensus algorithm.
 *           eventMap - a map of event hash => event (with custom data model).
 */
api.events.getRecentHistory = async ({ledgerNodeId}) => {
  const events = [];
  const eventMap = {};

  // get all *merge* events available
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const keys = await cache.client.smembers(outstandingMergeKey);
  if(!(keys && keys.length > 0)) {
    // no recent history to merge detected
    return {events, eventMap};
  }

  // result contains an array of JSON for event summary data (tree and parent
  // hash info, no operations, etc.)
  const result = await cache.client.mget(keys);
  // sanity check to ensure all merge events could be retrieved... if
  // `null` is in the array then at least one is missing
  if(result.includes(null)) {
    // FIXME: is this recoverable? how? document if so... does `repairCache`
    // handle it?
    throw new BedrockError(
      'One or more events are missing from the cache.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }
  for(const eventSummaryJson of result) {
    const parsed = JSON.parse(eventSummaryJson);
    const {eventHash} = parsed;
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
    eventMap[doc.eventHash] = doc;
  }

  return {events, eventMap};
};

api.gossip = {};

/**
 * Called when a node receives a notification from another peer.
 *
 * @param recipientId the ID of the recipient of the notification.
 * @param senderId the ID of the sender of the notification.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.gossip.addNotification = async ({receiverId, senderId}) => {
  const key = _cacheKey.gossipNotification(receiverId);
  // a set is used here because it only allows unique values
  return cache.client.sadd(key, senderId);
};

api.operations = {};

/**
 * Adds an operation to the cache.
 *
 * @param operation the operation to cache.
 * @param operationHash the hash for the operation.
 * @param recordId the `database.hash` of the ID of the record for the
 *          operation.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.operations.add = async (
  {operation, operationHash, recordId, ledgerNodeId}) => {
  // using uuid instead of hash because duplicate operations are allowed
  const opKey = _cacheKey.operation({ledgerNodeId, opId: uuid()});
  const opListKey = _cacheKey.operationList(ledgerNodeId);
  const opCountKey = _cacheKey.opCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  return cache.client.multi()
    .incr(opCountKey)
    .expire(opCountKey, operationsConfig.counter.ttl)
    .set(opKey, JSON.stringify({operation, meta: {operationHash}, recordId}))
    .rpush(opListKey, opKey)
    .publish(`continuity2017|operation|${ledgerNodeId}`, 'add')
    .exec();
};

async function _setHead({key, eventHash, generation}) {
  return cache.client.hmset(key, 'h', eventHash, 'g', generation);
}

async function _getHead({key}) {
  const [eventHash, generation] = await cache.client.hmget(key, 'h', 'g');
  // redis returns null if key is not found
  if(eventHash === null) {
    return null;
  }
  return {eventHash, generation: parseInt(generation, 10)};
}
