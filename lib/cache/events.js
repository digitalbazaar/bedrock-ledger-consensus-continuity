/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');
const logger = require('../logger');
const {config, util: {BedrockError, uuid}} = bedrock;

const operationsConfig = config['ledger-consensus-continuity'].operations;
const eventsConfig = config['ledger-consensus-continuity'].events;

/**
 * Adds an event received from a peer to the cache for later inserting into
 * persistent storage.
 *
 * @param event {Object} - The event to cache.
 * @param meta {Object} - The meta data for the event.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.addPeerEvent = async ({event, meta, ledgerNodeId}) => {
  const {eventHash} = meta;
  const {creator: creatorId, generation, type} = meta.continuity2017;
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  const eventJson = JSON.stringify({event, meta});

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
 * @param event {Object} - The event to cache.
 * @param meta {Object} - The meta data for the event.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.addLocalMergeEvent = async ({event, meta, ledgerNodeId}) => {
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  const {creator: creatorId, generation} = meta.continuity2017;
  const {eventHash} = meta;
  const headKey = _cacheKey.head({creatorId, ledgerNodeId});
  const outstandingMergeEventKey = _cacheKey.outstandingMergeEvent(
    {eventHash, ledgerNodeId});
  const eventGossipKey = _cacheKey.eventGossip({eventHash, ledgerNodeId});
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const {parentHash, treeHash, type} = event;
  const parentHashes = parentHash.filter(h => h !== treeHash);
  // no need to set a headGeneration key here, those are only used for
  // processing peer merge events
  // TODO: `creator` is quite a long URL, can a substitution be made?

  // full event without meta goes into cache for gossip purposes
  const fullEvent = JSON.stringify({event});
  const metaString = JSON.stringify({meta});
  // for local merge events, only cache a summary of the event because that
  // is all that is needed for consensus to be computed
  const eventSummary = JSON.stringify({
    event: {parentHash, treeHash, type},
    meta: {eventHash, continuity2017: {creator: creatorId}}
  });
  try {
    const message = `merge|${JSON.stringify([creatorId])}`;
    const result = await cache.client.multi()
      .srem(childlessKey, parentHashes)
      .srem(localChildlessKey, parentHashes)
      // this key is removed when the event reaches consensus
      .set(outstandingMergeEventKey, eventSummary)
      // expire key which is used for gossip
      .hmset(eventGossipKey, 'event', fullEvent, 'meta', metaString)
      .expire(eventGossipKey, 600)
      .sadd(outstandingMergeKey, outstandingMergeEventKey)
      .hmset(headKey, 'h', eventHash, 'g', generation)
      .publish(`continuity2017|event|${ledgerNodeId}`, message)
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
 * Record that a new local regular event has been added that needs merging.
 *
 * @param eventHash {string} - The hash of the new local regular event.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.addLocalRegularEvent = async ({eventHash, ledgerNodeId}) => {
  // new local events are `childless` meaning that they have no events
  // that descend from them; they must be merged
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  const localRegularEventCountKey = _cacheKey.eventCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  return cache.client.multi()
    .sadd(childlessKey, eventHash)
    .sadd(localChildlessKey, eventHash)
    .incr(localRegularEventCountKey)
    .expire(localRegularEventCountKey, eventsConfig.counter.ttl)
    // TODO: do we still want/need to publish this message here?
    // ... what triggered adding a local event was a merge to begin with
    // inform agents/listeners about new regular events so that they can be
    // merged
    .publish(`continuity2017|needsMerge|${ledgerNodeId}`, 'regular')
    .exec();
};

/**
 * Get event hashes that have no children and are candidates to
 * be merged by the local node.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<string[]>} hashes for childless events.
 */
exports.getChildlessHashes = async ({ledgerNodeId}) => {
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const childlessHashes = await cache.client.smembers(childlessKey);
  return {childlessHashes};
};

/**
 * Get local event hashes that have no children and are candidates to
 * be merged by the local node.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<string[]>} hashes for childless events.
 */
exports.getLocalChildlessHashes = async ({ledgerNodeId}) => {
  const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  const localChildlessHashes = await cache.client.smembers(localChildlessKey);
  return {localChildlessHashes};
};

/**
 * Get events.
 *
 * @param eventHash {string|string[]} - The event hash(es) to get.
 * @param [includeMeta=false] {Boolean} - Include event meta data.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object[]>} The events.
 */
exports.getEvents = async ({eventHash, includeMeta = false, ledgerNodeId}) => {
  eventHash = [].concat(eventHash);
  const fields = ['event'];
  if(includeMeta) {
    fields.push('meta');
  }
  const eventKeys = eventHash.map(eventHash =>
    _cacheKey.eventGossip({eventHash, ledgerNodeId}));
  const txn = cache.client.multi();
  for(const key of eventKeys) {
    txn.hmget(key, ...fields);
  }

  const result = await txn.exec();
  return result.map(r => {
    if(includeMeta) {
      const [event, meta] = r;
      return {event, meta};
    }
    const [event] = r;
    return {event};
  });
};

/**
 * Get the current merge status information. This status information includes:
 * - whether or not the ledger node is lagging/behind in gossip
 *   (indicating that it should not merge any events until caught up) and the
 * - the peer IDs of any priority peers (peers whose merge events would most
 *   speed along consensus)
 * - the hashes of any peer childless events (targets for merging ... to become
 *   parents of the next potential merge event).
 * - the hashes of any local childless events (targets for merging ... to
 *   become parents of the next potential merge event).
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object>} The merge status info.
 */
exports.getMergeStatus = async ({ledgerNodeId}) => {
  // see if there are any childless events to be merged and if the node
  // is too far behind in gossip to merge
  const gossipBehindKey = _cacheKey.gossipBehind(ledgerNodeId);
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  const priorityPeersKey = _cacheKey.priorityPeers(ledgerNodeId);
  const [
    gossipBehind, priorityPeers, peerChildlessHashes, localChildlessHashes
  ] = await cache.client.multi()
    .get(gossipBehindKey)
    .smembers(priorityPeersKey)
    .sdiff(childlessKey, localChildlessKey)
    .smembers(localChildlessKey)
    .exec();
  // TODO: `gossipBehind` seems like it could have a more clear name
  return {
    gossipBehind: gossipBehind !== null,
    priorityPeers,
    peerChildlessHashes,
    localChildlessHashes
  };
};

/**
 * Store an event and meta data for gossip purposes.
 *
 * @param event {Object} - The event.
 * @param eventHash {string} - The event hash.
 * @param [expire=600] {Number} - Expire the event after the specified ms.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param meta {Object} - The event meta data.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setEventGossip = async (
  {event, eventHash, expire = 600, ledgerNodeId, meta}) => {
  const eventKey = _cacheKey.eventGossip({eventHash, ledgerNodeId});
  const eventString = JSON.stringify({event});
  const metaString = JSON.stringify({meta});
  return cache.client.multi()
    .hmset(eventKey, 'event', eventString, 'meta', metaString)
    .expire(eventKey, expire)
    .exec();
};

/**
 * Set the current head for a creator ID in the cache. This head is stable,
 * meaning it refers to an event that is in storage.
 *
 * @param creatorId {string} - The ID of the creator.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param eventHash {string} - The event hash for the head.
 * @param generation {Number} - The generation for the head.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setHead = async (
  {creatorId, ledgerNodeId, eventHash, generation}) => {
  const key = _cacheKey.head({creatorId, ledgerNodeId});
  await _setHead({key, eventHash, generation});
};

/**
 * Get the current head from the cache. This head is stable, meaning it refers
 * to an event that is in storage.
 *
 * @param creatorId {string} - The ID of the creator.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves to the head or `null` if none found.
 */
exports.getHead = async ({creatorId, ledgerNodeId}) => {
  const key = _cacheKey.head({creatorId, ledgerNodeId});
  return _getHead({key});
};

/**
 * Set the event hash for the genesis merge event.
 *
 * @param eventHash {string} - The genesis merge event hash.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @return {Promise} resolves once the operation completes.
 */
exports.setGenesisHead = async ({eventHash, ledgerNodeId}) => {
  const key = _cacheKey.genesis(ledgerNodeId);
  await cache.client.set(key, eventHash);
};

/**
 * Get the genesis merge event hash.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @return {Promise<Object|null>} The head or `null` if none found.
 */
exports.getGenesisHead = async ({ledgerNodeId}) => {
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
 * @param creatorId {string} - The ID of the creator.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object>} The latest head or `null` if none found.
 */
exports.getUncommittedHead = async ({creatorId, ledgerNodeId}) => {
  const key = _cacheKey.latestPeerHead({creatorId, ledgerNodeId});
  return _getHead({key});
};

/**
 * Gets the generation for a single event identified by the given event hash
 * and stored by the given ledger node. The generation indicates the order of
 * an event relative to its creator node. A generation of `0` is the genesis
 * event and events count up from there, scoped to each node.
 *
 * @param eventHash {string} - The hash of the event.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Number|null>} The generation for the event.
 */
exports.getGeneration = async ({eventHash, ledgerNodeId}) => {
  // first check cache
  const key = _cacheKey.headGeneration({eventHash, ledgerNodeId});
  const generation = await cache.client.get(key);
  if(generation !== null) {
    return parseInt(generation, 10);
  }
  // no generation found for the eventHash
  return null;
};

/**
 * Bulk sets the generation for every event in the given Map.
 *
 * @param generationMap {Map} - A Map of eventHash => generation to set.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setGenerations = async ({generationMap, ledgerNodeId}) => {
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
 * @param eventHashes {string[]} - The hashes of the events.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object>} An object with:
 *         generationMap: a Map of eventHash => generation where the entries
 *           will preserve the order of `eventHashes`; any missing generation
 *           will be `null`.
 *         notFound: an array of eventHashes that were not found.
 */
exports.getGenerations = async ({eventHashes, ledgerNodeId}) => {
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
 * in the event cache.
 *
 * @param eventHashes {string[]} - The hashes of the events.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<string[]>} The event hashes that are *not* in the cache.
 */
exports.difference = async ({eventHashes, ledgerNodeId}) => {
  if(eventHashes.length === 0) {
    return [];
  }
  // get a random key to temporarily store the results of the diff operation
  const diffKey = _cacheKey.diff(uuid());
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
 * Gets the recent history of merge events from the cache. This history includes
 * all merge events in the cache that have not achieved consensus and a map of
 * each event's hash to each event. The event data model is also modified
 * with custom fields to assist in the consensus algorithm. Each event
 * looks like: `{eventHash, event, meta, _children, _parents}`. The
 * `_children` and `_parents` arrays will be populated by the consensus
 * algorithm. The `event` value only has summary info like tree and parent
 * hashes and does not include operations.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object>} An object with:
 *           events - an array of events with a data model customized for
 *             the consensus algorithm.
 *           eventMap - a map of event hash => event (with custom data model).
 */
exports.getRecentHistory = async ({ledgerNodeId}) => {
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

  return {events, eventMap};
};

/**
 * Sets the number of available merge permits.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param permits {Number} - The number of merge permits to set.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setMergePermits = async ({ledgerNodeId, permits}) => {
  const key = _cacheKey.mergePermits(ledgerNodeId);
  return cache.client.set(key, `${permits}`);
};

/**
 * Gets up to a certain number of permits. If fewer permits are available
 * than were requested, the maximum number of available permits is returned.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param permits {Number} - The number of merge permits to request.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.requestMergePermits = async ({ledgerNodeId, permits}) => {
  const key = _cacheKey.mergePermits(ledgerNodeId);
  // atomically get previous permit count and set it to zero to ensure
  // no permits are taken while we update it
  let available = await cache.client.getset(key, '0');
  if(available === null) {
    // no permits available
    return {available: 0, received: 0};
  }
  // decrement `available` by `permits` without dropping below zero available
  available = parseInt(available, 10) || 0;
  const received = Math.min(available, permits);
  available -= received;
  if(available > 0) {
    // put back any permits not claimed
    await cache.client.incrby(key, `${available}`);
  }
  return {available, received};
};

/**
 * Releases unused merge permits.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param permits {Number} - The number of merge permits to release.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.releaseMergePermits = async ({ledgerNodeId, permits}) => {
  const key = _cacheKey.mergePermits(ledgerNodeId);
  return cache.client.incrby(key, `${permits}`);
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
