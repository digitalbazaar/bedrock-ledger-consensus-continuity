/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const _cacheKey = require('./cacheKey');
const logger = require('../logger');
const {config, util: {uuid}} = bedrock;

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
  const {creator: creatorId, generation, localAncestorGeneration, type} =
    meta.continuity2017;
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  //const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  //const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
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
    const {basisBlockHeight, mergeHeight} = event;
    const latestPeerHeadKey = _cacheKey.latestPeerHead(
      {creatorId, ledgerNodeId});
    // expire the key in an hour, in case the peer/creator goes dark
    txn.hmset(
      latestPeerHeadKey,
      'h', eventHash,
      'g', generation,
      'bh', basisBlockHeight,
      'mh', mergeHeight,
      'la', localAncestorGeneration);
    txn.expire(latestPeerHeadKey, 3600);
  }
  // add the hash to the set used to check for dups and ancestors
  //txn.sadd(eventQueueSetKey, eventHash);
  // create a key that contains the event and meta
  txn.set(eventKey, eventJson);
  // FIXME: do not add to event-writer queue
  // push to the list that is handled in the event-writer
  //txn.rpush(eventQueueKey, eventKey);
  // FIXME: remove unused message
  //txn.publish(`continuity2017|peerEvent|${ledgerNodeId}`, 'new');
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
  const {basisBlockHeight, mergeHeight} = event;
  const {creator: creatorId, generation, localAncestorGeneration} =
    meta.continuity2017;
  const {eventHash} = meta;
  const headKey = _cacheKey.head({creatorId, ledgerNodeId});
  const outstandingMergeEventKey = _cacheKey.outstandingMergeEvent(
    {eventHash, ledgerNodeId});
  const eventGossipKey = _cacheKey.eventGossip({eventHash, ledgerNodeId});
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const {parentHash, treeHash, type} = event;
  const parentHashes = parentHash.filter(h => h !== treeHash);

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
    const result = await cache.client.multi()
      .srem(childlessKey, parentHashes)
      .srem(localChildlessKey, parentHashes)
      // this key is removed when the event reaches consensus
      .set(outstandingMergeEventKey, eventSummary)
      // expire key which is used for gossip
      .hmset(eventGossipKey, 'event', fullEvent, 'meta', metaString)
      .expire(eventGossipKey, 600)
      .sadd(outstandingMergeKey, outstandingMergeEventKey)
      .hmset(
        headKey,
        'h', eventHash,
        'g', generation,
        'bh', basisBlockHeight,
        'mh', mergeHeight,
        'la', localAncestorGeneration)
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
      basisBlockHeight,
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
 * @param [isConfig=false] {Boolean} - `true` if the event is a
 *        `WebLedgerConfigurationEvent`.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.addLocalRegularEvent = async (
  {eventHash, ledgerNodeId, isConfig = false}) => {
  // new local regular events are `childless` meaning that they have no events
  // that descend from them; they must be merged
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  const localRegularEventCountKey = _cacheKey.eventCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  const multi = cache.client.multi()
    .sadd(childlessKey, eventHash)
    .sadd(localChildlessKey, eventHash)
    // this key is for stats gathering only; do not make the system depend
    // on it for proper functioning, it may get slightly out of sync due to
    // failures
    .incr(localRegularEventCountKey)
    .expire(localRegularEventCountKey, eventsConfig.counter.ttl);
  if(isConfig) {
    // must notify that a config needs merging
    multi.publish(`continuity2017|needsMerge|${ledgerNodeId}`, 'config');
  }
  return multi.exec();
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
  // see if there are any childless events to be merged
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  const [
    peerChildlessHashes, localChildlessHashes
  ] = await cache.client.multi()
    .sdiff(childlessKey, localChildlessKey)
    .smembers(localChildlessKey)
    .exec();
  // FIXME: enabling the cache to report not just what is childless but
  // what is mergeable (especially if we pass in `witnesses`) would speed
  // up processing
  return {
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
 * Compute the difference between the given event hashes and those that are
 * in the event cache.
 *
 * @param eventHashes {string[]} - The hashes of the events.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<string[]>} The event hashes that are *not* in the cache.
 */
exports.difference = async ({eventHashes, ledgerNodeId}) => {
  // FIXME: update this function to use an in-memory event cache vs. a redis
  // one ... or, alternatively, use a different cache key from `eventQueueSet`
  // as the event queue is no longer in redis
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
