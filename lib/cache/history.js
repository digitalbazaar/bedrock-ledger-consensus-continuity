/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const _cacheKey = require('./cacheKey');
const {util: {BedrockError}} = bedrock;

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
 * Gets the recent history of merge events from the cache. This history includes
 * all merge events in the cache that have not achieved consensus and a map of
 * each event's hash to each event. Each event looks like:
 * `{eventHash, event, meta}`.  The `event` value only has summary
 * info like tree and parent hashes and does not include operations.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object>} An object with:
 *           events - an array of events with a data model customized for
 *             the consensus algorithm.
 */
exports.getRecent = async ({ledgerNodeId}) => {
  const events = [];

  // get all *merge* events available
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const keys = await cache.client.smembers(outstandingMergeKey);
  if(!(keys && keys.length > 0)) {
    // no recent history to merge detected
    return {events};
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
      eventHash,
      event: {parentHash, treeHash, type},
      meta: {continuity2017: {creator}}
    };
    events.push(doc);
  }

  return {events};
};

async function _setHead({key, eventHash, generation}) {
  // FIXME: include `basisBlockHeight` as `b` field
  return cache.client.hmset(key, 'h', eventHash, 'g', generation);
}

async function _getHead({key}) {
  // FIXME: include `basisBlockHeight` as `b` field
  const [eventHash, generation] = await cache.client.hmget(key, 'h', 'g');
  // redis returns null if key is not found
  if(eventHash === null) {
    return null;
  }
  return {eventHash, generation: parseInt(generation, 10)};
}
