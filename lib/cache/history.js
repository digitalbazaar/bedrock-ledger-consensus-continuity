/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cacheKey');

/**
 * Set the current head for a creator ID in the cache. This head is stable,
 * meaning it refers to an event that is in storage.
 *
 * @param creatorId {string} - The ID of the creator.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param eventHash {string} - The event hash for the head.
 * @param generation {Number} - The generation for the head.
 * @param basisBlockHeight {Number} - The basisBlockHeight for the head.
 * @param mergeHeight {Number} - The mergeHeight for the head.
 * @param localAncestorGeneration {Number} - The localAncestorGeneration for
 *   the head.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setHead = async ({
  creatorId, ledgerNodeId, eventHash, generation,
  basisBlockHeight, mergeHeight, localAncestorGeneration
}) => {
  const key = _cacheKey.head({creatorId, ledgerNodeId});
  await _setHead({
    key, eventHash, generation, basisBlockHeight, mergeHeight,
    localAncestorGeneration
  });
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
    return {
      eventHash,
      generation: 0,
      basisBlockHeight: 0,
      mergeHeight: 0,
      localAncestorGeneration: 0
    };
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

async function _setHead({
  key, eventHash, generation, basisBlockHeight, mergeHeight,
  localAncestorGeneration
}) {
  return cache.client.hmset(
    key,
    'h', eventHash,
    'g', generation,
    'bh', basisBlockHeight,
    'mh', mergeHeight,
    'la', localAncestorGeneration);
}

async function _getHead({key}) {
  const [
    eventHash, generation, basisBlockHeight, mergeHeight,
    localAncestorGeneration
  ] = await cache.client.hmget(key, 'h', 'g', 'bh', 'mh', 'la');
  // redis returns null if key is not found
  if(eventHash === null) {
    return null;
  }
  return {
    eventHash,
    generation: parseInt(generation, 10),
    basisBlockHeight: parseInt(basisBlockHeight, 10),
    mergeHeight: parseInt(mergeHeight, 10),
    localAncestorGeneration: parseInt(localAncestorGeneration, 10)
  };
}
