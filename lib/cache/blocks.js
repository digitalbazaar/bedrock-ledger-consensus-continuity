/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

const api = {};
module.exports = api;

/**
 * Sets the current block height in the cache for a ledger node.
 *
 * @param blockHeight the block height to set.
 * @param ledgerNodeId the ID of the ledger node to update.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.setBlockHeight = async ({blockHeight, ledgerNodeId}) => {
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
api.commitBlock = async ({eventHashes, ledgerNodeId}) => {
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
