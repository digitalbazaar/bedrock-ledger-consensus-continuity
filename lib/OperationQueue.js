/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache/cache-key');
const _util = require('./util');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('./logger');

const eventsConfig = config['ledger-consensus-continuity'].events;

module.exports = class OperationQueue {
  constructor({ledgerNodeId}) {
    this.chunk = null;
    this.chunkCached = false;
    this.ledgerNodeId = ledgerNodeId;
    // key for the list of all operation keys in the next chunk
    this.chunkCacheKey = _cacheKey.operationSelectedList(ledgerNodeId);
    // keys for all operations in the next chunk in the queue
    this.opKeys = null;
    // key to master operation list (list of keys for all pending operations)
    this.opListKey = _cacheKey.operationList(ledgerNodeId);
    // tracks whether or not there are more operations after the next chunk
    this.hasMore = false;
  }

  /**
   * Returns whether or not there is another chunk of operations that can
   * be put into a local regular event.
   *
   * @return a Promise that resolves to `true` or `false`.
   */
  async hasNextChunk() {
    if(this.opKeys) {
      return true;
    }

    // first, see if the next chunk of operations has already been cached
    // in redis (this happens when a previous call to `hasNext` occurred but
    // the next chunk was not popped off the queue, likely because an event
    // creation failed... so we can resume here)
    this.opKeys = await cache.client.lrange(this.chunkCacheKey, 0, -1);
    if(this.opKeys.length > 0) {
      this.chunkCached = true;
      return true;
    }

    // no next chunk of operations cached yet, so create one that will be
    // cached when `getNextChunk` is called and its operations are retrieved
    const {maxOperations} = eventsConfig;
    const [listLength, nextKeys] = await cache.client.multi()
      .llen(this.opListKey)
      .lrange(this.opListKey, 0, maxOperations - 1)
      .exec();
    if(listLength === 0) {
      // no new operations
      this.opKeys = null;
      return false;
    }
    this.hasMore = listLength > maxOperations;
    logger.debug(`New operations found: ${listLength}`);
    this.opKeys = nextKeys;
    return true;
  }

  /**
   * Get the next set of operations to insert into a local regular event.
   *
   * @return a Promise that resolves to an object with:
   *   operations - an array of operations lexicographically ordered by hash.
   *   TODO: rename to `hasMore`
   *   truncated - true if there are even more operations after the next
   *     chunk of operations.
   */
  async getNextChunk() {
    if(this.chunk) {
      // next chunk already cached in memory, return it
      return this.chunk;
    }

    // create an atomic redis transaction that will:
    // 1. Get all operations matching `opKeys`.
    // Then, if the next chunk of operations hasn't been cached yet...
    // 2. Remove the operation keys from the master operation key list.
    // 3. Cache the next chunk of operations by storing their keys in a list.
    const getOperationsTxn = cache.client.multi().mget(this.opKeys);
    if(!this.chunkCached) {
      // remove next chunk ops from the master operation list
      // ltrim *keeps* items from start to end
      getOperationsTxn.ltrim(this.opListKey, this.opKeys.length, -1);
      // create the next chunk
      getOperationsTxn.rpush(this.chunkCacheKey, this.opKeys);
    }

    // execute the redis transaction and get the operations for the chunk
    const [opJsons] = await getOperationsTxn.exec();
    // Note: tested different methods for fastest parsing, this was the winner
    // https://github.com/digitalbazaar/loop-bench
    const operations = opJsons.map(JSON.parse);
    // lexicographic sort on the hash of the operation determines the
    // order of operations in events
    _util.sortOperations(operations);
    return this.chunk = {operations, truncated: this.hasMore};
  }

  /**
   * Pop the next chunk of operations off of the queue. This method should
   * only be called once the operations have been successfully written to
   * a local regular event.
   *
   * @return a Promise that resolves once the operation completes.
   */
  async popChunk() {
    if(this.chunk) {
      // clear the events from the redis operation queue
      await cache.client.multi()
        // remove the cached chunk
        .del(this.chunkCacheKey)
        // delete the keys that contain the operation documents
        .del(this.opKeys)
        .exec();
      this.opKeys = this.chunk = null;
      this.chunkCached = false;
    }
  }
};
