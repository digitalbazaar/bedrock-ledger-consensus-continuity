/*!
 * Copyright (c) 2021 Digital Bazaar, Inc. All rights reserved.
 */
const database = require('bedrock-mongodb');
const logger = require('../logger');
const LRU = require('lru-cache');
const {config, util: {BedrockError}} = require('bedrock');
const {LruCache} = require('@digitalbazaar/lru-memoize');

/* Note: Max size is 1. This is to promise queue is only used for the update
   count function. */
const lruCacheOptions = {max: 1, disposeOnSettle: true};
const UPDATE_COUNT_PROMISE_QUEUE = new LruCache(lruCacheOptions);

// Error Messages
const DATABASE_QUEUE_LENGTH_ERROR =
  'Non-critical error: failed to cache length of operation queue on disk.';

class OperationWriter {
  /**
   * Constructor for creating OperationWriters. The defaults for `timeout` and
   * `highWaterMark` were chosen to ensure that at peak load, the operation
   * writer will flush a 100 operations/second to the persisteted queue in the
   * database. In the extraordinary case, where the process dies between
   * flushes, a maximum of 100 lost operations was deemed okay. A timeout of
   * 1 second strikes the proper balance of ensuring timely flushes to the
   * database, while also preventing overloading the database with constant
   * requests.
   *
   * @param {object} options - Options to use.
   * @param {object} options.ledgerNodeId - The `id` of the ledger node.
   * @param {object} options.highWaterMark - The max number of records the
   * writer should retain in memory before forcing a flush to the database.
   * @param {object} options.timeout - The max amount of time in milliseconds
   * records should be retained in memory before forcing a flush to the databse.
   */
  constructor({ledgerNodeId, highWaterMark = 100, timeout = 1000} = {}) {
    if(!ledgerNodeId) {
      throw new Error(`"ledgerNodeId" is required.`);
    }
    this.ledgerNodeId = ledgerNodeId;
    // maximum number of operations to have in the queue before there is
    // pressure to schedule flushing the queue
    this.highWaterMark = highWaterMark;
    // the maximum amount of time that an operation can sit in the queue
    // before being flushed when the operation writer is running
    this.timeout = timeout;
    // whether or not the operation writer is running
    this.running = false;
    // an ID for when the next timeout-based flush will occur
    this.timeoutId = null;
    // the queue of operations
    this.queue = [];
    // current stopped promise; resolves once stopped
    this.stopped = null;
    this._resolveStopped = null;
    /* Tracks operations hashes for soft duplicate prevention within the same
    process. This acts as a filter and first line of defense before inserting
    operations into the queue in the database. If this soft check fails, then
    the operation will try to be inserted into the database and yield a
    DuplicateError. If a duplicate operation was inserted into the database and
    did not already exist in the queue, then it will be filtered out when read
    from the consensus worker.
    */
    this.recentOperationHashes = new LRU({max: 1000});
    // operation queue collection
    this.collection = database.collections.continuity2017_operation_queue;
    // queue length in database
    this.databaseOperationQueueLength = 0;
    const operationsConfig = config['ledger-consensus-continuity'].operations;
    // max operation queue length
    this.MAX_OPERATION_QUEUE_LENGTH = operationsConfig.maxQueueSize;
  }

  /**
   * Starts this OperationWriter. Any operations that have been added to its
   * queue will be bulk inserted when the queue fills or when a timeout occurs.
   */
  start() {
    const {queue, running, highWaterMark, timeout} = this;
    if(running) {
      // already running
      return;
    }

    // now running
    this.running = true;

    // if the queue is at or over the highWaterMark, schedule immediate flush
    if(queue.length >= highWaterMark) {
      this._scheduleFlush({timeout: 0});
    } else if(queue.length > 0) {
      // queue is not empty, schedule a timeout flush
      this._scheduleFlush({timeout});
    }

    this.stopped = new Promise(res => {
      this._resolveStopped = res;
    });
  }

  /**
   * Stops this OperationWriter. Events can still be added to the queue, but
   * they will not be automatically bulk inserted.
   */
  stop() {
    const {running, timeoutId} = this;
    if(!running) {
      // already not running
      return;
    }
    // cancel any scheduled flush
    if(timeoutId !== null) {
      clearTimeout(timeoutId);
      this.timeoutId = null;
    }
    this.running = false;
    this.stopped = null;
    this._resolveStopped();
  }

  /**
   * Adds an operation to be written. If this OperationWriter is running and the
   * operation causes the queue to reach the configured `highWaterMark`, then
   * it will automatically flush operations to the underlying database via a
   * bulk insert.
   *
   * @param {object} options - Options to use.
   * @param {object} options.operation - The operation to write.
   * @param {object} options.meta - The metadata to write.
   * @param {object} [options.forceFlush=false] - Force the queue to flush to
   * the database after adding the operation.
   */
  async add({operation, meta, forceFlush = false} = {}) {
    const {queue, running, highWaterMark, MAX_OPERATION_QUEUE_LENGTH} = this;

    if(this.databaseOperationQueueLength >= MAX_OPERATION_QUEUE_LENGTH) {
      /* The operation writer has no feedback when the queue gets drained. We
      must update the queue length to see if future operations may be accepted.
      We intentionally do not await the promise, the length can be updated
      after the error is returned without ill effects. It is by design that the
      current operation will be rejected before the length is updated. */
      this._updateDatabaseQueueLength().catch(e => {
        logger.debug(DATABASE_QUEUE_LENGTH_ERROR, {error: e});
      });
      throw new BedrockError(
        'The node is not accepting operations. Try again later.',
        'OperationError', {
          httpStatusCode: 503,
          public: true,
        });
    }
    /* Performs a soft duplicate check. This check does not prevent other
    processes from accepting the operation or the case where the operation's
    hash fell out of the in-memory LRU Cache. When the cheap and fast soft
    check fails, there exists slower and robust checks when the operation
    is inserted into the queue and then read from the queue. */
    if(this._isDuplicateOperation(meta)) {
      throw new BedrockError(
        'The operation already exists.',
        'DuplicateError', {
          duplicateLocation: 'cache',
          httpStatusCode: 409,
          ledgerNodeId: this.ledgerNodeId,
          operationRecord: {operation, meta},
          public: true
        });
    }

    // queue the operation
    queue.push({operation, meta});

    // do not flush if not writer is not running
    if(!running) {
      return;
    }

    // force flush if required
    if(forceFlush) {
      await this.flush();
      // return as flush was forced; no need to potentially schedule a flush
      return;
    }

    // if the queue length matches the highWaterMark, schedule an immediate
    // flush; only schedule this when they match to prevent repeatedly
    // scheduling flushes or preventing flushes under heavy load
    if(queue.length === highWaterMark) {
      this._scheduleFlush({timeout: 0});
      return;
    }

    // schedule a flush if one has not already been scheduled
    const {timeout, timeoutId} = this;
    if(timeoutId === null) {
      this._scheduleFlush({timeout});
    }
  }

  /**
   * Forcibly flushes the operation queue, bulk inserting any pending
   * operations.
   *
   * @returns {Promise} A promise that resolves once the operation completes.
   */
  async flush() {
    // clear any scheduled flush
    if(this.timeoutId !== null) {
      clearTimeout(this.timeoutId);
      this.timeoutId = null;
    }

    if(this.queue.length === 0) {
      return;
    }

    // save current queue and replace it with a new one
    const queue = this.queue;
    this.queue = [];

    const now = Date.now();
    const records = queue.map(({meta, operation}) => ({
      ledgerNodeId: this.ledgerNodeId,
      meta: {
        ...meta,
        created: now,
        updated: now
      },
      operation
    }));

    try {
      /* */
      const writeOptions = {...database.writeOptions, ordered: false};
      await this.collection.insertMany(records, writeOptions);
      await this._updateDatabaseQueueLength().catch(e => {
        logger.debug(DATABASE_QUEUE_LENGTH_ERROR, {error: e});
      });
    } catch(e) {
      // ignore duplicates; other processes that do not share memory
      // with this one could have concurrently written duplicate ops;
      // these duplicate ops must be filtered out later by the worker
      // that puts ops into events
      if(!database.isDuplicateError(e)) {
        throw e;
      }
    }
  }

  /**
   *
   * Updates the length of the database in queue in memory if it hasn't been
   * updated too recently. Updates to the database queue length are debounced
   * for better performance. Getting a consistent value for the database queue
   * length is also not part of the design, as multiple isolated processes may
   * be writing to the database queue concurrently and another isolated process
   * may be draining it concurrently. Therefore, design here presumes there
   * will be not strong consistency when tracking the database length; rather,
   * it allows for some operations to be refused to be queued when under load,
   * even if it may have been technically possible for those operations to be
   * queued with a strongly consistent design. This is considered to be an
   * acceptable condition given the performance benefits from not requiring
   * strong consistency and with the observation that false positives will be
   * in the margins on a taxed system.
   */
  async _updateDatabaseQueueLength() {
    const fn = async () => {
      const {ledgerNodeId} = this;
      const count = await this.collection.countDocuments({ledgerNodeId});
      this.databaseOperationQueueLength = count;
    };

    try {
      await UPDATE_COUNT_PROMISE_QUEUE.memoize({key: 'updateCount', fn});
    } catch(e) {
      logger.debug(DATABASE_QUEUE_LENGTH_ERROR, {error: e});
    }
  }

  /**
   * Schedules the queue to be flushed after the given timeout.
   *
   * @param {object} options - Options to use.
   * @param {object} options.timeout - The timeout after which to run `flush`.
   */
  _scheduleFlush({timeout} = {}) {
    if(this.timeoutId !== null) {
      clearTimeout(this.timeoutId);
    }
    this.timeoutId = setTimeout(() => this.flush().catch(e => {
      logger.error('Error writing to database.', {error: e});
    }), timeout);
  }

  /**
   * Checks if the operation is a duplicate.
   *
   * @param {object} meta - The metadata of an operation.
   * @param {object} meta.operationHash - The hash of the operation.
   */
  _isDuplicateOperation({operationHash}) {
    const found = this.recentOperationHashes.get(operationHash);
    if(found) {
      return true;
    }
    this.recentOperationHashes.set(operationHash, true);
    return false;
  }
}

module.exports = OperationWriter;
