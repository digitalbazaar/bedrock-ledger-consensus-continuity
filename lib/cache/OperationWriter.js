/*!
 * Copyright (c) 2021 Digital Bazaar, Inc. All rights reserved.
 */
const database = require('bedrock-mongodb');
const logger = require('../logger');
const LRU = require('lru-cache');
const {util: {BedrockError}} = require('bedrock');

class OperationWriter {
  constructor({ledgerNodeId, highWaterMark = 100, timeout = 1000} = {}) {
    if(!ledgerNodeId) {
      throw new Error(`"ledgerNodeId" is required.`);
    }
    this.ledgerNodeId = ledgerNodeId;
    // maximum number of operations to have in the queue before there is
    // pressure apply to flush the queue
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
    // tracks operation hashes for duplicate prevention
    this.recentOperationHashes = new LRU({
      max: 1000, maxAge: 5 * 60 * 1000, updateAgeOnGet: false
    });
    // operation queue collection
    this.collection = database.collections.continuity2017_operation_queue;
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
   * operation causes the queue to reach the configured capacity threshold, then
   * it will automatically flush operations to the underlying database via a
   * bulk insert.
   *
   * @param {object} options - Options to use.
   * @param {object} options.operation - The operation to write.
   * @param {object} options.meta - The metadata to write.
   *
   */
  async add({operation, meta, forceFlush = false} = {}) {
    const {queue, running, highWaterMark} = this;

    if(this._isDuplicateOperation({meta})) {
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
    }

    // if the queue length matches the highWaterMark, schedule an immediate
    // flush; only schedule this when they match do prevent repeatedly
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

    // save current queue and replace it with a new one
    const queue = this.queue;
    this.queue = [];

    if(queue.length === 0) {
      return;
    }

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
      const writeOptions = {...database.writeOptions, ordered: false};
      await this.collection.insertMany(records, writeOptions);
    } catch(e) {
      if(!database.isDuplicateError(e)) {
        throw e;
      }
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

  _isDuplicateOperation({meta}) {
    const {operationHash} = meta;
    if(!operationHash) {
      throw new Error('`meta` must contain "operationHash".');
    }
    const found = this.recentOperationHashes.get(operationHash);
    if(found) {
      return true;
    }
    this.recentOperationHashes.set(operationHash, true);
    return false;
  }
}

module.exports = OperationWriter;
