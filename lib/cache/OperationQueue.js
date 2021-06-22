/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _continuityConstants = require('../continuityConstants');
const _util = require('../util');
const database = require('bedrock-mongodb');
const logger = require('../logger');
const pLimit = require('p-limit');

const limit = pLimit(25);
const DEFAULT_MAX_CACHED_QUEUE_SIZE = 100;

module.exports = class OperationQueue {
  constructor({ledgerNode} = {}) {
    this.chunk = null;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    // tracks whether or not there are more operations after the next chunk
    this.basisBlockHeight = -1;
    this.hasMore = false;

    this.collection = database.collections.continuity2017_operation_queue;
    this.queue = null;
    this.operations = [];
    this.removedOperations = [];
    this.removing = null;
    this.window = {
      head: 0,
      tail: 0
    };
    this.opCount = 0;

    this.MAX_OPERATIONS = _continuityConstants.events.maxOperations;
    this.MAX_CACHED_QUEUE_SIZE = Math.max(
      DEFAULT_MAX_CACHED_QUEUE_SIZE,
      this.MAX_OPERATIONS);
  }

  /**
   * Returns whether or not there is another chunk of operations that can
   * be put into a local regular event.
   *
   * @return a Promise that resolves to `true` or `false`.
   */
  async hasNextChunk() {
    if(this.opCount > 0 && this.basisBlockHeight >= 0) {
      return true;
    }

    try {
      // wait for removal of previous chunk before continuing
      await this.removing;
    } catch(e) {
      // ignore errors if any exist
    }

    // no next chunk of operations cached yet, so create one that will be
    // cached when `getNextChunk` is called and its operations are retrieved
    if(!(Array.isArray(this.queue) && this.window.tail < this.queue.length)) {
      this.window.head = 0;
      this.window.tail = 0;
      this.basisBlockHeight = -1;
      this.queue = null;

      const {ledgerNodeId} = this;
      const projection = {_id: 0, meta: 1, operation: 1};
      const records = await this.collection.find({ledgerNodeId}, {projection})
        .sort({'meta.created': 1}).limit(this.MAX_CACHED_QUEUE_SIZE).toArray();

      if(records.length === 0) {
        // no new operations
        return false;
      }
      this.queue = records;
    }

    // record the basisBlockHeight of the first operation,
    const basisBlockHeight = this.basisBlockHeight =
      this.queue[this.window.head].meta.basisBlockHeight;

    // since the queue is FIFO, we can stop when we hit an operation with a
    // different basisBlockHeight value
    let opCount = 0;
    let validHead = -1;
    for(let i = this.window.head; i < this.queue.length; ++i) {
      const operation = this.queue[i];
      const bbh = operation.meta.basisBlockHeight;
      if(bbh !== basisBlockHeight || opCount >= this.MAX_OPERATIONS) {
        break;
      }
      if(validHead === -1) {
        try {
          const {valid} = await this._isValidOperation({operation});
          if(!valid) {
            logger.error(
              'Non-critical operation validation error. ' +
              'Invalid head of operation queue.', {
                operation
              });
            continue;
          }
          validHead = i;
        } catch(e) {
          logger.error(
            'Non-critical operation validation error. ' +
            'Invalid head of operation queue.', {
              operation,
              error: e
            });
          continue;
        }
      }
      ++opCount;
    }

    if(validHead === -1) {
      return false;
    }
    this.window.head = validHead;
    logger.debug('New operations found.', {basisBlockHeight, opCount});

    // record that a subset of the available operations is being returned
    this.opCount = opCount;
    this.hasMore = (this.window.tail + this.opCount) < this.queue.length;

    return true;
  }

  /**
   * Get the next set of operations to insert into a local regular event.
   *
   * @return {Promise} that resolves to an object with:
   *   operations - an array of operations lexicographically ordered by hash.
   *   hasMore - true if there are even more operations after the next
   *     chunk of operations.
   */
  async getNextChunk() {
    if(this.chunk) {
      // next chunk already cached in memory, return it
      return this.chunk;
    }

    const operations = this.operations = await this._getChunkOperations();

    // lexicographic sort on the hash of the operation determines the
    // order of operations in events
    _util.sortOperations(operations);

    return this.chunk = {
      basisBlockHeight: this.basisBlockHeight,
      hasMore: this.hasMore,
      operations
    };
  }

  /**
   * Pop the next chunk of operations off of the queue. This method should
   * only be called once the operations have been successfully written to
   * a local regular event.
   *
   * @return {Promise} resolves once the operation completes.
   */
  async popChunk() {
    this.chunk = null;
    this.window.head += this.opCount;
    this.window.tail += this.opCount;
    this.opCount = 0;

    const {operations} = this;
    this.removing = this._removeOperationsFromQueue({operations}).catch(e => {
      logger.error('Non-critical operation removal error', {error: e});
    });
    this.operations = [];
  }

  async _getChunkOperations() {
    // an array to gather all successful operations for a chunk
    const operations = [];
    // the stopping point for iterating through the local queue (exclusive)
    const end = this.window.head + this.opCount;
    // a set to help enforce uniqueness across a chunk of operations
    const seenOperationHashes = new Set();
    // an array to gather promises
    const promises = [];

    for(let i = this.window.head; i < end; i++) {
      const operation = this.queue[i];

      // ensure we have not previously seen this operation hash
      if(seenOperationHashes.has(operation.meta.operationHash)) {
        this.removedOperations.push(operation);
        continue;
      }

      // push the result of validating an operation to the promises array
      promises.push(limit(async () => {
        try {
          // the first operation at the head of the window is always validated,
          // so only validate when the current index `i` is not the index of the
          // the first operation found at index `this.window.head`
          if(i !== this.window.head) {
            const {valid} = await this._isValidOperation({operation});
            if(!valid) {
              this.removedOperations.push(operation);
              // return early to prevent adding the operation
              return;
            }
          }
          // adds the first operation and all successfully validated operations
          operations.push(operation);
        } catch(e) {
          logger.error('Non-critical operation validation error', {
            error: e,
            operation
          });
          this.removedOperations.push(operation);
        }
      }));

      seenOperationHashes.add(operation.meta.operationHash);
    }

    await Promise.all(promises);

    if(this.removedOperations.length > 0) {
      const msg = 'Non-critical error. Removed invalid operations from chunk';
      logger.error(msg, {n: this.removedOperations.length});
    }

    return operations;
  }

  async _removeOperationsFromQueue({operations = []} = {}) {
    for(const operation of operations) {
      this.removedOperations.push(operation);
    }

    if(this.removedOperations.length === 0) {
      return;
    }

    try {
      const bulkOp = this.collection.initializeUnorderedBulkOp();

      for(const {meta} of this.removedOperations) {
        bulkOp.find({
          ledgerNodeId: this.ledgerNodeId,
          'meta.operationHash': meta.operationHash
        }).delete();
      }

      await bulkOp.execute();

      this.removedOperations = [];
    } catch(e) {
      logger.error('Error during operation removal from database queue.', {
        error: e
      });
    }
  }

  async _isValidOperation({operation}) {
    const {meta: {basisBlockHeight, operationHash}} = operation;
    const {ledgerNode} = this;

    const [result, exists] = await Promise.all([
      ledgerNode.operations.validate({
        basisBlockHeight,
        ledgerNode,
        operation: operation.operation
      }),
      ledgerNode.storage.operations.exists({operationHash})
    ]);

    return {valid: !exists && result.valid};
  }
};
