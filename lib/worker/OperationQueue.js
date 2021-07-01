/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _continuityConstants = require('../continuityConstants');
const _util = require('../util');
const database = require('bedrock-mongodb');
const logger = require('../logger');
const pLimit = require('p-limit');

/* Controls the max concurrency for a given number of promises that are
validating operations. This is done to ensure we reduce pressure on the
NodeJS Promise Queue and limit the number concurrent I/O operations. The
number 25 was chosen to be 1/10th the max number of concurrent ops
being validated from the queue.*/
const limit = pLimit(25);
// Controls the maximum number of documents to pull from the database and cache
// locally. The Math.max(DEFAULT_MAX_CACHED_QUEUE_SIZE, MAX_OPS_PER_REG_EVENT)
// will be the final value used to limit the number of documents pulled from
// the database.
const DEFAULT_MAX_CACHED_QUEUE_SIZE = 250;

// Error Messages
const OPERATIONS_REMOVAL_ERROR = 'Failed to remove operations from queue.';
const OPERATION_VALIDATION_ERROR =
  'Non-critical operation validation error. Invalid head of operation queue.';

module.exports = class OperationQueue {
  constructor({worker}) {
    // session worker
    this.worker = worker;
    const {ledgerNode} = worker;
    // ledger node instance
    this.ledgerNode = ledgerNode;
    // ledger node's id
    this.ledgerNodeId = ledgerNode.id;
    // operation queue collection
    this.collection = database.collections.continuity2017_operation_queue;
    // cached chunk of operations to return for a regular event
    this.chunk = null;
    // tracks whether or not there are more operations after the next chunk
    this.hasMore = false;
    // cached queue from the database
    this.queue = null;
    // index of the head of the cached queue of operations
    this.head = 0;
    // count of possible operations to be added in the chunk
    this.opCount = 0;
    // list of operations to be removed
    this.removedOperations = [];
    // caches operation removal promise
    this.removing = null;
    // caches database empty check promise
    this.databaseEmpty = null;
    // caches fetch record promise
    this.fetchRecords = null;
    // constants
    this.MAX_OPERATIONS = _continuityConstants.events.maxOperations;
    this.MAX_CACHED_QUEUE_SIZE = Math.max(
      DEFAULT_MAX_CACHED_QUEUE_SIZE,
      this.MAX_OPERATIONS);
  }

  /**
   * Returns whether there is another chunk of operations that can be put into a
   * local regular event. This MUST NOT be called concurrently.
   *
   * @return a Promise that resolves to `true` or `false`.
   */
  async hasNextChunk() {
    // if opCount is non-zero then the function has previously been called and
    // the chunk was not popped from the queue
    if(this.opCount > 0) {
      return true;
    }

    const {ledgerNodeId} = this;
    // wait for removal of previous chunk before continuing
    if(this.removing) {
      await this.removing.catch(e => {
        logger.error(OPERATIONS_REMOVAL_ERROR, {ledgerNodeId, error: e});
      });
    }

    // if the queue is not initialized with the head of the queue before the end
    // of the queue, then no next chunk of operations is cached yet, so create
    // one that will be cached when `getNextChunk` is called and its operations
    // are retrieved
    const end = (this.queue || []).length - 1;
    if(!(Array.isArray(this.queue) && this.head < end)) {
      this.head = 0;
      this.queue = null;

      if(this.fetchRecords === null) {
        const {MAX_CACHED_QUEUE_SIZE} = this;
        const projection = {_id: 0, meta: 1, operation: 1};
        this.fetchRecords = this.collection.find({ledgerNodeId}, {projection})
          .sort({'meta.created': 1}).limit(MAX_CACHED_QUEUE_SIZE).toArray();
      }

      try {
        const records = await this.fetchRecords;

        if(records.length === 0) {
          this.fetchRecords = null;
          // no new operations
          return false;
        }

        this.queue = records;
      } catch(e) {
        this.fetchRecords = null;
        const msg = 'Error fetching operations from queue in database.';
        logger.error(msg, {ledgerNodeId, error: e});
        // error fetching records from queue
        return false;
      }
    }

    const {head, opCount} = await this._getHead();

    if(head === null) {
      this.fetchRecords = null;
      logger.debug('Unable to find a valid head of operation queue.', {
        ledgerNodeId
      });
      return false;
    }

    this.head = head;
    this.opCount = opCount;

    const basisBlockHeight = this.worker.consensusState.blockHeight;
    logger.debug('New operations found.', {
      ledgerNodeId,
      basisBlockHeight,
      opCount
    });

    // record that there are more operations to process
    this.hasMore = (this.head + this.opCount) < this.queue.length;
    // only schedule promise to check for an empty queue in database if
    // `this.queue`, the cached queue, has been exhausted
    if(!this.hasMore) {
      this.databaseEmpty = this._isDatabaseEmpty();
    }

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

    const operations = await this._getValidOperations();

    // lexicographic sort on the hash of the operation determines the
    // order of operations in events
    _util.sortOperations(operations);

    return this.chunk = {
      // we only await the check for an empty database if the cached queue was
      // exhausted
      hasMore: this.hasMore || !await this.databaseEmpty,
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
    const {operations} = this.chunk;
    this.chunk = null;
    this.head += this.opCount;
    this.opCount = 0;
    this.fetchRecords = null;

    const {ledgerNodeId} = this;
    this.removing = this._removeOperationsFromQueue({operations}).catch(e => {
      logger.error(OPERATIONS_REMOVAL_ERROR, {ledgerNodeId, error: e});
    });
  }

  async _getValidOperations() {
    // an array to gather all successful operations for a chunk
    const operations = [];
    // the stopping point for iterating through the local queue (exclusive)
    const end = Math.min((this.head + this.opCount), this.queue.length);
    // an array to gather promises
    const promises = [];

    for(let i = this.head; i < end; i++) {
      const operation = this.queue[i];
      // push the result of validating an operation to the promises array
      promises.push(limit(async () => {
        try {
          // the first operation at the head of the queue is always validated,
          // so only validate when the current index `i` is not the index of the
          // the first operation found at index `this.head`
          if(i !== this.head) {
            const {valid} = await this._isValidOperation({operation});
            if(!valid) {
              this.removedOperations.push(operation);
              // return early to prevent adding the operation
              return;
            }
          }
          // adds the first operation and all other successfully validated
          // operations
          operations.push(operation);
        } catch(e) {
          logger.debug(OPERATION_VALIDATION_ERROR,
            {ledgerNodeId: this.ledgerNodeId, operation, error: e});
          this.removedOperations.push(operation);
        }
      }));
    }

    await Promise.all(promises);

    if(this.removedOperations.length > 0) {
      const msg = 'Non-critical error. Removed invalid operations from chunk.';
      const {ledgerNodeId} = this;
      logger.debug(msg, {ledgerNodeId, n: this.removedOperations.length});
    }

    return operations;
  }

  async _getHead() {
    let opCount = 0;
    let head = null;

    for(let i = this.head; i < this.queue.length; ++i) {
      const operation = this.queue[i];
      if(opCount >= this.MAX_OPERATIONS) {
        break;
      }
      if(head === null) {
        try {
          const {valid} = await this._isValidOperation({operation});
          if(!valid) {
            this.removedOperations.push(operation);
            logger.debug(OPERATION_VALIDATION_ERROR, {operation});
            continue;
          }
          head = i;
        } catch(e) {
          this.removedOperations.push(operation);
          logger.debug(OPERATION_VALIDATION_ERROR,
            {ledgerNodeId: this.ledgerNodeId, operation, error: e});
          continue;
        }
      }
      ++opCount;
    }

    if(this.removedOperations.length > 0) {
      this.removing = this._removeOperationsFromQueue().catch(e => {
        logger.error(OPERATIONS_REMOVAL_ERROR,
          {ledgerNodeId: this.ledgerNodeId, error: e});
      });
    }

    return {head, opCount};
  }

  async _isDatabaseEmpty() {
    const {ledgerNodeId} = this;
    const projection = {_id: 1};
    try {
      const count = await this.collection.find({ledgerNodeId}, {projection})
        .limit(1).count();
      return count === 0;
    } catch(e) {
      logger.error('Error checking database for empty operation queue.', {
        ledgerNodeId,
        error: e
      });
      return true;
    }
  }

  async _removeOperationsFromQueue({operations = []} = {}) {
    for(const operation of operations) {
      this.removedOperations.push(operation);
    }

    if(this.removedOperations.length === 0) {
      return;
    }

    const {ledgerNodeId} = this;
    try {
      const bulkOp = this.collection.initializeUnorderedBulkOp();

      for(const {meta} of this.removedOperations) {
        bulkOp.find({
          ledgerNodeId,
          'meta.operationHash': meta.operationHash
        }).delete();
      }

      await bulkOp.execute();

      this.removedOperations = [];
    } catch(e) {
      logger.error(OPERATIONS_REMOVAL_ERROR, {ledgerNodeId, error: e});
    }
  }

  async _isValidOperation({operation}) {
    const {meta: {operationHash}} = operation;
    const {ledgerNode} = this;

    const [result, exists] = await Promise.all([
      this._validateOperation({
        basisBlockHeight: this.worker.consensusState.blockHeight,
        ledgerNode,
        operation
      }),
      ledgerNode.storage.operations.exists({operationHash})
    ]);

    return {valid: !exists && result.valid};
  }

  async _validateOperation({operation, basisBlockHeight}) {
    const {meta: {basisBlockHeight: operationBbh}} = operation;
    const {ledgerNode} = this;

    /* Operations are validated at the latest block height of the ledger node
	  before being accepted into the queue. We can return early when the ledger
	  node has not advanced its block height, therefore preventing
	  duplicating the execution of the same validation check. */
    if(operationBbh === basisBlockHeight) {
      return {valid: true};
    }

    const result = await ledgerNode.operations.validate({
      basisBlockHeight,
      ledgerNode,
      operation: operation.operation
    });

    return result;
  }
};
