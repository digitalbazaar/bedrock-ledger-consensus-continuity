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

module.exports = class OperationQueue {
  constructor({ledgerNode} = {}) {
    this.chunk = null;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    // tracks whether or not there are more operations after the next chunk
    this.basisBlockHeight = -1;
    this.hasMore = false;
    this.hMore = false;

    this.collection = database.collections['bedrock-ledger-operation-queue'];
    this.queue = null;
    this.window = {
      head: 0,
      tail: 0
    };
    this.opCount = -1;
  }

  /**
   * Returns whether or not there is another chunk of operations that can
   * be put into a local regular event.
   *
   * @return a Promise that resolves to `true` or `false`.
   */
  async hasNextChunk() {
    if(this.opCount >= 0 && this.basisBlockHeight >= 0) {
      return true;
    }

    // no next chunk of operations cached yet, so create one that will be
    // cached when `getNextChunk` is called and its operations are retrieved
    const {events: {maxOperations}} = _continuityConstants;
    if(!(Array.isArray(this.queue) && this.window.tail < this.queue.length)) {
      this.window.head = 0;
      this.window.tail = 0;
      this.basisBlockHeight = -1;
      this.queue = null;

      const doc = await this.collection.findOne({
        'queue.ledgerNodeId': this.ledgerNodeId
      });
      if(!doc) {
        return false;
      }
      const {queue: {records}} = doc;
      const listLength = records.length;
      if(listLength === 0) {
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
      if(bbh !== basisBlockHeight || opCount >= maxOperations) {
        break;
      }
      if(validHead === -1) {
        try {
          const {valid} = await this._isValidOperation({operation});
          if(!valid) {
            console.log('Head not valid');
            continue;
          }
          validHead = i;
        } catch(e) {
          console.error('Error validating head');
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

    const operations = [];
    const end = this.window.head + this.opCount;

    let removed = 0;
    const localDuplicateOpMap = new Map();
    const promises = [];
    for(let i = this.window.head; i < end; i++) {
      const operation = this.queue[i];
      if(localDuplicateOpMap.has(operation.meta.operationHash)) {
        removed++;
        continue;
      }
      promises.push(limit(async () => {
        try {
          // the first operation at the head of the window is always validated
          if(i !== this.window.head) {
            const {valid} = await this._isValidOperation({operation});
            if(!valid) {
              removed++;
              return;
            }
          }
          operations.push(operation);
        } catch(e) {
          console.error('Error during validating operation', e);
          removed++;
        }
      }));
    }

    await Promise.all(promises);

    if(this.removed > 0) {
      console.warn('removed', removed);
    }

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
    this.opCount = -1;

    if(this.window.tail >= this.queue.length) {
      await this.collection.updateOne({
        'queue.ledgerNodeId': this.ledgerNodeId
      }, {
        $pullAll: {
          'queue.records': this.queue
        }
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

    if(!result.valid) {
      console.error('Validation error', result.error);
    }

    return {valid: !exists && result.valid};
  }
};
