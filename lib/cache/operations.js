/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config, util: {BedrockError}} = bedrock;
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
const _cacheKey = require('./cacheKey');
const OperationWriter = require('./OperationWriter');

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await database.openCollections(['bedrock-ledger-operation-queue']);
  await database.createIndexes([{
    collection: 'bedrock-ledger-operation-queue',
    fields: {'queue.ledgerNodeId': 1},
    options: {unique: true, background: false}

  }]);
});

// FIXME: Maps MUST have a limited size
const OPERATION_WRITER_MAP = new Map();

const operationsConfig = config['ledger-consensus-continuity'].operations;

exports._getOperationWriter = _getOperationWriter;

/**
 * Adds an operation to the cache.
 *
 * @param operation {Object} - The operation data.
 * @param meta {Object} - The operation meta data.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.add = async ({ledgerNodeId, operation, meta, forceFlush}) => {
  const operationWriter = await _getOperationWriter({ledgerNodeId});
  // const {basisBlockHeight, operationHash} = meta;
  const opListKey = _cacheKey.operationList(ledgerNodeId);

  const operationQueueSize = await cache.client.llen(opListKey);
  if(operationQueueSize >= operationsConfig.maxQueueSize) {
    throw new BedrockError(
      'The node is not accepting operations. Try again later.',
      'OperationError', {
        httpStatusCode: 503,
        public: true,
      });
  }
  return operationWriter.add({operation, meta, forceFlush});
};

/**
 * Check if the operation cache is empty or not. this only checks the persisted
 * queue in MongoDB
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Boolean>} True if the operation cache is empty, false if
 *   not.
 */
exports.isEmpty = async ({ledgerNodeId}) => {
  // FIXME: Check in-memory cache of operation queue before hitting DB, this is
  //       only checks the persisted queue in MongoDB
  const collection = database.collections['bedrock-ledger-operation-queue'];
  // FIXME: lru-memoize, usePromiseQueue: true
  const doc = await collection.findOne({
    'queue.ledgerNodeId': ledgerNodeId
  });
  if(!doc) {
    return true;
  }
  return doc.queue.records.length === 0;
};

async function _getOperationWriter({ledgerNodeId}) {
  let operationWriter = OPERATION_WRITER_MAP.get(ledgerNodeId);

  if(!operationWriter) {
    operationWriter = new OperationWriter({ledgerNodeId});
    await operationWriter.init();
    OPERATION_WRITER_MAP.set(ledgerNodeId, operationWriter);
  }

  operationWriter.start();
  return operationWriter;
}
