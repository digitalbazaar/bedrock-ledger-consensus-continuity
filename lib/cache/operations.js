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
  await database.openCollections(['continuity2017_operation_queue']);
  await database.createIndexes([{
    collection: 'continuity2017_operation_queue',
    fields: {ledgerNodeId: 1, 'meta.operationHash': 1, 'meta.created': 1},
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
  const collection = database.collections.continuity2017_operation_queue;
  // FIXME: lru-memoize, usePromiseQueue: true
  const projection = {_id: 1};
  const doc = await collection.findOne({ledgerNodeId}, {projection});
  return !doc;
};

async function _getOperationWriter({ledgerNodeId}) {
  let operationWriter = OPERATION_WRITER_MAP.get(ledgerNodeId);

  if(!operationWriter) {
    operationWriter = new OperationWriter({ledgerNodeId});
    OPERATION_WRITER_MAP.set(ledgerNodeId, operationWriter);
  }

  operationWriter.start();
  return operationWriter;
}
