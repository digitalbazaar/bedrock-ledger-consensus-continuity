/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const _stats = require('./stats');
const database = require('bedrock-mongodb');
const LRU = require('lru-cache');
const OperationWriter = require('./cache/OperationWriter');

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await database.openCollections(['continuity2017_operation_queue']);
  await database.createIndexes([{
    collection: 'continuity2017_operation_queue',
    fields: {ledgerNodeId: 1, 'meta.operationHash': 1},
    options: {unique: true, background: false}
  }, {
    collection: 'continuity2017_operation_queue',
    fields: {ledgerNodeId: 1, 'meta.created': 1},
    options: {unique: false, background: false}
  }]);
});

// This dispose function ensures that the operation writer being evicted flushes
// its operations to disk.
const dispose = async (_, operationWriter) => {
  await operationWriter.flush();
};
const OPERATION_WRITER_MAP = new LRU({max: 100, dispose});

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
  _stats.incrementOpsCounter({ledgerNodeId, type: 'local'});
  return operationWriter.add({operation, meta, forceFlush});
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
