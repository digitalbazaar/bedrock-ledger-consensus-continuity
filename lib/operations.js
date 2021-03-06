/*!
 * Copyright (c) 2018-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _localPeers = require('./localPeers');
const _util = require('./util');
const {util: {clone, BedrockError}} = require('bedrock');
const {getSchema, validateInstance} = require('bedrock-validation');

const api = {};
module.exports = api;

/**
 * Adds a new operation. Operations first pass through the LedgerNode API
 * where they are validated using the `operationValidator` defined in the
 * ledger configuration.
 *
 * @param operation the operation to add.
 * @param ledgerNode the node that is tracking this operation.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.add = async ({meta, operation, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const expectedCreator = await _localPeers.getPeerId({ledgerNodeId});
  const localOperationSchema = getSchema('continuity.localOperation');
  localOperationSchema.properties.creator.enum = [expectedCreator];
  const validationResult = validateInstance(operation, localOperationSchema);
  if(!validationResult.valid) {
    throw validationResult.error;
  }
  meta.recordId = _util.generateRecordId({ledgerNode, operation});
  const operationHash = await _util.hasher(operation);
  meta.operationHash = operationHash;
  // determine if the operation is already in queue to be added to an event
  // a redis watch will be put on the opHashKey which will be used to detect a
  // duplicate arriving while the database is checked
  let exists = await _cache.operations.exists({ledgerNodeId, operationHash});
  if(exists) {
    throw new BedrockError(
      'The operation already exists.',
      'DuplicateError', {
        duplicateLocation: 'cache',
        httpStatusCode: 409,
        ledgerNodeId,
        operation,
        operationHash,
        public: true
      });
  }
  // check the database for an existing operation
  exists = await ledgerNode.storage.operations.exists({operationHash});
  if(exists) {
    throw new BedrockError(
      'The operation already exists.',
      'DuplicateError', {
        duplicateLocation: 'db',
        httpStatusCode: 409,
        ledgerNodeId,
        operation,
        operationHash,
        public: true
      });
  }
  const result = await _cache.operations.add({meta, operation, ledgerNodeId});
  // null is returned if the `add` transaction was aborted due to the redis
  // watch which indicates that another worker adds the opHashKey to the cache
  // while this worker was checking the database for duplicates above
  if(result === null) {
    throw new BedrockError(
      'The operation already exists.',
      'DuplicateError', {
        duplicateLocation: 'cache',
        httpStatusCode: 409,
        ledgerNodeId,
        operation,
        operationHash,
        public: true
      });
  }
  return {operation, meta};
};

/**
 * Writes operations to storage.
 *
 * @param operations the operations to store.
 * @param eventHash the event hash for the event the operations are in.
 * @param ledgerNode the node that is tracking this operation.
 *
 * @return {Promise} resolves once the operation completes.
 */
api.write = async ({operations, eventHash, ledgerNode}) => {
  const records = [];
  for(let eventOrder = 0; eventOrder < operations.length; ++eventOrder) {
    const record = operations[eventOrder];
    const meta = {...clone(record.meta), eventHash, eventOrder};
    const {operation} = record;
    const {recordId} = meta;
    delete meta.recordId;
    // FIXME: `basisBlockHeight` should be on the `operation` itself, remove
    // this delete call
    // basisBlockHeight is recorded on the event
    delete meta.basisBlockHeight;
    records.push({meta, operation, recordId});
  }
  return ledgerNode.storage.operations.addMany({operations: records});
};
