/*!
 * Copyright (c) 2018-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _localPeers = require('./localPeers');
const _operationWriter = require('./operationWriter');
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
api.add = async ({meta, operation, options = {}, ledgerNode}) => {
  const {forceFlush} = options;
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
  // check the database for an existing operation
  const exists = await ledgerNode.storage.operations.exists({operationHash});
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
  await _operationWriter.add({meta, operation, ledgerNodeId, forceFlush});
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
    // basisBlockHeight is recorded on the event
    // FIXME: investigate what is going on with `basisBlockHeight` here
    delete meta.basisBlockHeight;
    records.push({meta, operation, recordId});
  }
  return ledgerNode.storage.operations.addMany({operations: records});
};
