/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _util = require('./util');
const bedrock = require('bedrock');
const {callbackify} = require('bedrock').util;

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
api.add = callbackify(async ({meta, operation, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  meta.recordId = _util.generateRecordId({ledgerNode, operation});
  meta.operationHash = await _util.hasher(operation);
  await _cache.operations.add({meta, operation, ledgerNodeId});
  return {operation, meta};
});

/**
 * Writes operations to storage.
 *
 * @param operations the operations to store.
 * @param eventHash the event hash for the event the operations are in.
 * @param ledgerNode the node that is tracking this operation.
 *
 * @return {Promise} resolves once the operation completes.
 */
api.write = callbackify(async ({operations, eventHash, ledgerNode}) => {
  const records = [];
  for(let eventOrder = 0; eventOrder < operations.length; ++eventOrder) {
    const record = operations[eventOrder];
    const meta = bedrock.util.clone(record.meta);
    Object.assign(meta, {eventHash, eventOrder});
    const {operation} = record;
    const {recordId} = meta;
    delete meta.recordId;
    // basisBlockHeight is recorded on the event
    delete meta.basisBlockHeight;
    records.push({meta, operation, recordId});
  }
  return ledgerNode.storage.operations.addMany({operations: records});
});
