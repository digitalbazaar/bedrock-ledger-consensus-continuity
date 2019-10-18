/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');

exports.generateRecordId = ({ledgerNode, operation}) => {
  let recordId;
  if(operation.type === 'CreateWebLedgerRecord') {
    recordId = operation.record.id;
  }
  if(operation.type === 'UpdateWebLedgerRecord') {
    recordId = operation.recordPatch.target;
  }
  return ledgerNode.storage.driver.hash(recordId);
};

exports.hasher = brLedgerNode.consensus._hasher;
exports.rdfCanonizeAndHash = brLedgerNode.consensus._rdfCanonizeAndHash;

/**
 * Lexicographically sorts an array of operation records by
 * `meta.operationHash`. The given array of operations is mutated.
 *
 * @param operations the array of operations to sort by operation hash.
 */
exports.sortOperations = operations => {
  operations.sort((a, b) => a.meta.operationHash.localeCompare(
    b.meta.operationHash));
};

exports.hasValue = (obj, key, value) => [].concat(obj[key]).includes(value);
