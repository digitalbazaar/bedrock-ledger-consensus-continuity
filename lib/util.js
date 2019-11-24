/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
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

// TODO: merge events could be directly mapped to expanded form here as an
// optimization... could even directly map it to RDF
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
