/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');

const api = {};
module.exports = api;

api.generateRecordId = ({ledgerNode, operation}) => {
  let recordId;
  if(operation.type === 'CreateWebLedgerRecord') {
    recordId = operation.record.id;
  }
  if(operation.type === 'UpdateWebLedgerRecord') {
    recordId = operation.recordPatch.target;
  }
  return ledgerNode.storage.driver.hash(recordId);
};

api.hasher = brLedgerNode.consensus._hasher;
