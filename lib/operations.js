/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const config = bedrock.config;
const events = require('./events');

const api = {};
module.exports = api;

/**
 * Adds a new operation.
 *
 * @param operation the operation to add.
 * @param ledgerNode the node that is tracking this operation.
 * @param options the options to use.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (operation, ledgerNode, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  // TODO: add an operation queue so N operations can be bundled every
  //   period of time T into a single event to reduce overhead
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerEvent',
    operation: [operation]
  };
  events.add(event, ledgerNode, options, err => callback(err));
};
