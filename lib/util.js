/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');

const api = {};
module.exports = api;

api.hasher = brLedgerNode.consensus._hasher;
