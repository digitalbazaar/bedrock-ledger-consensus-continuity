/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');

// module API
const api = {};
module.exports = api;

api.consensusMethod = 'Continuity9000';

// expose external APIs
api.config = {change: async () => {}};
api.events = {add: async () => {}};
api.operations = {add: async () => {}};
api.scheduleWork = async () => {};

// register this ledger plugin
bedrock.events.on('bedrock.start', () => {
  brLedgerNode.use('Continuity9000', {
    type: 'consensus',
    api: api
  });
});
