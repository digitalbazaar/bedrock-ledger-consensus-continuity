/*
 * Web Ledger Continuity2017 Consensus module.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

// require submodules
api.events = require('./events');
api._election = require('./election');
api._hasher = require('./hasher');
api._server = require('./server');
api._voters = require('./voters');
api._storage = require('./storage');
api._worker = require('./worker');

api.scheduleWork = api._worker.scheduleWork;

// register this ledger plugin
bedrock.events.on('bedrock.start', () => {
  brLedger.use('Continuity2017', {
    type: 'consensus',
    api: api
  });
});
