/*
 * Client for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const config = bedrock.config;

require('./config');

// module API
const api = {};
module.exports = api;

// define request pool for all gossip requests
const requestPool = {};
Object.defineProperty(requestPool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-continuity'].gossip.requestPool.maxSockets
});

// TODO: add method for getting block status

// TODO: add method for sending gossip given block status

// TODO: add method for getting gossip given block status

// TODO: add method for getting manifests

// TODO: add method for getting events
