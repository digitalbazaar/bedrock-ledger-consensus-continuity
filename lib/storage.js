/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const database = require('bedrock-mongodb');
const consensus = brLedger.consensus;

require('./config');

// module API
const api = {};
module.exports = api;

api.manifests = require('./manifestStorage');
api.votes = require('./voteStorage');
api.voters = require('./voterStorage');
