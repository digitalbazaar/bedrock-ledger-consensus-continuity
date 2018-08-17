/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const {config} = require('bedrock');
const path = require('path');

config.mocha.tests.push(path.join(__dirname, 'mocha'));

config.jsonld.strictSSL = false;

// MongoDB
config.mongodb.name = 'bedrock_ledger_continuity_test';
config.mongodb.dropCollections.onInit = true;
config.mongodb.dropCollections.collections = [];

// reduce processing interval for testing
config['ledger-consensus-continuity'].worker.election.gossipInterval = 0;

// decrease delay for gossiping with the same peer
config['ledger-consensus-continuity'].gossip.coolDownPeriod = 250;

config['ledger-consensus-continuity'].gossip.maxDepth = 6;

// disable caching in test
config['ledger-consensus-continuity'].gossip.cache.enabled = false;

// lower compression threshold so that compression is used in tests
config['ledger-consensus-continuity'].gossip.compression.threshold = 5;

// reduce debounce in the event-writer
config['ledger-consensus-continuity'].writer.debounce = 50;
