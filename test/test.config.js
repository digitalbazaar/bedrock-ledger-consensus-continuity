/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
const {config} = require('bedrock');
const path = require('path');

config.mocha.tests.push(path.join(__dirname, 'mocha'));
config.mocha.options.timeout = 60000;

// MongoDB
config.mongodb.name = 'bedrock_ledger_continuity_test';
config.mongodb.dropCollections.onInit = true;
config.mongodb.dropCollections.collections = [];

// decrease delay for gossiping with the same peer
config['ledger-consensus-continuity'].gossip.coolDownPeriod = 250;
// limit event validation worker processes to 1
config['ledger-consensus-continuity'].gossip.eventsValidation.workers = 1;
// update peers very frequently for tests so that new peers are onboarded
// and spread quickly for tests that typically only run for 30-60 seconds
config['ledger-consensus-continuity'].gossip.peerSamplingCache.maxAge = 1000;

config.mocha.options.bail = true;

config['https-agent'].rejectUnauthorized = false;
