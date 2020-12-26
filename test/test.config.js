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

config.mocha.options.bail = true;

config['https-agent'].rejectUnauthorized = false;

// put jobs stuff in another redis database so db 0 can be flushed
config.jobs.queueOptions.db = 1;
