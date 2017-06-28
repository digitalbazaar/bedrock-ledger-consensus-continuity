/*
 * Web Ledger Continuity2017 Consensus module.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

// require submodules
api._gossip = require('./gossip');

// register this ledger plugin
bedrock.events.on('bedrock.start', callback => {
  brLedger.use({
    capabilityName: 'Continuity2017',
    capabilityValue: {
      type: 'consensus',
      api: api
    }
  }, callback);
});

///////////////////////////////// BLOCKS API /////////////////////////////////
const blocks = api.blocks = {};

// TODO: document
blocks.setConfig = (ledgerNode, configBlock, callback) => {
  async.auto({
    hashBlock: callback => hasher(configBlock, callback),
    writeConfig: ['hashBlock', (results, callback) => {
      // FIXME: hash needs label prefix? (e.g. `ni` something ... sha256:)
      const meta = {
        blockHash: results.hashBlock,
        consensus: Date.now()
      };
      const options = {};
      ledgerNode.storage.blocks.add(configBlock, meta, options, callback);
    }]
  }, callback);
};

///////////////////////////////// EVENTS API /////////////////////////////////
const events = api.events = {};

// TODO: document
events.add = (event, eventStorage, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  async.auto({
    hashBlock: callback => hasher(event, callback),
    writeEvent: ['hashBlock', (results, callback) => {
      // FIXME: hash needs label prefix? (e.g. sha256:)
      const meta = {
        eventHash: results.hashBlock
      };
      const options = {};
      eventStorage.add(event, meta, options, callback);
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

// FIXME: normalize data
function hasher(data, callback) {
  callback(
    null, crypto.createHash('sha256').update(JSON.stringify(data))
      .digest('hex'));
}
