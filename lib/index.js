/*
 * Web Ledger Continuity2017 Consensus module.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const crypto = require('crypto');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

// require submodules
api._worker = require('./worker');
api._server = require('./server');

// register this ledger plugin
bedrock.events.on('bedrock.start', () => {
  brLedger.use('Continuity2017', {
    type: 'consensus',
    api: api
  });
});

///////////////////////////////// EVENTS API /////////////////////////////////
const events = api.events = {};

// TODO: document
events.add = (event, storage, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  async.auto({
    hashEvent: callback => hasher(event, callback),
    writeEvent: ['hashEvent', (results, callback) => {
      // FIXME: hash needs label prefix? (e.g. sha256:)
      const meta = {
        eventHash: results.hashEvent
      };
      if(options.genesis) {
        meta.consensus = true;
        meta.consensusDate = Date.now();
      }
      storage.events.add(event, meta, {}, callback);
    }],
    writeBlock: ['writeEvent', (results, callback) => {
      if(!options.genesis) {
        return callback();
      }
      const configBlock = {
        '@context': 'https://w3id.org/webledger/v1',
        // FIXME: This should be generated based on the latest block number
        //id: event.input[0].ledger + '/blocks/1',
        id: 'did:v1:eb8c22dc-bde6-4315-92e2-59bd3f3c7d59/blocks/1',
        type: 'WebLedgerEventBlock',
        event: [event]
      };
      async.auto({
        hashBlock: callback => hasher(configBlock, callback),
        writeBlock: ['hashBlock', (results, callback) => {
          const meta = {
            blockHash: results.hashBlock,
            consensus: true,
            consensusDate: Date.now()
          };
          storage.blocks.add(configBlock, meta, {}, callback);
        }]
      }, callback);
    }]
  }, (err, results) => callback(err, results.writeEvent));
};

// FIXME: normalize data
function hasher(data, callback) {
  callback(
    null, crypto.createHash('sha256').update(JSON.stringify(data))
      .digest('hex'));
}
