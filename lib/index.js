/*
 * Web Ledger Continuity2017 Consensus module.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const jsonld = bedrock.jsonld;
const niUri = require('ni-uri');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

// require submodules
api._worker = require('./worker');
api._server = require('./server');
api._hasher = require('./hasher');

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
    hashEvent: callback => api._hasher(event, callback),
    writeEvent: ['hashEvent', (results, callback) => {
      // FIXME: hash needs label prefix? (e.g. sha256:)
      const meta = {
        eventHash: results.hashEvent
      };
      if(options.genesis) {
        meta.consensus = true;
        meta.consensusDate = Date.now();

        // TODO: run validators for genesis event
      }

      // TODO: create `voter` and sign event

      storage.events.add(event, meta, {}, callback);
    }],
    writeBlock: ['writeEvent', (results, callback) => {
      if(!options.genesis) {
        return callback();
      }
      const configBlock = {
        '@context': 'https://w3id.org/webledger/v1',
        // FIXME: This should be generated based on the latest block number
        //id: event.input[0].ledger + '/blocks/0',
        id: 'did:v1:eb8c22dc-bde6-4315-92e2-59bd3f3c7d59/blocks/0',
        type: 'WebLedgerEventBlock',
        event: [event],
        blockHeight: 0
      };
      async.auto({
        hashBlock: callback => api._hasher(configBlock, callback),
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
