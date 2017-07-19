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
api._election = require('./election');
api._hasher = require('./hasher');
api._server = require('./server');
api._voters = require('./voters');
api._storage = require('./storage');
api._worker = require('./worker');

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
// FIXME: `storage` should become `ledgerNode`
events.add = (event, storage, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }

  // TODO: refactor to run separate code paths based on `genesis` flag
  // rather than checking it each step of the way

  async.auto({
    voter: callback => {
      // FIXME: `options.ledgerNode` currently only available during
      // genesis ... will be fixed
      if(!options.genesis) {
        return callback();
      }
      api._voters.get(options.ledgerNode.id, callback);
    },
    hashEvent: callback => api._hasher(event, callback),
    writeEvent: ['hashEvent', (results, callback) => {
      const meta = {
        eventHash: results.hashEvent
      };
      if(options.genesis) {
        // TODO: run validators for genesis event
      }

      // TODO: sign event with voter?

      storage.events.add(event, meta, {}, callback);
    }],
    vote: ['voter', 'writeEvent', (results, callback) => {
      if(!options.genesis) {
        return callback();
      }
      // genesis vote
      api._election.voteForEvents(
        options.ledgerNode, results.voter, 0, [{id: results.voter.id}],
        err => {
        if(err) {
          return callback(err);
        }
        api._storage.votes.getLast(
          options.ledgerNode.id, 0, 'Events', results.voter.id, callback);
        });
    }],
    updateEvent: ['vote', 'writeEvent', (results, callback) => {
      if(!options.genesis) {
        return callback();
      }
      // mark event has having achieved consensus
      storage.events.update(results.hashEvent, [{
        op: 'set',
        changes: {
          meta: {
            consensus: true,
            consensusDate: Date.now()
          }
        }
      }], callback);
    }],
    writeBlock: ['updateEvent', (results, callback) => {
      if(!options.genesis) {
        return callback();
      }
      const configBlock = {
        '@context': 'https://w3id.org/webledger/v1',
        id: event.input[0].ledger + '/blocks/0',
        type: 'WebLedgerEventBlock',
        event: [event],
        electionResults: [results.vote],
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
