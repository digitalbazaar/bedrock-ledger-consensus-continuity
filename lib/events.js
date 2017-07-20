/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const election = require('./election');
const hasher = require('./hasher');
const voters = require('./voters');
const continuityStorage = require('./storage');

const api = {};
module.exports = api;

// TODO: document
// FIXME: `storage` should become `ledgerNode`
api.add = (event, storage, options, callback) => {
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
      voters.get(options.ledgerNode.id, callback);
    },
    hashEvent: callback => hasher(event, callback),
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
      election.voteForEvents(
        options.ledgerNode, results.voter, 0, [{id: results.voter.id}],
        err => {
          if(err) {
            return callback(err);
          }
          continuityStorage.votes.getLast(
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
        consensusMethod: 'Continuity2017',
        event: [event],
        electionResults: [results.vote],
        blockHeight: 0
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
