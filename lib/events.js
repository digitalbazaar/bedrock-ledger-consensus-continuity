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
api.add = (event, ledgerNode, options, callback) => {
  if(!ledgerNode.storage) {
    throw new Error('Matt!!!');
  }
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  const storage = ledgerNode.storage;
  _writeEvent(storage, event, (err, eventRecord) => {
    if(err) {
      return callback(err);
    }
    if(!options.genesis) {
      // no need to create genesis block, return early
      return callback(null, eventRecord);
    }

    // need to write the genesis block, either from `options.genesisBlock`
    // to mirror an existing ledger, or create it ourselves for a new ledger
    async.auto({
      createBlock: callback => {
        if(options.genesisBlock) {
          // use given genesis block, mirroring an existing ledger
          return callback(null, options.genesisBlock);
        }
        // create genesis block, creating a new ledger
        _createGenesisBlock(ledgerNode, event, callback);
      },
      writeBlock: ['createBlock', (results, callback) => {
        return _writeGenesisBlock(
          storage, results.createBlock, eventRecord.meta.eventHash, callback);
      }]
    }, err => err ? callback(err) : callback(null, eventRecord));
  });
};

function _writeEvent(storage, event, callback) {
  async.auto({
    eventHash: callback => hasher(event, callback),
    // TODO: validate event
    validate: callback => callback(),
    writeEvent: ['eventHash', 'validate', (results, callback) => {
      const meta = {
        eventHash: results.eventHash
      };
      // TODO: may want to get voter and sign events (so long as `genesisBlock`
      //   was not given in the options, in which case we're mirroring)
      storage.events.add(event, meta, {}, callback);
    }]
  }, (err, results) => err ?
    callback(err) : callback(null, results.writeEvent));
}

function _createGenesisBlock(ledgerNode, event, callback) {
  async.auto({
    voter: callback => {
      voters.get(ledgerNode.id, callback);
    },
    vote: ['voter', (results, callback) => {
      // genesis vote
      election.voteForEvents(
        ledgerNode, results.voter, 0, [{id: results.voter.id}],
        err => {
          if(err) {
            return callback(err);
          }
          continuityStorage.votes.getLast(
            ledgerNode.id, 0, 'Events', results.voter.id, callback);
        });
    }],
    block: ['vote', (results, callback) => {
      const block = {
        '@context': 'https://w3id.org/webledger/v1',
        id: event.input[0].ledger + '/blocks/0',
        type: 'WebLedgerEventBlock',
        consensusMethod: 'Continuity2017',
        event: [event],
        electionResults: [results.vote],
        blockHeight: 0
      };
      callback(null, block);
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.block));
}

function _writeGenesisBlock(storage, block, eventHash, callback) {
  const now = Date.now();
  async.auto({
    hashBlock: callback => hasher(block, callback),
    writeBlock: ['hashBlock', (results, callback) => {
      const meta = {
        blockHash: results.hashBlock,
        consensus: true,
        consensusDate: now
      };
      storage.blocks.add(block, meta, {}, callback);
    }],
    updateEvent: ['writeBlock', (results, callback) => {
      // mark event has having achieved consensus
      storage.events.update(eventHash, [{
        op: 'set',
        changes: {
          meta: {
            consensus: true,
            consensusDate: now
          }
        }
      }], callback);
    }]
  }, callback);
}
