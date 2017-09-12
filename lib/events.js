/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const election = require('./election');
const hasher = brLedgerNode.consensus._hasher;
const jsigs = require('jsonld-signatures')();
const validate = require('bedrock-validation').validate;
const voters = require('./voters');
const continuityStorage = require('./storage');

const api = {};
module.exports = api;

jsigs.use('jsonld', brDidClient.jsonld);

/**
 * Adds a new event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 * @param options the options to use.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (event, ledgerNode, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  const storage = ledgerNode.storage;
  _writeEvent(ledgerNode, event, options, (err, eventRecord) => {
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
        _createGenesisBlock(ledgerNode, eventRecord.event, callback);
      },
      writeBlock: ['createBlock', (results, callback) => {
        return _writeGenesisBlock(
          storage, results.createBlock, eventRecord.meta.eventHash, callback);
      }]
    }, err => err ? callback(err) : callback(null, eventRecord));
  });
};

function _writeEvent(ledgerNode, event, options, callback) {
  const storage = ledgerNode.storage;
  async.auto({
    validate: callback => validate('continuity.event', event, callback),
    config: ['validate', (results, callback) => {
      if(options.genesis) {
        return callback(null, event.ledgerConfiguration);
      }
      storage.events.getLatestConfig((err, result) => {
        if(err) {
          return callback(err);
        }
        callback(null, result.event.ledgerConfiguration);
      });
    }],
    eventValidator: ['config', (results, callback) => {
      const config = results.config;
      const eventValidator = config.eventValidator || [];
      const requireEventValidation = config.requireEventValidation || false;
      brLedgerNode.consensus._validateEvent(
        event, eventValidator, {requireEventValidation}, callback);
    }],
    voter: ['eventValidator', (results, callback) => {
      if((options.continuity2017 && options.continuity2017.peer) ||
        options.genesisBlock) {
        // can't use voter to sign when mirroring a genesis block or when
        // event is from a peer
        return callback();
      }
      voters.get(ledgerNode.id, callback);
    }],
    signedEvent: ['voter', (results, callback) => {
      if((options.continuity2017 && options.continuity2017.peer) ||
        options.genesisBlock) {
        // can't use voter to sign when mirroring a genesis block or when
        // event is from a peer
        return callback(null, event);
      }
      jsigs.sign(event, {
        algorithm: 'LinkedDataSignature2015',
        privateKeyPem: results.voter.publicKey.privateKey.privateKeyPem,
        creator: results.voter.publicKey.id
      }, callback);
    }],
    eventHash: ['signedEvent', (results, callback) =>
      hasher(results.signedEvent, callback)],
    writeEvent: ['eventHash', (results, callback) => {
      // TODO: may want to get voter and sign events (so long as `genesisBlock`
      //   was not given in the options, in which case we're mirroring)
      storage.events.add(
        results.signedEvent, {eventHash: results.eventHash}, callback);
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
        {ledgerNode, voter: results.voter, blockHeight: 0,
        electors: [{id: results.voter.id}], round: 1},
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
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        id: event.ledgerConfiguration.ledger + '/blocks/0',
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
