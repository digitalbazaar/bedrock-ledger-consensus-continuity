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
const BedrockError = bedrock.util.BedrockError;

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
      }],
      updateKey: ['writeBlock', (results, callback) => {
        const block = results.createBlock;
        const key = Object.assign({}, block.publicKey[0]);
        key['@context'] = config.constants.WEB_LEDGER_CONTEXT_V1_URL;
        key.seeAlso = block.id;
        continuityStorage.keys.addPublicKey(ledgerNode.id, key, err => {
          if(err && err.name === 'DuplicateError') {
            // attempt to update key instead
            return continuityStorage.keys.updatePublicKey(
              ledgerNode.id, key, callback);
          }
          callback(err);
        });
      }]
    }, err => err ? callback(err) : callback(null, eventRecord));
  });
};

api.addLocalMergeEvent = () => {

};

// FIXME: there should also be an index on `meta.created
function _getLocalBranchHead(eventsCollection, options, callback) {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }

  // find the latest merge event
  const query = {
    'event.type': 'ContinuityMergeEvent',
    'meta.deleted': {$exists: false},
  };
  const projection = {
    'meta.eventHash': 1,
    _id: 0
  };
  eventsCollection.find(query, projection).sort({'meta.created': -1}).limit(1)
    .toArray((err, result) => {
      if(err) {
        return callback(err);
      }
      const eventHash = result.length === 1 ? result[0].meta.eventHash : null;
      callback(null, eventHash);
    });
}

function _writeEvent(ledgerNode, event, options, callback) {
  const storage = ledgerNode.storage;
  const eventType = [].concat(event.type);
  // if the event came from a peer through the HTTP API, it's already
  // been validated
  const fromPeer = (options.continuity2017 && options.continuity2017.peer);
  const meta = {};
  async.auto({
    validate: callback => {
      if(fromPeer) {
        return callback();
      }
      validate('continuity.webLedgerEvents', event, callback);
    },
    treeHash: ['validate', (results, callback) => {
      if(fromPeer || options.genesis) {
        return callback();
      }
      _getLocalBranchHead(storage.events.collection, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return callback(new BedrockError(
            'No local merge event was found.',
            'InvalidStateError', {
              httpStatusCode: 400,
              public: true,
              event,
            }));
        }
        // add the hash of the local merge event as the treeHash of this event
        event.treeHash = result;
        callback();
      });
    }],
    signature: ['validate', (results, callback) => {
      // only check signatures on ContinuityMergeEvent
      // ContinuityMergeEvents are guaranteed to have signatures here
      if(!eventType.includes('ContinuityMergeEvent')) {
        return callback();
      }
      jsigs.verify(event, {
        checkKeyOwner: (owner, key, options, callback) => {
          // capture the key owner id
          meta.continuity2017 = {
            creator: owner.id
          };
          callback(null, true);
        }
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result.verified) {
          return callback(new BedrockError(
            'The signature on the event could not verified.', 'DataError', {
              httpStatusCode: 400,
              public: true,
              event,
              keyResults: result.keyResults
            }));
        }
        // success
        callback();
      });
    }],
    eventHash: callback => hasher(event, callback),
    hasMerge: ['eventHash', 'validate', (results, callback) => {
      if(!fromPeer) {
        return callback();
      }
      // nothing to do if it's not a WebLedgerEvent
      if(!(eventType.length === 1 && eventType[0] === 'WebLedgerEvent')) {
        return callback();
      }
      // if it's from a peer, it must have a treeHash
      if(!event.treeHash) {
        return callback(new BedrockError(
          'The event must include a `treeHash` property.', 'DataError', {
            httpStatusCode: 400,
            public: true,
            event,
          }));
      }
      const query = {'event.parentHash': results.eventHash};
      storage.events.existsQuery(query, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return callback(new BedrockError(
            'This event has no corresponding merge event.',
            'InvalidStateError', {
              httpStatusCode: 400,
              public: true,
              event,
            }));
        }
        // success
        callback();
      });
    }],
    config: ['signature', (results, callback) => {
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
    writeEvent: ['eventValidator', 'hasMerge', (results, callback) => {
      // TODO: may want to get voter and sign events (so long as `genesisBlock`
      //   was not given in the options, in which case we're mirroring)
      meta.eventHash = results.eventHash;
      storage.events.add(event, meta, callback);
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
    key: ['vote', (results, callback) => {
      const keyId = results.vote.signature.creator;
      continuityStorage.keys.getPublicKey(ledgerNode.id, {id: keyId}, callback);
    }],
    block: ['key', (results, callback) => {
      const block = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        id: event.ledgerConfiguration.ledger + '/blocks/0',
        type: 'WebLedgerEventBlock',
        consensusMethod: 'Continuity2017',
        event: [event],
        electionResult: [results.vote],
        blockHeight: 0,
        publicKey: [{
          id: results.key.id,
          type: results.key.type,
          owner: results.key.owner,
          publicKeyPem: results.key.publicKeyPem
        }]
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
      if(typeof block.event[0] !== 'string') {
        block = bedrock.util.clone(block);
        block.event[0] = eventHash;
      }
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
