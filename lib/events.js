/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const database = require('bedrock-mongodb');
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

    const mergeEvent = {
      '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
      type: ['WebLedgerEvent', 'ContinuityMergeEvent'],
      parentHash: [eventRecord.meta.eventHash],
    };

    // need to write the genesis block, either from `options.genesisBlock`
    // to mirror an existing ledger, or create it ourselves for a new ledger
    async.auto({
      // add indexes specific to Continuity
      createIndexes: callback => {
        const collection = storage.events.collection.s.name;
        database.createIndexes([{
          collection,
          fields: {'meta.continuity2017.creator': 1},
          options: {sparse: true, background: false}
        }, {
          collection,
          fields: {'event.parentHash': 1},
          options: {sparse: true, background: false}
        }], callback);
      },
      creator: callback => voters.get(ledgerNode.id, callback),
      signedMergeEvent: ['creator', (results, callback) =>
        jsigs.sign(mergeEvent, {
          algorithm: 'LinkedDataSignature2015',
          privateKeyPem: results.creator.publicKey.privateKey.privateKeyPem,
          creator: results.creator.publicKey.id
        }, callback)],
      eventHash: ['signedMergeEvent', (results, callback) =>
        hasher(results.signedMergeEvent, callback)],
      mergeEventRecord: ['eventHash', (results, callback) => {
        const {creator, eventHash, signedMergeEvent} = results;
        const meta = {eventHash, continuity2017: {creator: creator.id}};
        storage.events.add(signedMergeEvent, meta, callback);
      }],
      createBlock: ['mergeEventRecord', (results, callback) => {
        if(options.genesisBlock) {
          // use given genesis block, mirroring an existing ledger
          return callback(null, options.genesisBlock);
        }
        // create genesis block, creating a new ledger
        const block = {
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          id: eventRecord.event.ledgerConfiguration.ledger + '/blocks/0',
          type: 'WebLedgerEventBlock',
          consensusMethod: 'Continuity2017',
          event: [eventRecord.meta.eventHash],
          consensusProof: [results.mergeEventRecord.meta.eventHash],
          blockHeight: 0,
          publicKey: [{
            id: results.creator.publicKey.id,
            type: 'CryptographicKey',
            owner: results.creator.id,
            publicKeyPem: results.creator.publicKey.publicKeyPem
          }]
        };
        callback(null, block);
      }],
      writeBlock: ['createBlock', (results, callback) => _writeGenesisBlock(
        storage, results.createBlock, callback)],
      // mark event has having achieved consensus
      updateEvent: ['writeBlock', (results, callback) => storage.events.update(
        eventRecord.meta.eventHash, [{
          op: 'set',
          changes: {
            meta: {
              consensus: true,
              consensusDate: Date.now()
            }
          }
        }], callback)],
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

// called from consensus worker
api.mergeBranches = (ledgerNode, callback) => {
  const storage = ledgerNode.storage;
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: ['WebLedgerEvent', 'ContinuityMergeEvent'],
    parentHash: [],
  };
  async.auto({
    // FIXME: maybe creator will be supplied as a parameter?
    creator: callback => voters.get(ledgerNode.id, callback),
    treeHash: callback => api._getLocalBranchHead(
      storage.events.collection, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return callback(new BedrockError(
            'Could not locate branch head.',
            'InvalidStateError', {
              httpStatusCode: 400,
              public: true,
            }));
        }
        // add the hash of the local merge event as the treeHash of this event
        event.treeHash = result;
        callback();
      }),
    events: ['creator', 'treeHash', (results, callback) => {
      const query = {
        $or: [{
          'event.type': {$ne: 'ContinuityMergeEvent'},
          'meta.continuity2017.creator': results.creator.id,
          'meta.consensus': {$exists: false}
        }, {
          'event.type': 'ContinuityMergeEvent',
          'meta.continuity2017.creator': {$ne: results.creator.id},
          // meta.consensus intentionally omitted
        }]
      };
      const projection = {
        'eventHash': 1,
        _id: 0
      };
      // FIXME: what should happen if no results are found here?
      storage.events.collection.find(query, projection)
        .forEach(doc => event.parentHash.push(doc.eventHash), callback);
    }],
    sign: ['events', (results, callback) => jsigs.sign(event, {
      algorithm: 'LinkedDataSignature2015',
      privateKeyPem: results.creator.publicKey.privateKey.privateKeyPem,
      creator: results.creator.publicKey.id
    }, callback)],
    eventHash: ['sign', (results, callback) => hasher(results.sign, callback)],
    write: ['eventHash', (results, callback) => {
      const {eventHash, creator} = results;
      const meta = {continuity2017: {creator: creator.id}, eventHash};
      storage.events.add(results.sign, meta, callback);
    }]
  }, (err, results) => callback(err, results.write));
};

// exposed for testing
api._getLocalBranchHead = (eventsCollection, options, callback) => {
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
};

function _parentExists(eventsCollection, eventHash, callback) {
  const query = {
    'meta.deleted': {$exists: false},
    'event.parentHash': eventHash
  };
  eventsCollection.find(query).count((err, result) =>
    callback(err, result > 0));
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
      api._getLocalBranchHead(storage.events.collection, (err, result) => {
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
    // add creator to local events
    localCreator: ['validate', (results, callback) => {
      if(fromPeer) {
        return callback();
      }
      voters.get(ledgerNode.id, (err, result) => {
        if(err) {
          return callback(err);
        }
        meta.continuity2017 = {
          creator: result.id
        };
        callback(null, result);
      });
    }],
    signature: ['validate', (results, callback) => {
      // only check signatures on ContinuityMergeEvent
      // ContinuityMergeEvents are guaranteed to have signatures here
      if(!(fromPeer && eventType.includes('ContinuityMergeEvent'))) {
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
    eventHash: ['treeHash', (results, callback) => hasher(event, callback)],
    hasMerge: ['eventHash', 'validate', (results, callback) => {
      if(!fromPeer) {
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
      // if it's a regular WebLedgerEvent ensure that there is a corresponding
      // merge event
      if(eventType.length === 1 && eventType[0] === 'WebLedgerEvent') {
        return storage.events.exists(event.treeHash, (err, result) => {
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
      }
      if(eventType.includes('ContinuityMergeEvent')) {
        return storage.events.exists(event.treeHash, (err, result) => {
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
      }
      callback(new BedrockError(
        'Invalid event type.', 'DataError',
        {httpStatusCode: 400, public: true, event}));
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
    writeEvent: ['eventValidator', 'hasMerge', 'localCreator',
      (results, callback) => {
        meta.eventHash = results.eventHash;
        storage.events.add(event, meta, callback);
      }]
  }, (err, results) => err ?
    callback(err) : callback(null, results.writeEvent));
}

function _writeGenesisBlock(storage, block, callback) {
  async.auto({
    hashBlock: callback => hasher(block, callback),
    writeBlock: ['hashBlock', (results, callback) => {
      const meta = {
        blockHash: results.hashBlock,
        consensus: true,
        consensusDate: Date.now()
      };
      storage.blocks.add(block, meta, callback);
    }]
  }, callback);
}
