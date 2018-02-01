/*
 * Voter functions for Continuity2017 consensus method.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const async = require('async');
const bedrock = require('bedrock');
const brDidClient = require('bedrock-did-client');
const cache = require('bedrock-redis');
const config = bedrock.config;
const crypto = require('crypto');
const bs58 = require('bs58');
const ed25519 = require('ed25519');

const storage = require('./storage');

require('./config');

// module API
const api = {};
module.exports = api;

/**
 * Get the voter used for the given ledger node ID. If no such voter exists,
 * it will be created.
 *
 * Creating the voter on demand as part of this `get` method is done
 * intentionally, so as to avoid a costly key generation operation in a
 * `create` call that will fail every time after the first call but not
 * until after time was spent generating the key and checking the database
 * for a duplicate.
 *
 * @param ledgerNodeId the ID of the ledger node.
 * @param callback(err, voter) called once the operation completes.
 */
api.get = ({ledgerNodeId, privateKey = false, publicKey = false}, callback) => {
  const voterKey = _cacheKey.voter(ledgerNodeId);
  async.auto({
    cache: callback => {
      if(privateKey || publicKey) {
        // keys are not cached
        return callback();
      }
      cache.client.get(voterKey, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result === null) {
          // cache miss
          return callback();
        }
        callback(null, {id: result});
      });
    },
    voter: ['cache', (results, callback) => {
      if(results.cache) {
        // cache hit
        return callback(null, results.cache);
      }
      // simultaneous calls to this API can lead to multiple workers attempting
      // to create keys causing _createVoter to produce a DuplicateError
      function errorFilter(err) {
        return err.name === 'DuplicateError';
      }
      // does not introduce a delay since no interval is specified
      async.retry({times: 30, errorFilter}, callback => storage.voters.get(
        {ledgerNodeId, privateKey, publicKey}, (err, voter) => {
          if(err && err.name === 'NotFoundError') {
            return _createVoter(ledgerNodeId, callback);
          }
          callback(err, voter);
        }), callback);
    }],
    cacheInsert: ['voter', (results, callback) => {
      if(results.cache) {
        return callback();
      }
      const {id} = results.voter;
      cache.client.set(voterKey, id, callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, results.voter);
  });
};

api.getLedgerNodeId = (voterId, callback) => {
  const ledgerNodeKey = _cacheKey.ledgerNode(voterId);
  async.auto({
    cache: callback => {
      cache.client.get(ledgerNodeKey, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result === null) {
          // cache miss
          return callback();
        }
        callback(null, result);
      });
    },
    ledgerNodeId: ['cache', (results, callback) => {
      if(results.cache) {
        // cache hit
        return callback(null, results.cache);
      }
      storage.voters.get({voterId}, (err, result) => {
        if(err) {
          return callback(err);
        }
        return callback(null, result.ledgerNodeId);
      });
    }],
    cacheInsert: ['ledgerNodeId', (results, callback) => {
      if(results.cache) {
        return callback();
      }
      cache.client.set(ledgerNodeKey, results.ledgerNodeId, callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, results.ledgerNodeId);
  });
};

api.sign = ({doc, ledgerNodeId}, callback) => {
  async.auto({
    creator: callback => api.get({ledgerNodeId, privateKey: true}, callback),
    sign: ['creator', (results, callback) => jsigs.sign(doc, {
      algorithm: 'LinkedDataSignature2015',
      privateKeyPem: results.creator.publicKey.privateKey.privateKeyPem,
      creator: results.creator.publicKey.id
    }, callback)],
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, results.sign);
  });
};

// TODO: need a method for voter signature verification whereby the public
// key for a voter will be retrieved from the blockchain and verified against
// its voter ID (and cached for future use?)

function _createVoter(ledgerNodeId, callback) {
  _generateEd25519Key((err, kp) => {
    if(err) {
      return callback(err);
    }
    const voterId = config.server.baseUri +
      '/consensus/continuity2017/voters/' + kp.publicKeyBase58;
    const voter = {
      id: voterId,
      ledgerNodeId: ledgerNodeId,
      publicKey: {
        // same ID as voter (i.e. voter *is* a key)
        id: voterId,
        publicKeyBase58: kp.publicKeyBase58,
        privateKey: {
          privateKeyBase58: kp.privateKeyBase58
        }
      }
    };
    storage.voters.add(voter, (err, result) => {
      if(err) {
        return callback(err);
      }
      callback(null, result.voter);
    });
  });
}

function _generateEd25519Key(callback) {
  // generate keypair for ledger node
  let kp = {};
  try {
    const keypair = ed25519.MakeKeypair(crypto.randomBytes(32));
    kp = {
      publicKeyBase58: bs58.encode(keypair.publicKey),
      privateKeyBase58: bs58.encode(keypair.privateKey)
    };
  } catch(e) {
    return callback(e);
  }
  callback(null, kp);
}
