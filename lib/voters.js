/*!
 * Voter functions for Continuity2017 consensus method.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const async = require('async');
const bedrock = require('bedrock');
const brDidClient = require('bedrock-did-client');
const bs58 = require('bs58');
const cache = require('bedrock-redis');
const config = bedrock.config;
const chloride = require('chloride');
const jsigs = require('jsonld-signatures')();
const multicodec = require('multicodec');
const multibase = require('multibase');
const BedrockError = bedrock.util.BedrockError;

jsigs.use('jsonld', brDidClient.jsonld);

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
        return ['DuplicateError', 'LockError'].includes(err.name);
      }
      async.retry(
        {interval: 50, times: 100, errorFilter}, callback => storage.voters.get(
          {ledgerNodeId, privateKey, publicKey}, (err, voter) => {
            if(err && err.name === 'NotFoundError') {
              const lockKey = `keygenerationlock|${ledgerNodeId}`;
              return async.auto({
                lock: callback => cache.client.setnx(
                  lockKey, '', (err, result) => {
                    if(err) {
                      return callback(err);
                    }
                    if(result === 0) {
                      return callback(new BedrockError(
                        'Key generation in process. Try again.', 'LockError'));
                    }
                    // lock was sucssful
                    callback();
                  }),
                create: ['lock', (results, callback) =>
                  _createVoter(ledgerNodeId, callback)],
                unlock: ['create', (results, callback) =>
                  cache.client.del(lockKey, callback)]
              }, (err, results) => {
                if(err) {
                  return callback(err);
                }
                callback(null, results.create);
              });
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
      algorithm: 'Ed25519Signature2018',
      privateKeyBase58: results.creator.publicKey.privateKey.privateKeyBase58,
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
    // use multicodec to encode the voter ID
    const publicKeyBytes = bs58.decode(kp.publicKeyBase58);
    const mcPubkey = multicodec.addPrefix('ed25519-pub', publicKeyBytes);
    const mbPubkey = multibase.encode('base58btc', mcPubkey).toString();
    const voterId = config.server.baseUri +
      '/consensus/continuity2017/voters/' + mbPubkey;
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
    const keypair = chloride.crypto_sign_keypair();
    kp = {
      publicKeyBase58: bs58.encode(keypair.publicKey),
      privateKeyBase58: bs58.encode(keypair.secretKey)
    };
  } catch(e) {
    return callback(e);
  }
  callback(null, kp);
}
