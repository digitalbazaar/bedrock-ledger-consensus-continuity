/*
 * Voter functions for Continuity2017 consensus method.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
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
api.get = (ledgerNodeId, callback) => {
  storage.voters.get({ledgerNodeId}, (err, voter) => {
    if(err && err.name === 'NotFoundError') {
      return _createVoter(ledgerNodeId, (err, result) => {
        if(err && err.name === 'DuplicateError') {
          return api.get(ledgerNodeId, callback);
        }
        callback(err, result);
      });
    }
    callback(err, voter);
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
