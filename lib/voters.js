/*
 * Voter functions for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const config = bedrock.config;
const ursa = require('ursa');
const uuid = require('uuid/v4');

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
    if(err && err.name === 'NotFound') {
      return _createVoter(ledgerNodeId, callback);
    }
    callback(err, voter);
  });
};

// TODO: need a method for voter signature verification whereby the public
// key for a voter will be retrieved from the blockchain and verified against
// its voter ID (and cached for future use?)

function _createVoter(ledgerNodeId, callback) {
  _generateRsaKey((err, kp) => {
    if(err) {
      return callback(err);
    }
    const voterId = config.server.baseUri +
      '/consensus/continuity2017/voters/' + uuid();
    const voter = {
      id: voterId,
      ledgerNodeId: ledgerNodeId,
      publicKey: {
        id: voterId + '#key',
        publicKeyPem: kp.publicKeyPem,
        privateKey: {
          privateKeyPem: kp.privateKeyPem
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

function _generateRsaKey(callback) {
  // generate keypair for ledger node
  let kp = {};
  try {
    const keypair = ursa.generatePrivateKey(
      config['ledger-consensus-continuity'].keyParameters.RSA.modulusBits);
    kp = {
      publicKeyPem: keypair.toPublicPem('utf8'),
      privateKeyPem: keypair.toPrivatePem('utf8')
    };
  } catch(e) {
    return callback(e);
  }
  callback(null, kp);
}
