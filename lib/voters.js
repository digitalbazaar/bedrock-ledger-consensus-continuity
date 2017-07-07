/*
 * Voter functions for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const config = bedrock.config;
const consensus = brLedger.consensus;
const ursa = require('ursa');

const storage = require('./storage');

require('./config');

// module API
const api = {};
module.exports = api;

/**
 * Get the voter population for the given ledger node ID and block ID.
 *
 * The voters will be passed to the given callback using the given
 * data structure:
 *
 * [{id: voter_id, url: voter_endpoint1}, ... ]
 *
 * @param ledgerNodeId the ID of the ledger node.
 * @param blockId the ID of the block.
 * @param callback(err, voters) called once the operation completes.
 */
api.getPopulation = (ledgerNodeId, blockId, callback) => {
  // TODO: implement
};

/**
 * Get the voter ID used for the given ledger node ID. If no such ID exists,
 * it will be created.
 *
 * Creating the ID on demand as part of this `get` method is done
 * intentionally, so as to avoid a costly key generation operation in a
 * `create` call that will fail every time after the first call but not
 * until after time was spent generating the key and checking the database
 * for a duplicate.
 *
 * @param ledgerNodeId the ID of the ledger node.
 * @param callback(err, continuityId) called once the operation completes.
 */
api.get = (ledgerNodeId, callback) => {
  storage.voters.get(ledgerNodeId, (err, voter) => {
    if(err && err.name === 'NotFound') {
      return _createVoter(ledgerNodeId, callback);
    }
    callback(err, voter);
  });
};

function _createVoter(ledgerNodeId, callback) {

  async.auto({
    createAgent: callback => _createAgent(callback),
    generateKey: callback => _generateRsaKey(callback),
    addVoter: ['createAgent', 'generateKey', (results, callback) => {
      storage.voters.add({
        ledgerNodeId, {
          id:
        }
      })


    }]

  })
}

function _createAgent(callback) {
  // Note: This may result in an agent being created that is never used if
  // there is a race to create one for a particular ledger node.

  // TODO: implement
  callback(new Error('Not implemented'));
}

function _generateRsaKey(callback) {
  // generate keypair for ledger node
  let kp = {};
  try {
    const keypair = ursa.generatePrivateKey(
      config['ledger-continuity'].keyParameters.RSA.modulusBits);
    kp = {
      publicKey: keypair.toPublicPem('utf8'),
      privateKey: keypair.toPrivatePem('utf8')
    };
  } catch(e) {
    return callback(e);
  }
  callback(null, kp);
}
