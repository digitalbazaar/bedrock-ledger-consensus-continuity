/*
 * Voter functions for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const config = bedrock.config;
const ursa = require('ursa');

const storage = require('./storage');

require('./config');

// module API
const api = {};
module.exports = api;

/**
 * Get the voter population for the given ledger node and block ID.
 *
 * The voters will be passed to the given callback using the given
 * data structure:
 *
 * [{id: voter_id, url: voter_endpoint1}, ... ]
 *
 * @param ledgerNode the ledger node API to use.
 * @param blockId the ID of the block.
 * @param callback(err, voters) called once the operation completes.
 */
api.getPopulation = (ledgerNode, blockId, callback) => {
  // TODO: need to process previous blocks to intelligently build up a
  // voting population; need to also include contingencies from the
  // most recent config which may add certain voters to the population based
  // on the current conditions of the ledger (e.g. if fewer than X events
  // ever recorded or previous voting stats -- the latter of which is hard
  // to do without including that information in the blocks and hashing it,
  // which is presently an unsolved problem).

  // TODO: implement
  callback(new Error('Not implemented'));
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
    // TODO: produce voter ID from public key fingerprint
    const voter = {
      id: '',
      ledgerNodeId: ledgerNodeId,
      publicKeyPem: kp.publicKeyPem,
      privateKeyPem: kp.privateKeyPem
    };
    storage.voters.add(voter, callback);
  });
}

function _generateRsaKey(callback) {
  // generate keypair for ledger node
  let kp = {};
  try {
    const keypair = ursa.generatePrivateKey(
      config['ledger-continuity'].keyParameters.RSA.modulusBits);
    kp = {
      publicKeyPem: keypair.toPublicPem('utf8'),
      privateKeyPem: keypair.toPrivatePem('utf8')
    };
  } catch(e) {
    return callback(e);
  }
  callback(null, kp);
}
