/*
 * Voter functions for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const config = bedrock.config;
const forge = require('node-forge');
const ni = require('ni-uri');
const ursa = require('ursa');

const storage = require('./storage');

require('./config');

// module API
const api = {};
module.exports = api;

/**
 * Get the voter population for the given ledger node and block height.
 *
 * The voters will be passed to the given callback using the given
 * data structure:
 *
 * [{id: voter_id, sameAs: previous_voter_id}, ... ]
 *
 * @param ledgerNode the ledger node API to use.
 * @param blockHeight the height of the block.
 * @param callback(err, voters) called once the operation completes.
 */
api.getBlockElectors = (ledgerNode, blockHeight, callback) => {
  // FIXME: use real electors
  // TODO: need to process previous blocks to intelligently build up a
  // voting population; need to also include contingencies from the
  // most recent config which may add certain voters to the population based
  // on the current conditions of the ledger (e.g. if fewer than X events
  // ever recorded or previous voting stats -- the latter of which is hard
  // to do without including that information in the blocks and hashing it,
  // which is presently an unsolved problem).
  api.get(ledgerNode.id, (err, voter) => {
    if(err) {
      return callback(err);
    }
    callback(null, [].concat(voter));
  });

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
      console.log('VOTER NOT FOUND');
      return _createVoter(ledgerNodeId, callback);
    }
    // FIXME: check return `voter`, this should likely return voter.id
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
    const publicKey = forge.pki.publicKeyFromPem(kp.publicKeyPem);
    const fingerprint = forge.util.encode64(forge.pki.getPublicKeyFingerprint(
      publicKey, {md: forge.md.sha256.create(), encoding: 'binary'}))
      .replace(/\+/g, '-')
      .replace(/\//g, '_')
      .replace(/=+$/, '');
    // TODO: use full URL here for `id`
    const voter = {
      id: ni.format({
        algorithm: 'sha-256',
        value: fingerprint
      }),
      ledgerNodeId: ledgerNodeId,
      publicKeyPem: kp.publicKeyPem,
      privateKeyPem: kp.privateKeyPem
    };
    storage.voters.add(voter, (err, result) => {
      if(err) {
        return callback(err);
      }
      callback(null, result.voter.id);
    });
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
