/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brDidClient = require('bedrock-did-client');
const jsigs = require('jsonld-signatures')();
const logger = require('./logger');
const storage = require('./storage');
const BedrockError = bedrock.util.BedrockError;
const url = require('url');

const api = {};
module.exports = api;

jsigs.use('jsonld', brDidClient.jsonld);

// TODO: cache keys for verification in redis, might leverage caching already
// happening in `voters` API.

// verify the signature on a document utilizing the publicKey cache
api.verify = ({doc, ledgerNodeId}, callback) => {
  // TODO: public key cache totally unnecessary, retrieving keys from blockchain
  // nodes is not a requirement (and actually is problematic), it isn't a
  // security issue -- they can be generated from the key ID directly with no
  // need to dereference from the node itself
  let cacheKey;
  let keyOwner;
  let newKey;
  async.auto({
    verify: callback => jsigs.verify(doc, {
      getPublicKey: _getPublicKey,
      getPublicKeyOwner: _getPublicKeyOwner,
      checkTimestamp: false
    }, (err, result) => callback(err, result)),
    storeKey: ['verify', (results, callback) => {
      // no new key to store
      if(!newKey) {
        return callback();
      }

      // FIXME: owner and key ID in the new key are the same, and `type` is
      // set to include Identity and Continuity2017Peer, manually setting type
      // if this is not done, nodes creating blocks are putting their own key
      // in blocks with the proper type, and nodes that have cached the key
      // via this API are putting the key in with the wrong type information.
      newKey.type = 'Ed25519VerificationKey2018';
      // TODO: use very specific, limited shape/profile of `newKey` that
      // every valid continuity peer will also use

      // store new key, ignore all errors
      storage.keys.addPublicKey(ledgerNodeId, newKey, () => callback());
    }],
    finalize: ['storeKey', (results, callback) => {
      if(!results.verify.verified) {
        const {verified} = results.verify;
        const keyResultError = _.get(
          results, 'verify.keyResults.error', 'none');
        logger.debug(
          'Signature Verification Failure', {verified, keyResultError});
        return callback(new BedrockError(
          'Document signature verification failed.',
          'AuthenticationError', {
            doc,
            // FIXME: enable when bedrock.logger can properly log `error`
            // keyResults: results.verify.keyResults
          }));
      }

      // confirm that the last path element of the voter ID matches the key's
      // fingerprint
      const key = cacheKey || newKey;
      if(key.publicKeyBase58) {
        // get last path element of owner ID
        const parsedKeyOwner = url.parse(keyOwner.id);
        const last = parsedKeyOwner.pathname.split('/').pop();
        if(key.publicKeyBase58 !== last) {
          return callback(new BedrockError(
            'Document signer key fingerprint does not match their ID.',
            'AuthenticationError', {doc}));
        }
      }
      callback();
    }]
  }, err => {
    if(err) {
      return callback(err);
    }
    callback(null, {keyOwner});
  });

  function _getPublicKey(publicKeyId, options, callback) {
    // TODO: no need to do local storage of public key, just generate it
    // on the fly from the ID -- not a security issue for blockchain nodes
    async.auto({
      localStorage: callback => storage.keys.getPublicKey(
        {ledgerNodeId, publicKeyId}, (err, result) => {
          if(err && err.name === 'NotFoundError') {
            // cache miss
            return callback();
          }
          cacheKey = result;
          callback(err, result);
        }),
      getKey: ['localStorage', (results, callback) => {
        if(results.localStorage) {
          return callback(null, results.localStorage);
        }
        newKey = {
          '@context': 'https://w3id.org/security/v2',
          id: publicKeyId,
          type: [
            'https://w3id.org/identity#Identity',
            'https://w3id.org/webledger#Continuity2017Peer',
            'Ed25519VerificationKey2018'],
          owner: publicKeyId,
          publicKey: publicKeyId,
          publicKeyBase58: _getPublicKeyFromId(publicKeyId)
        };
        callback(null, newKey);
      }]
    }, (err, results) => callback(err, results.getKey));
  }

  function _getPublicKeyOwner(owner, options, callback) {
    // TODO: remove cache, always generate, not a security issue for
    // blockchain nodes
    if(cacheKey) {
      // auto-generate owner identity because it was validated when key was
      // added to the cache
      keyOwner = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        type: ['Identity', 'Continuity2017Peer'],
        id: owner,
        publicKey: cacheKey
      };
      return callback(null, keyOwner);
    }
    // TODO: don't need cache, always generate the owner document
    // cache miss, create the owner document
    keyOwner = {
      '@context': 'https://w3id.org/webledger/v1',
      id: owner,
      type: ['Identity', 'Continuity2017Peer'],
      publicKey: {
        id: owner,
        type: 'Ed25519VerificationKey2018',
        owner: owner,
        publicKeyBase58: _getPublicKeyFromId(owner)
      }
    };
    callback(null, keyOwner);
  }
};

function _getPublicKeyFromId(id) {
  // TODO: should we do this check (or an even more comprehensive one
  //   or be strict like we are now?)
  // remove trailing slash
  // if(id.endsWith('/')) {
  //   id = id.substr(0, id.length - 1);
  // }
  const parsed = url.parse(id);
  const last = parsed.pathname.split('/').pop();
  return last;
}
