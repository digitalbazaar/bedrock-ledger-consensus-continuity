/*
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

// verify the signature on a document utilizing the publicKey cache
api.verify = ({doc, ledgerNodeId}, callback) => {
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
      const keyResultError = _.get(results.verify, 'keyResults.error');
      const {verified} = results.verify;
      logger.debug('Signature Status', {verified, keyResultError});
      if(!results.verify.verified) {
        return callback(new BedrockError(
          'Document signature verification failed.',
          'AuthenticationError', {doc, keyResults: results.verify.keyResults}));
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
        jsigs.getPublicKey(publicKeyId, options, (err, result) => {
          if(err) {
            return callback(err);
          }
          newKey = result;
          callback(null, result);
        });
      }]
    }, (err, results) => callback(err, results.getKey));
  }

  function _getPublicKeyOwner(owner, options, callback) {
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
    // cache miss, get the owner document
    jsigs.getJsonLd(owner, options, (err, owner) => {
      keyOwner = owner;
      callback(err, owner);
    });
  }
};
