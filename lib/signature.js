/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brDidClient = require('bedrock-did-client');
const forge = require('node-forge');
const jsigs = require('jsonld-signatures')();
// const logger = require('./logger');
const storage = require('./storage');
const BedrockError = bedrock.util.BedrockError;
const url = require('url');

const api = {};
module.exports = api;

jsigs.use('jsonld', brDidClient.jsonld);

// verify the signature on a document utilizing the publicKey cache
api.verify = ({doc, ledgerNodeId}, callback) => {
  let cacheKey;
  let newKey;
  let keyOwner;
  async.auto({
    verify: callback => jsigs.verify(doc, {
      getPublicKey: _getPublicKey,
      getPublicKeyOwner: _getPublicKeyOwner
    }, (err, result) => callback(err, result)),
    storeKey: ['verify', (results, callback) => {
      // no new key to store
      if(!newKey) {
        return callback();
      }
      // store new key, ignore all errors
      storage.keys.addPublicKey(ledgerNodeId, newKey, () => callback());
    }],
    finalize: ['storeKey', (results, callback) => {
      if(!results.verify.verified) {
        return callback(new BedrockError(
          'Document signature verification failed.',
          'AuthenticationError', {doc, keyResults: results.verify.keyResults}));
      }

      // confirm that the last path element of the voter ID matches the key's
      // fingerprint
      const key = cacheKey || newKey;
      if(key.publicKeyPem) {
        // get last path element of owner ID
        const parsedKeyOwner = url.parse(keyOwner.id);
        const last = parsedKeyOwner.pathname.split('/').pop();
        const fingerprint = api.getPublicKeyFingerprint(key.publicKeyPem);
        if(fingerprint !== last) {
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

  function _getPublicKey(keyId, options, callback) {
    async.auto({
      cache: callback => storage.keys.getPublicKey(
        ledgerNodeId, {id: keyId}, (err, result) => {
          if(err && err.name === 'NotFoundError') {
            // cache miss
            return callback();
          }
          cacheKey = result;
          callback(err);
        }),
      getKey: ['cache', (results, callback) => {
        if(cacheKey) {
          return callback(null, cacheKey);
        }
        jsigs.getPublicKey(keyId, options, (err, result) => {
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

/**
 * Get the SubjectPublicKeyInfo base64url-encoded fingerprint for the
 * given public key PEM.
 *
 * @param publicKeyPem the public key PEM to get the fingerprint for.
 *
 * @return the public key's fingerprint.
 */
api.getPublicKeyFingerprint = publicKeyPem => {
  const publicKey = forge.pki.publicKeyFromPem(publicKeyPem);
  const fingerprint = forge.pki.getPublicKeyFingerprint(
    publicKey, {
      md: forge.md.sha256.create(),
      type: 'SubjectPublicKeyInfo'
    });
  // base64url encode
  return forge.util.encode64(fingerprint.getBytes())
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
};
