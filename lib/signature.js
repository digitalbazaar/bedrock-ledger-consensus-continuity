/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brDidClient = require('bedrock-did-client');
const jsigs = require('jsonld-signatures')();
const storage = require('./storage');
const BedrockError = bedrock.util.BedrockError;

const api = {};
module.exports = api;

jsigs.use('jsonld', brDidClient.jsonld);

// verify the signature on a document utilizing the publicKey cache
api.verify = ({doc, ledgerNodeId}, callback) => {
  let cacheKey;
  let newKey;
  async.auto({
    verify: callback => jsigs.verify(doc, {
      getPublicKey: _getPublicKey,
      getPublicKeyOwner: _getPublicKeyOwner
    }, (err, result) => {
      if(err) {
        return callback(err);
      }
      if(!result.verified) {
        return callback(new BedrockError(
          'Document signature verification failed.',
          'AuthenticationError', {doc, keyResults: result.keyResults}));
      }
      callback();
    }),
    storeKey: ['verify', (results, callback) => {
      // key was verified, store it if it's new
      if(!newKey) {
        return callback();
      }
      // ignore all errors
      storage.keys.addPublicKey(ledgerNodeId, newKey, () => callback());
    }]
  }, callback);

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
          newKey = result;
          callback(err, result);
        });
      }]
    }, (err, results) => callback(err, results.getKey));
  }

  function _getPublicKeyOwner(owner, options, callback) {
    if(cacheKey) {
      // auto-generate owner identity because it was validated when key was
      // added to the cache
      return callback(null, {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        type: ['Identity', 'Continuity2017Peer'],
        id: owner,
        publicKey: cacheKey
      });
    }
    // cache miss, get the owner document
    jsigs.getJsonLd(owner, options, callback);
  }
};
