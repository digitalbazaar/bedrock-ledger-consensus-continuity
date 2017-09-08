/*
 * Client for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const election = require('./election');
// NOTE: request.defaults not used here so that request can be stubbed
const request = require('request');
const validate = require('bedrock-validation').validate;
const BedrockError = bedrock.util.BedrockError;

require('./config');

// module API
const api = {};
module.exports = api;

const strictSSL = config.jsonld.strictSSL;

// define request pool for all gossip requests
const requestPool = {};
Object.defineProperty(requestPool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-consensus-continuity'].gossip.requestPool.maxSockets
});

// TODO: make `peer` the first parameter for all of these functions

api.getBlockStatus = (blockHeight, peer, callback) => {
  const url = peer + '/blocks/' +
    encodeURIComponent(blockHeight) + '/status';
  request.get({url, strictSSL, json: true}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not get block status.', 'NetworkError', {blockHeight, peer},
        err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Could not get block status.',
        res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
          httpStatusCode: res.statusCode,
          public: true,
          blockHeight,
          peer,
          error: res.body,
        }));
    }
    validate('continuity.blockStatus', res.body, err =>
      callback(err, res.body));
  });
};

api.getManifest = (manifestHash, peer, callback) => {
  const url = peer + '/manifests?id=' +
    encodeURIComponent(manifestHash);
  async.auto({
    get: callback => request.get({url, strictSSL, json: true}, (err, res) => {
      if(err) {
        return callback(new BedrockError(
          'Could not get manifest.', 'NetworkError',
          {manifestHash, peer}, err));
      }
      if(res.statusCode !== 200) {
        return callback(new BedrockError(
          'Could not get manifest.',
          res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
            httpStatusCode: res.statusCode,
            public: true,
            manifestHash,
            peer,
            error: res.body,
          }));
      }
      callback(null, res.body);
    }),
    validate: ['get', (results, callback) =>
      validate('continuity.manifest', results.get, callback)],
    validateHash: ['validate', (results, callback) => {
      const manifest = results.get;
      const expectedHash = election.createManifestHash(manifest.item);
      if(manifest.id !== manifestHash) {
        return callback(new BedrockError(
          'The hash contained in the manifest is invalid.',
          'ValidationError', {
            httpStatusCode: 400,
            public: true,
            expectedHash,
            manifest,
            peer,
          }));
      }
      callback();
    }]
  }, (err, results) => err ? callback(err) : callback(err, results.get));
};

api.sendEvent = (event, peer, callback) => {
  const url = peer + '/events';
  request.post({url, strictSSL, json: event}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not send event.', 'NetworkError', {event, peer}, err));
    }
    if(res.statusCode !== 201) {
      return callback(new BedrockError(
        'Could not send event.',
        'NetworkError', {
          httpStatusCode: res.statusCode,
          public: true,
          event,
          peer,
          error: res.body,
        }));
    }
    callback(null, res.headers.location);
  });
};

api.sendManifest = (manifest, peer, callback) => {
  const url = peer + '/manifests';
  request.post({url, strictSSL, json: manifest}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not send manifest.', 'NetworkError', {manifest, peer}, err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Could not send manifest.',
        'NetworkError', {
          httpStatusCode: res.statusCode,
          public: true,
          manifest,
          peer,
          error: res.body,
        }));
    }
    callback();
  });
};

api.sendVote = (voteRecord, peer, callback) => {
  const url = peer + '/votes/' + voteRecord.topic;
  request.post({url, strictSSL, json: voteRecord.vote}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not send vote.', 'NetworkError', {voteRecord, peer}, err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Could not send vote.',
        'NetworkError', {
          httpStatusCode: res.statusCode,
          voteRecord,
          peer,
          error: res.body,
        }));
    }
    callback();
  });
};

api.getEvent = (eventHash, peer, callback) => {
  const url = peer + '/events?id=' +
    encodeURIComponent(eventHash);
  request.get({url, strictSSL, json: true}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not get event.', 'NetworkError', {eventHash, peer}, err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Could not get event.',
        res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
          httpStatusCode: res.statusCode,
          public: true,
          eventHash,
          peer,
          error: res.body,
        }));
    }
    validate('continuity.event', res.body, err =>
      callback(err, res.body));
  });
};
