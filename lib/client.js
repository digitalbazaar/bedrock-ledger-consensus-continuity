/*
 * Client for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const config = bedrock.config;
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
  request.get({url, strictSSL, json: true}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not get manifest.', 'NetworkError', {manifestHash, peer}, err));
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
    // TODO: validate manifest
    callback(null, res.body);
  });
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
