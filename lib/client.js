/*
 * Client for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const config = bedrock.config;
let request = require('request');
// FIXME: deal with strictSSL
request = request.defaults({json: true, strictSSL: false});
const BedrockError = bedrock.util.BedrockError;

require('./config');

// module API
const api = {};
module.exports = api;

// define request pool for all gossip requests
const requestPool = {};
Object.defineProperty(requestPool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-continuity'].gossip.requestPool.maxSockets
});

api.getBlockStatus = (blockHeight, peer, callback) => {
  const url = peer.id + '/blocks/' +
    encodeURIComponent(blockHeight) + '/status';
  request.get(url, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {blockHeight, peer}, err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {
          httpStatusCode: res.statusCode,
          message: res.body,
          blockHeight,
          peer
        }));
    }
    callback(null, res.body);
  });
};

api.getManifest = (manifestHash, peer, callback) => {
  // TODO: get manifest (event hashes) from peer and store via manifestStorage
  const url = peer.id + '/manifests?id=' +
    encodeURIComponent(manifestHash);
  request.get(url, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {manifestHash, peer}, err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {
          httpStatusCode: res.statusCode,
          message: res.body,
          manifestHash,
          peer
        }));
    }
    callback(null, res.body);
  });
};

api.sendEvent = (event, peer, callback) => {
  const url = peer.id + '/events';
  request.post({url, json: event}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {event, peer}, err));
    }
    if(res.statusCode !== 201) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {
          httpStatusCode: res.statusCode,
          message: res.body,
          event,
          peer
        }));
    }
    callback(null, res.headers.location);
  });
};

api.getEvent = (eventHash, peer, callback) => {
  const url = peer.id + '/events?id=' +
    encodeURIComponent(eventHash);
  request.get(url, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {eventHash, peer}, err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Communications error.',
        // FIXME: something else
        'NotFound', {
          httpStatusCode: res.statusCode,
          message: res.body,
          eventHash,
          peer
        }));
    }
    callback(null, res.body);
  });
};
