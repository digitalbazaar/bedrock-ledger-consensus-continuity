/*
 * Client for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const config = bedrock.config;
// NOTE: request.defaults not used here so that request can be stubbed
const request = require('request');
// const validate = require('bedrock-validation').validate;
const BedrockError = bedrock.util.BedrockError;

require('./config');

// module API
const api = {};
module.exports = api;

const strictSSL = config.jsonld.strictSSL;
const timeout = config['ledger-consensus-continuity'].client.timeout;

// define request pool for all gossip requests
const pool = {};
Object.defineProperty(pool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-consensus-continuity'].gossip.requestPool.maxSockets
});

api.getCompressedHistory = ({callerId, creatorHeads, peerId}, callback) => {
  if(!(callerId && creatorHeads)) {
    throw new TypeError('`callerId` and `creatorHeads` are required.');
  }
  const url = peerId + '/gossip-compressed';
  const json = {callerId, creatorHeads};
  const buffers = [];
  const BOUNDARY = '###c680ff7e49bb###';
  const r = request.post({url, strictSSL, json, timeout});
  let jsonStarted = false;
  let jsonComplete = false;
  let fileStarted = false;
  let jsonPayload = '';
  r.on('error', callback);
  r.on('response', res => {
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Error returned by server.',
        res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
          httpStatusCode: res.statusCode,
          peerId,
          public: true,
        }));
    }
    r.on('data', data => {
      // console.log('====', data.toString());
      if(!jsonComplete && !jsonStarted &&
        data.toString('utf8').includes(BOUNDARY)) {
        jsonStarted = true;
      } else if(jsonStarted && !jsonComplete &&
        data.toString('utf8').includes(BOUNDARY)) {
        jsonComplete = true;
        fileStarted = true;
      } else if(jsonStarted && !jsonComplete) {
        jsonPayload += data.toString('utf8');
      } else if(fileStarted) {
        buffers.push(data);
      }
    });
    r.on('end', err => {
      if(err) {
        return callback(err);
      }
      let creatorHeads;
      try {
        creatorHeads = JSON.parse(jsonPayload);
      } catch(err) {
        return callback(err);
      }
      callback(null, {creatorHeads, file: Buffer.concat(buffers)});
    });
  });
};

api.getHistory = ({callerId, creatorHeads, headsOnly, peerId}, callback) => {
  if(!callerId) {
    throw new TypeError('`callerId` is required.');
  }
  const url = peerId + '/gossip';
  const json = {callerId};
  if(creatorHeads) {
    json.creatorHeads = creatorHeads;
  }
  if(headsOnly) {
    json.headsOnly = true;
  }
  request.post(
    {url, strictSSL, json, timeout}, (err, res) => {
      if(err) {
        return callback(new BedrockError(
          'Could not get history.', 'NetworkError', {peerId, creatorHeads},
          err));
      }
      if(res.statusCode === 503) {
        return callback(new BedrockError(
          'Gossip session already in progress.', 'AbortError', {
            error: res.body,
            headsOnly,
            httpStatusCode: res.statusCode,
            peerId,
            public: true,
          }));
      }
      if(res.statusCode !== 200) {
        return callback(new BedrockError(
          'Could not get history.',
          res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
            creatorHeads,
            error: res.body,
            headsOnly,
            httpStatusCode: res.statusCode,
            peerId,
            public: true,
          }));
      }
      callback(null, res.body);
    });
};

api.notifyPeer = ({callerId, peerId}, callback) => {
  const url = `${peerId}/notify`;
  request.post({
    json: {callerId}, strictSSL, timeout, url
  }, (err, res) => {
    if(err) {
      return callback(err);
    }
    if(res.statusCode !== 204) {
      return callback(new BedrockError(
        'Could not send notification.', 'NetworkError', {callerId, peerId}));
    }
    callback();
  });
};
