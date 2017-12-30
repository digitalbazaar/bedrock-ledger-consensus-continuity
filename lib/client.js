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
const timeout = config['ledger-consensus-continuity'].client.timeout;

// define request pool for all gossip requests
const requestPool = {};
Object.defineProperty(requestPool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-consensus-continuity'].gossip.requestPool.maxSockets
});

api.getHistory = ({creatorHeads, peerId}, callback) => {
  const url = peerId + '/gossip';
  request.post(
    {url, strictSSL, json: {creatorHeads}, timeout}, (err, res) => {
      if(err) {
        return callback(new BedrockError(
          'Could not get history.', 'NetworkError', {peerId, creatorHeads},
          err));
      }
      if(res.statusCode !== 200) {
        return callback(new BedrockError(
          'Could not get history.',
          res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
            httpStatusCode: res.statusCode,
            public: true,
            peerId,
            creatorHeads,
            error: res.body,
          }));
      }
      callback(null, res.body);
    });
};

api.sendEvent = ({event, eventHash, peerId}, callback) => {
  const url = `${peerId}/events`;
  request.post(
    {url, strictSSL, json: {event, eventHash}, timeout}, (err, res) => {
      if(err) {
        return callback(new BedrockError(
          'Could not send event.', 'NetworkError', {event, peerId}, err));
      }
      /*
      if(res.statusCode === 409) {
        // ignore duplicate
        return callback();
      }
      */
      if(res.statusCode !== 201) {
        return callback(new BedrockError(
          'Could not send event.',
          'NetworkError', {
            httpStatusCode: res.statusCode,
            public: true,
            event,
            eventHash,
            peerId,
            error: res.body,
          }));
      }
      callback(null, res.headers.location);
    });
};

api.getEvent = ({eventHash, peerId}, callback) => {
  const url = peerId + '/events?id=' +
    encodeURIComponent(eventHash);
  request.get({url, strictSSL, json: true, timeout}, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not get event.', 'NetworkError', {eventHash, peerId}, err));
    }
    if(res.statusCode !== 200) {
      return callback(new BedrockError(
        'Could not get event.',
        res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
          httpStatusCode: res.statusCode,
          public: true,
          eventHash,
          peerId,
          error: res.body,
        }));
    }
    // NOTE: a genesis merge event will not pass validation because it does
    // not have a `treeHash` property, there is currently no use case
    // for retrieving the genesis merge event via this API
    validate('continuity.webLedgerEvents', res.body, err =>
      callback(err, res.body));
  });
};
