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
const pool = {};
Object.defineProperty(pool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-consensus-continuity'].gossip.requestPool.maxSockets
});

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

api.sendEvent = ({callerId, event, eventHash, mergeHash, peerId}, callback) => {
  const url = `${peerId}/events`;
  request.post({
    json: {callerId, event, eventHash, mergeHash}, pool, strictSSL, timeout, url
  }, (err, res) => {
    if(err) {
      return callback(new BedrockError(
        'Could not send event.', 'NetworkError', {event, peerId}, err));
    }
    if(res.statusCode === 409) {
      return callback(new BedrockError(
        'The merge event associated with this event already exists.',
        'DuplicateError', {
          eventHash,
          mergeHash,
          peerId,
          public: true,
        }));
    }
    // server already had event, but keep sending
    if(res.statusCode === 204) {
      return callback();
    }
    if(res.statusCode === 201) {
      return callback(null, res.headers.location);
    }
    return callback(new BedrockError(
      'Could not send event.',
      'NetworkError', {
        error: res.body,
        event,
        eventHash,
        httpStatusCode: res.statusCode,
        mergeHash,
        peerId,
        public: true,
      }));
  });
};

api.getEvent = ({eventHash, peerId}, callback) => {
  const url = peerId + '/events?id=' + encodeURIComponent(eventHash);
  request.get({url, strictSSL, json: true, pool, timeout}, (err, res) => {
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
