/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config} = bedrock;
const {callbackify, BedrockError} = bedrock.util;
// NOTE: request.defaults not used here so that request can be stubbed
const request = require('request-promise-native');
const requestPromise = require('request-promise-native');
// TODO: replace once validation supports promises
const {promisify} = require('util');
const validate = promisify(require('bedrock-validation').validate);

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

api.getEvent = callbackify(async ({eventHash, peerId}) => {
  const url = peerId + '/events?id=' + encodeURIComponent(eventHash);
  let res;
  try {
    res = await requestPromise.get({
      url,
      strictSSL,
      json: true,
      pool,
      timeout,
      simple: false,
      resolveWithFullResponse: true
    });
  } catch(e) {
    throw new BedrockError(
      'Could not get event.', 'NetworkError', {eventHash, peerId}, e);
  }

  if(res.statusCode !== 200) {
    throw new BedrockError(
      'Could not get event.',
      res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
        httpStatusCode: res.statusCode,
        public: true,
        eventHash,
        peerId,
        error: res.body,
      });
  }

  // NOTE: a genesis merge event will not pass validation because it does
  // not have a `treeHash` property, there is currently no use case
  // for retrieving the genesis merge event via this API
  await validate('continuity.webLedgerEvents', res.body);
  return res.body;
});

// TODO: document; returns a stream
api.getEvents = ({eventHash, peerId}) => {
  const url = peerId + '/events-query';
  const json = {eventHash};
  return request.post({url, strictSSL, json, pool, timeout});
};

api.getHistory = callbackify(async (
  {callerId, creatorHeads, headsOnly, peerId}) => {
  if(!callerId) {
    throw new TypeError('"callerId" is required.');
  }
  const url = peerId + '/gossip';
  const json = {callerId};
  if(creatorHeads) {
    json.creatorHeads = creatorHeads;
  }
  if(headsOnly) {
    json.headsOnly = true;
  }
  let res;
  try {
    res = await requestPromise.post({
      url,
      strictSSL,
      json,
      timeout,
      simple: false,
      resolveWithFullResponse: true
    });
  } catch(e) {
    throw new BedrockError(
      'Could not get history.', 'NetworkError', {peerId, creatorHeads}, e);
  }

  if(res.statusCode === 503) {
    throw new BedrockError(
      'Gossip session already in progress.', 'AbortError', {
        error: res.body,
        headsOnly,
        httpStatusCode: res.statusCode,
        peerId,
        public: true,
      });
  }
  if(res.statusCode !== 200) {
    throw new BedrockError(
      'Could not get history.',
      res.statusCode === 404 ? 'NotFoundError' : 'NetworkError', {
        creatorHeads,
        error: res.body,
        headsOnly,
        httpStatusCode: res.statusCode,
        peerId,
        public: true,
      });
  }
  if(!res.body) {
    throw new BedrockError(
      'Could not get history. Response body was empty.', 'NetworkError', {
        creatorHeads,
        headsOnly,
        httpStatusCode: res.statusCode,
        peerId,
        public: true,
      });
  }

  // FIXME: validate body `{creatorHeads, history, truncated}`?

  return res.body;
});

api.notifyPeer = callbackify(async ({callerId, peerId}) => {
  const url = `${peerId}/notify`;
  let res;
  try {
    res = await requestPromise.post({
      url,
      json: {callerId},
      strictSSL,
      timeout,
      simple: false,
      resolveWithFullResponse: true
    });
  } catch(e) {
    throw new BedrockError(
      'Could not send notification.', 'NetworkError', {callerId, peerId}, e);
  }

  if(res.statusCode !== 204) {
    throw new BedrockError(
      'Could not send notification.', 'NetworkError', {callerId, peerId});
  }
});
