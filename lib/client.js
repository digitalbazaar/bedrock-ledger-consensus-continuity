/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const axios = require('axios');
const bedrock = require('bedrock');
const {config} = bedrock;
const {callbackify, BedrockError} = bedrock.util;
// NOTE: request.defaults not used here so that request can be stubbed
const https = require('https');
const requestPromise = require('request-promise-native');
const {validate} = require('bedrock-validation');

require('./config');

// module API
const api = {};
module.exports = api;

// define request pool for all gossip requests
const pool = {};
Object.defineProperty(pool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-consensus-continuity'].gossip.requestPool.maxSockets
});

// TODO: document; returns a stream
api.getEvents = async ({eventHash, peerId}) => {
  const url = peerId + '/events-query';
  const data = {eventHash};
  const {
    jsonld: {strictSSL},
    'ledger-consensus-continuity': {client: {timeout}}
  } = config;
  return axios({
    httpsAgent: new https.Agent({rejectUnauthorized: strictSSL}),
    method: 'POST', url, strictSSL, data, responseType: 'stream', timeout
  }).then(res => res.data, error => {
    throw error;
  });
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
  const {strictSSL} = config.jsonld;
  const {timeout} = config['ledger-consensus-continuity'].client;
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
  const {strictSSL} = config.jsonld;
  const {timeout} = config['ledger-consensus-continuity'].client;
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
