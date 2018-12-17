/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const axios = require('axios');
const {config, util: {callbackify, BedrockError}} = require('bedrock');
const https = require('https');

require('./config');

// module API
const api = {};
module.exports = api;

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
  // the peerId sent to the peer node is the peerId of the local node
  const data = {peerId: callerId};
  if(creatorHeads) {
    data.creatorHeads = creatorHeads;
  }
  if(headsOnly) {
    data.headsOnly = true;
  }
  const {
    jsonld: {strictSSL},
    'ledger-consensus-continuity': {client: {timeout}}
  } = config;
  let res;
  try {
    res = await axios({
      httpsAgent: new https.Agent({rejectUnauthorized: strictSSL}),
      method: 'POST',
      url,
      data,
      timeout,
    });
  } catch(e) {
    const {response} = e;
    let cause;
    // it's BedrockError
    if(response && response.data && response.data.details) {
      cause = new BedrockError(
        response.data.message, response.data.type, response.data.details);
    }
    cause = cause || e;
    throw new BedrockError(
      'Could not send notification.', 'NetworkError',
      {callerId, peerId}, cause);
  }
  if(!res.data) {
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
  return res.data;
});

api.notifyPeer = callbackify(async ({callerId, peerId}) => {
  const url = `${peerId}/notify`;
  const {
    jsonld: {strictSSL},
    'ledger-consensus-continuity': {client: {timeout}}
  } = config;
  try {
    await axios({
      httpsAgent: new https.Agent({rejectUnauthorized: strictSSL}),
      method: 'POST',
      url,
      // the peerId sent to the peer node is the peerId of the local node
      data: {peerId: callerId},
      timeout,
    });
  } catch(e) {
    const {response} = e;
    let cause;
    // it's BedrockError
    if(response && response.data && response.data.details) {
      cause = new BedrockError(
        response.data.message, response.data.type, response.data.details);
    }
    cause = cause || e;
    throw new BedrockError(
      'Could not send notification.', 'NetworkError',
      {callerId, peerId}, cause);
  }
});
