/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const axios = require('axios');
const brHttpsAgent = require('bedrock-https-agent');
const {config, util: {callbackify, BedrockError}} = require('bedrock');

// TODO: document; returns a stream
exports.getEvents = async ({eventHash, peerId}) => {
  const url = peerId + '/events-query';
  const data = {eventHash};
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
  const {httpsAgent} = brHttpsAgent;
  return axios({
    httpsAgent,
    method: 'POST', url, data, responseType: 'stream', timeout
  }).then(res => res.data, error => {
    throw error;
  });
};

exports.getHistory = callbackify(async (
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
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
  const {httpsAgent} = brHttpsAgent;
  let res;
  try {
    res = await axios({
      httpsAgent,
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

exports.notifyPeer = callbackify(async ({callerId, peerId}) => {
  const url = `${peerId}/notify`;
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
  const {httpsAgent} = brHttpsAgent;
  try {
    await axios({
      httpsAgent,
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
