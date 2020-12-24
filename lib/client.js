/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const axios = require('axios');
const brHttpsAgent = require('bedrock-https-agent');
const {httpClient} = require('@digitalbazaar/http-client');
const {config, util: {BedrockError}} = require('bedrock');
const {pipeline} = require('stream');
const split2 = require('split2');
const {validate} = require('bedrock-validation');

// TODO: document; returns a stream
exports.getEventStream = async ({eventHash, peerId}) => {
  const url = peerId + '/events-query';
  const data = {eventHash};
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
  const {httpsAgent} = brHttpsAgent;
  const response = await httpClient.post(url, {
    agent: httpsAgent,
    json: data,
    timeout
  });
  if(!response.ok) {
    throw new Error(`Error retrieving events from peer: "${peerId}"`);
  }
  return response.body;
};

exports.getHistory = async ({
  blockHeight, localPeerId, peerHeads, remotePeerId,
  // FIXME: old params
  creatorHeads, headsOnly
} = {}) => {
  if(!localPeerId) {
    throw new TypeError('"localPeerId" is required.');
  }
  const url = `${remotePeerId}/gossip`;
  const heads = [];
  for(const [creator, head] of peerHeads) {
    heads.push({creator, ...head});
  }
  // the peerId sent to the peer node is the peerId of the local node
  const data = {
    blockHeight,
    peerId: localPeerId,
    peerHeads: heads
  };
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
  } catch(error) {
    const {cause, httpStatusCode} = _processAxiosError(error);
    throw new BedrockError(
      'Could not get peer history.', 'NetworkError',
      {httpStatusCode, localPeerId, remotePeerId}, cause);
  }
  if(!res.data) {
    throw new BedrockError(
      'Could not get peer history. Response body was empty.', 'NetworkError', {
        creatorHeads,
        headsOnly,
        httpStatusCode: res.status,
        localPeerId,
        remotePeerId,
        public: true
      });
  }
  // FIXME: validate body `{creatorHeads, history, truncated}`
  return res.data;
};

exports.notifyPeer = async ({callerId, peerId}) => {
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
  } catch(error) {
    const {cause, httpStatusCode} = _processAxiosError(error);
    throw new BedrockError(
      'Could not send peer notification.', 'NetworkError',
      {callerId, httpStatusCode, peerId}, cause);
  }
};

exports.getEvents = async function({eventHashes, peerId}) {
  /* Note: Here we connect to the peer and download all requested events. The
  events currently must be added in order (TODO: handle ordering later and
  just put them into the cache as quickly as possible to optimize).

  If an error occurs with adding the events (since they must be added
  in order), we should terminate the connection to the peer immediately and
  bail out. */
  const events = [];

  // request events in chunks for size `maxEvents`
  const {'ledger-consensus-continuity': {gossip: {maxEvents}}} = config;
  const chunks = _.chunk(eventHashes, maxEvents);
  for(const eventHash of chunks) {
    try {
      const stream = await exports.getEventStream({eventHash, peerId});
      const eventIterator = pipeline(stream, split2(), _pipelineComplete);
      for await (const e of eventIterator) {
        const eventJson = JSON.parse(e);
        const result = validate('continuity.webLedgerEvents', eventJson.event);
        if(!result.valid) {
          throw result.error;
        }
        events.push(eventJson.event);
      }
    } catch(error) {
      if(error.response) {
        // The request was made and the server responded with a status code
        // that falls out of the range of 2xx
        try {
          const text = await error.response.text();
          throw new BedrockError(
            error.message || 'A network error occurred.',
            'NetworkError', {
              httpStatusCode: error.response.status,
              peerId,
            }, text);
        } catch(e) {
          throw new BedrockError(
            error.message || 'A network error occurred.',
            'NetworkError', {
              httpStatusCode: error.response.status,
              peerId
            });
        }
      }
      // no response was received or something happened in setting up the
      // request that triggered an Error
      const message = error.message || 'An HTTP Error occurred.';
      throw new BedrockError(message, 'NetworkError', {}, error);
    }
  }
  if(eventHashes.length !== events.length) {
    // we did not receive the events that what we needed
    throw new BedrockError(
      'The peer did not fulfill the request for requested events.',
      'DataError', {
        requestedLength: eventHashes.length,
        receivedLength: events.length,
        peerId,
      });
  }
  // return the accumulated events
  return events;
};

function _pipelineComplete(/* err */) {
  // error is handled before this handler is called, this is a no-op
}

function _processAxiosError(error) {
  const {request, response} = error;
  let cause;
  let httpStatusCode;
  if(response && response.data && response.data.details) {
    // it's BedrockError
    httpStatusCode = response.status;
    cause = new BedrockError(
      response.data.message, response.data.type, response.data.details);
  } else if(response) {
    httpStatusCode = response.status;
    cause = new BedrockError('An HTTP error occurrred.', 'NetworkError', {
      data: response.data,
      httpStatusCode,
    });
  } else if(request) {
    // no status code available
    const {address, code, errno, port} = error;
    cause = new BedrockError(error.message, 'NetworkError', {
      address, code, errno, port
    });
  } else {
    // no status code available
    cause = new BedrockError(error.message, 'NetworkError');
  }
  return {cause, httpStatusCode};
}
