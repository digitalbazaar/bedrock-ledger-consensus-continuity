/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const axios = require('axios');
const brHttpsAgent = require('bedrock-https-agent');
const {httpClient} = require('@digitalbazaar/http-client');
const https = require('https');
const bedrock = require('bedrock');
const {config, util: {BedrockError}} = bedrock;
const LRU = require('lru-cache');
const {pipeline} = require('stream');
const split2 = require('split2');
const {validate} = require('bedrock-validation');
const {URL} = require('url');

let EVENTS_VALIDATION_HTTPS_AGENT;
const EVENTS_VALIDATION_URL_CACHE = new LRU({max: 100, maxAge: 60 * 1000});
bedrock.events.on('bedrock.ready', async () => {
  const {'ledger-consensus-continuity': {
    gossip: {eventsValidation}
  }} = config;

  const {httpsAgentOpts} = eventsValidation;

  if(httpsAgentOpts) {
    EVENTS_VALIDATION_HTTPS_AGENT = new https.Agent(httpsAgentOpts);
  } else {
    EVENTS_VALIDATION_HTTPS_AGENT = brHttpsAgent.httpsAgent;
  }
});

// TODO: document; returns a stream
exports.getEventStream = async ({eventHash, remotePeer}) => {
  const url = remotePeer.url + '/events-query';
  const data = {eventHash};
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
  const {httpsAgent} = brHttpsAgent;
  const response = await httpClient.post(url, {
    agent: httpsAgent,
    json: data,
    timeout
  });
  if(!response.ok) {
    throw new Error(`Error retrieving events from peer: "${remotePeer.id}"`);
  }
  return response.body;
};

exports.validateEvent = async ({event, ledgerNodeId, localPeerId, session}) => {
  const {'ledger-consensus-continuity': {
    gossip: {eventsValidation}
  }} = config;

  const url = _getValidationServiceUrl({localPeerId});

  const data = {event, ledgerNodeId, session};
  const response = await httpClient.post(url, {
    agent: EVENTS_VALIDATION_HTTPS_AGENT,
    json: data,
    timeout: eventsValidation.timeout
  });

  return response.data;
};

exports.getHistory = async ({
  basisBlockHeight, localPeerId, peerHeadsMap, remotePeer, localEventNumber
} = {}) => {
  if(!localPeerId) {
    throw new TypeError('"localPeerId" is required.');
  }
  const {id: remotePeerId} = remotePeer;
  const url = `${remotePeer.url}/gossip`;
  // send only the event hashes for the peer heads, the server would have
  // to validate any other information anyway because it will assume we are
  // an untrusted peer
  const peerHeads = [];
  for(const heads of peerHeadsMap.values()) {
    for(const head of heads) {
      peerHeads.push(head.eventHash);
    }
  }
  const data = {
    // this is the latest block height that has reached consensus on the client
    basisBlockHeight,
    // the peerId sent to the peer node is the peerId of the local node
    peerId: localPeerId,
    peerHeads
  };
  // if `localEventNumber` is specified, include it for skipping events
  // that have already been received
  if(localEventNumber !== undefined) {
    data.localEventNumber = localEventNumber;
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
        httpStatusCode: res.status,
        localPeerId,
        remotePeerId,
        public: true
      });
  }
  // FIXME: validate res.data
  return res.data;
};

exports.notifyPeer = async ({localPeerId, remotePeer}) => {
  const {id: remotePeerId} = remotePeer;
  const url = `${remotePeer.url}/notify`;
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
  const {httpsAgent} = brHttpsAgent;
  // FIXME: use http-signature to authenticate
  try {
    await axios({
      httpsAgent,
      method: 'POST',
      url,
      // the peerId sent to the remote peer is the peerId of the local peer
      // FIXME: send actual gossip URL instead of `localPeerId` twice
      data: {peer: {id: localPeerId, url: localPeerId}},
      timeout,
    });
  } catch(error) {
    const {cause, httpStatusCode} = _processAxiosError(error);
    throw new BedrockError(
      'Could not send peer notification.', 'NetworkError',
      {localPeerId, httpStatusCode, remotePeerId}, cause);
  }
};

exports.getEvents = async function({eventHashes, remotePeer}) {
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
      const stream = await exports.getEventStream({eventHash, remotePeer});
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
              remotePeer,
            }, text);
        } catch(e) {
          throw new BedrockError(
            error.message || 'A network error occurred.',
            'NetworkError', {
              httpStatusCode: error.response.status,
              remotePeer
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
        remotePeer,
      });
  }
  // return the accumulated events
  return events;
};

function _getValidationServiceUrl({localPeerId}) {
  const {'ledger-consensus-continuity': {
    gossip: {eventsValidation}
  }} = config;

  // events validation url is not set, use the node's localPeerId
  if(!eventsValidation.baseUrl) {
    return localPeerId + '/events-validation';
  }

  const key = `${localPeerId}${eventsValidation.baseUrl}`;

  let eventsValidationUrl = EVENTS_VALIDATION_URL_CACHE.get(key);
  if(eventsValidationUrl) {
    return eventsValidationUrl;
  }

  const url = new URL(localPeerId);
  eventsValidationUrl = eventsValidation.baseUrl +
    `${url.pathname}/events-validation`;

  EVENTS_VALIDATION_URL_CACHE.set(key, eventsValidationUrl);

  return eventsValidationUrl;
}

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
