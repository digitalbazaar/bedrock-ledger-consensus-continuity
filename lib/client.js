/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const {httpsAgent} = require('bedrock-https-agent');
const {httpClient} = require('@digitalbazaar/http-client');
const {
  createAuthzHeader,
  createSignatureString
} = require('http-signature-header');
const {createHeaderValue} = require('@digitalbazaar/http-digest-header');
const https = require('https');
const bedrock = require('bedrock');
const {config, util: {BedrockError}} = bedrock;
const LRU = require('lru-cache');
const {pipeline} = require('stream');
const split2 = require('split2');
const {validate} = require('bedrock-validation');
const {URL} = require('url');
const {getKeyPair} = require('./signature');

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
    EVENTS_VALIDATION_HTTPS_AGENT = httpsAgent;
  }
});

// TODO: document; returns a stream
exports.getEventStream = async ({eventHash, remotePeer}) => {
  const url = remotePeer.url + '/events-query';
  const data = {eventHash};
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
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

  const url = exports.getValidationServiceUrl({localPeerId});

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
  let res;
  try {
    res = await httpClient.post(url, {
      json: data,
      timeout,
      agent: httpsAgent
    });
  } catch(error) {
    const {cause, httpStatusCode} = _processHTTPError(error);
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

exports.notifyPeer = async ({ledgerNodeId, remotePeer}) => {
  const {id: remotePeerId} = remotePeer;
  const url = `${remotePeer.url}/notify`;
  const {'ledger-consensus-continuity': {client: {timeout}}} = config;
  // the peerId sent to the remote peer is the peerId of the local peer
  // FIXME: send actual gossip URL instead of `localPeerId` twice
  const json = {peer: {id: localPeerId, url: localPeerId}};
  try {
    const headers = await _signRequest(
      {keyId: localPeerId, url, json, ledgerNodeId});
    await httpClient.post(url, {
      agent: httpsAgent,
      headers,
      json,
      timeout
    });
  } catch(error) {
    const {cause, httpStatusCode} = _processHTTPError(error);
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

exports.getValidationServiceUrl = ({localPeerId}) => {
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
};

function _pipelineComplete(/* err */) {
  // error is handled before this handler is called, this is a no-op
}

function _processHTTPError(error) {
  const {response} = error;
  const data = error.data || (response && response.data);
  let cause;
  let httpStatusCode;
  if(response && data && data.details) {
    // it's BedrockError
    httpStatusCode = response.status;
    cause = new BedrockError(
      data.message, data.type, data.details);
  } else if(response) {
    httpStatusCode = response.status;
    cause = new BedrockError('An HTTP error occurred.', 'NetworkError', {
      data,
      httpStatusCode,
    });
  } else if(!response) {
    // no status code available
    // NOTE: have not seen a ky error with port or address
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

async function _signRequest({keyId, url, json, ledgerNodeId}) {
  const created = Math.floor(Date.now() / 1000);
  const expires = created + 5 * 60;
  const includeHeaders = [
    '(created)',
    '(expires)',
    // (key-id) will include the peerId in the signature itself
    '(key-id)',
    '(request-target)',
    'digest',
    'content-type',
    'host'
  ];
  const headers = {
    digest: await createHeaderValue({data: json, useMultihash: true}),
    'content-type': 'application/json'
  };
  const plainText = createSignatureString({
    includeHeaders,
    headers,
    requestOptions: {created, expires, url, method: 'POST', headers, keyId}
  });
  const data = new TextEncoder().encode(plainText);
  const keyPair = await getKeyPair({ledgerNodeId});
  const sigBuffer = await keyPair.signer().sign({data});
  const signature = Buffer.from(
    sigBuffer, sigBuffer.offset, sigBuffer.length).toString('base64');
  const authorization = createAuthzHeader({
    created,
    expires,
    includeHeaders,
    keyId,
    signature
  });
  return {authorization, ...headers};
}
