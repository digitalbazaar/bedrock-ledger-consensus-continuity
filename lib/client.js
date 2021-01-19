/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const axios = require('axios');
const brHttpsAgent = require('bedrock-https-agent');
const delay = require('delay');
const {httpClient} = require('@digitalbazaar/http-client');
const bedrock = require('bedrock');
const {config, util: {BedrockError}} = bedrock;
const logger = require('./logger');
const LRU = require('lru-cache');
const {pipeline} = require('stream');
const split2 = require('split2');
const {validate} = require('bedrock-validation');
const {URL} = require('url');

let EVENT_VALIDATION_HTTPS_AGENT;
const EVENT_VALIDATION_URL_CACHE = new LRU({max: 10});

let OPERATION_VALIDATION_HTTPS_AGENT;
const OPERATION_VALIDATION_URL_CACHE = new LRU({max: 10});

bedrock.events.on('bedrock.ready', async () => {
  const {'ledger-consensus-continuity': {
    gossip: {eventValidation},
    operations: {validation: operationValidation},
  }} = config;

  if(eventValidation.httpsAgentOpts) {
    EVENT_VALIDATION_HTTPS_AGENT = new brHttpsAgent.HttpsAgent(
      eventValidation.httpsAgentOpts);
  } else {
    EVENT_VALIDATION_HTTPS_AGENT = brHttpsAgent.httpsAgent;
  }

  if(operationValidation.httpsAgentOpts) {
    OPERATION_VALIDATION_HTTPS_AGENT = new brHttpsAgent.HttpsAgent(
      operationValidation.httpsAgentOpts);
  } else {
    OPERATION_VALIDATION_HTTPS_AGENT = brHttpsAgent.httpsAgent;
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

exports.validateEvent = async (event,
  {ledgerNodeId, localPeerId, sessionId} = {}) => {
  const {'ledger-consensus-continuity': {
    gossip: {eventValidation}
  }} = config;

  const url = exports.getEventValidationServiceUrl({localPeerId});

  const data = {event, ledgerNodeId, sessionId};
  const response = await httpClient.post(url, {
    agent: EVENT_VALIDATION_HTTPS_AGENT,
    json: data,
    timeout: eventValidation.timeout
  });

  return response.data;
};

exports.validateOperation = async (operation, {
  ledgerNodeId, basisBlockHeight, localPeerId, validationServiceUrl, sessionId
} = {}) => {
  const {'ledger-consensus-continuity': {
    operations: {validation: operationValidation},
  }} = config;

  let url = validationServiceUrl;
  if(!validationServiceUrl) {
    url = exports.getOperationValidationServiceUrl({localPeerId});
  }

  const data = {operation, ledgerNodeId, basisBlockHeight, sessionId};
  const response = await httpClient.post(url, {
    agent: OPERATION_VALIDATION_HTTPS_AGENT,
    json: data,
    timeout: operationValidation.timeout
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

exports.getEventValidationServiceUrl = ({localPeerId}) => {
  const {'ledger-consensus-continuity': {
    gossip: {eventValidation}
  }} = config;

  const key = `${localPeerId}${eventValidation.baseUrl}`;

  let eventValidationUrl = EVENT_VALIDATION_URL_CACHE.get(key);
  if(eventValidationUrl) {
    return eventValidationUrl;
  }

  // event validation url is not set, use the node's localPeerId
  if(!eventValidation.baseUrl) {
    const {origin} = new URL(localPeerId);
    eventValidationUrl = origin + '/consensus/continuity2017/validation/events';
  } else {
    eventValidationUrl = eventValidation.baseUrl +
      `/consensus/continuity2017/validation/events`;
  }

  EVENT_VALIDATION_URL_CACHE.set(key, eventValidationUrl);

  return eventValidationUrl;
};

exports.getOperationValidationServiceUrl = ({localPeerId}) => {
  const {'ledger-consensus-continuity': {
    operations: {validation: operationValidation},
  }} = config;

  const key = `${localPeerId}${operationValidation.baseUrl}`;

  let operationValidationUrl = OPERATION_VALIDATION_URL_CACHE.get(key);
  if(operationValidationUrl) {
    return operationValidationUrl;
  }

  // operation validation url is not set, use the node's localPeerId
  if(!operationValidation.baseUrl) {
    const {origin} = new URL(localPeerId);
    operationValidationUrl = origin +
      '/consensus/continuity2017/validation/operations';
  } else {
    operationValidationUrl = operationValidation.baseUrl +
      `/consensus/continuity2017/validation/operations`;
  }

  OPERATION_VALIDATION_URL_CACHE.set(key, operationValidationUrl);

  return operationValidationUrl;
};

exports.validateTaskViaService = async ({
  task, taskOpts, totalTaskCount, sharedState, halt, validate,
  getValidationServiceUrl
}) => {
  const {localPeerId} = taskOpts;
  const {pendingValidations} = sharedState;

  // keep trying to validate task when timeouts occur until work session halts
  while(!halt()) {
    // if the number of pending validations has reached the high water mark,
    // then wait for the validation service to be less busy
    if(pendingValidations.size >= sharedState.highWaterMark) {
      await _waitForValidationService({sharedState});
      continue;
    }

    let promise;
    try {
      // try to validate task
      promise = validate(task, taskOpts);

      // await validation and do pending validation set management
      pendingValidations.add(promise);
      const result = await promise;
      pendingValidations.delete(promise);

      /* Since validation was successful, increment the high water mark to test
      if more CPU is now available. It is expected that this will, at most,
      allow two additional concurrent validation requests to be made. One can
      be made in place of the request that just successfully finished here, and
      another via the increment.

      In theory, if the CPU load hasn't changed, one will be successful and the
      other will timeout and reduce the high water mark again. If the CPU load
      has increased, both will timeout but the service will only be burdened by
      one additional request; after which the high water mark will be reduced
      again. If the CPU load has decreased, both will be successful and each
      will allow one extra request to be tried. Each subsequent time more CPU
      is available, 2x additional requests will make requests, but this
      doubling will reset as soon as the high water mark is reduced again. */
      sharedState.highWaterMark++;
      return result;
    } catch(e) {
      // remove failed validation from pending set
      if(promise) {
        pendingValidations.delete(promise);
      }
      // if validation can't be retried, throw error; will cause the whole
      // batch to be thrown out
      if(!_canRetryValidation(e)) {
        e.validationServiceUrl = await getValidationServiceUrl({localPeerId});
        throw e;
      }
      logger.verbose(
        'A non-critical error occurred while communicating with the ' +
        'validation service.', {error: e});
      // allow looping to retry validation, but reduce the high water mark
      // that is shared across all validation tasks to the number of pending
      // validations; this number represents the most validations that are
      // currently possible given the CPU load... we allow this number to grow
      // elsewhere
      if(pendingValidations.size < sharedState.highWaterMark) {
        sharedState.highWaterMark = pendingValidations.size;
      }
    }
  }

  // did not get a result before session halted, throw timeout error
  throw new BedrockError(
    'Timed out while validating tasks.',
    'TimeoutError', {
      httpStatusCode: 503,
      public: true,
      localPeerId,
      totalTaskCount
    });
};

async function _waitForValidationService({sharedState}) {
  const {pendingValidations} = sharedState;
  if(pendingValidations.size === 0) {
    // if there are no pending validations, wait for a second before retry;
    // here we give the service a full second to try and free up its CPU,
    // it's so busy it can't validate even a single task
    await delay(1000);
    // reset the high water mark to allow maximum validations again
    sharedState.highWaterMark = Infinity;
    return;
  }

  // wait for any validation to settle, whether resolved or rejected
  try {
    await Promise.race([...pendingValidations]);
  } catch(e) {
    // do not throw there, we only wanted to wait to get on the queue
  }
}

function _canRetryValidation(e) {
  // can ignore connection reset, service unavailable, bad gateway, and timeout
  // errors; these types of errors are all non-critical errors related to the
  // validation service being too busy or timing out
  const errorCodes = [502, 503];
  return (
    (e.name === 'FetchError' &&
      (e.code === 'ECONNRESET' || e.code === 'EPIPE')) ||
    (e.name === 'HTTPError' && e.response &&
      errorCodes.includes(e.response.status)) ||
    e.name === 'TimeoutError');
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
    cause = new BedrockError('An HTTP error occurred.', 'NetworkError', {
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
