/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _client = require('./client');
const _events = require('./events');
const _voters = require('./voters');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {config, util: {BedrockError}} = bedrock;
const logger = require('./logger');
const ndjson = require('ndjson');
const {validate} = require('bedrock-validation');
const {'ledger-consensus-continuity': {gossip: {maxEvents}}} = config;

const api = {};
module.exports = api;

api.GossipPeer = require('./agents/GossipPeer');

// communicate the very latest heads to the peer
api.gossipWith = async ({callerId, ledgerNode, peer}) => {
  callerId = callerId || (await _voters.get({ledgerNodeId: ledgerNode.id})).id;
  const creator = {id: callerId};
  const {creatorId: peerId} = peer;
  logger.verbose('Start gossipWith', {peerId});
  const startTime = Date.now();
  try {
    const creatorHeads = await _events.getCreatorHeads(
      {latest: true, ledgerNode, localCreatorId: callerId, peerId});

    const history = await _client.getHistory({
      callerId,
      creatorHeads: creatorHeads.heads,
      peerId
    });

    const {history: eventHashes, truncated} = history;
    if(eventHashes.length === 0) {
      // peer has nothing to share
      await peer.success();
      return {creator, creatorHeads, history, done: true};
    }

    // check to see what's needed from the peer by diffing with the cache
    // and local storage
    const needed = await _diff({eventHashes, ledgerNode});
    if(needed.length === 0) {
      // we already have what we need from other peers, so no need to gossip
      await peer.success();
      return {creator, creatorHeads, history, done: true};
    }

    let events;
    try {
      events = await _getNeeded({needed, peerId});
    } catch(e) {
      if(_.get(e, 'details.httpStatusCode') === 404) {
        // peer has nothing to share
        await peer.success();
        return {creator, creatorHeads, history, done: true};
      }
      throw e;
    }
    await peer.success();
    return {creator, creatorHeads, events, history, needed, done: !truncated};
  } catch(e) {
    await peer.fail(e);
    throw e;
  } finally {
    logger.verbose('End gossipWith', {
      duration: Date.now() - startTime
    });
  }
};

// used in server to only get current heads
api.getHeads = async ({creatorId}) => {
  const ledgerNodeId = await _voters.getLedgerNodeId(creatorId);
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
  const {heads} = await _events.getCreatorHeads({ledgerNode});
  return heads;
};

async function _diff({eventHashes, ledgerNode}) {
  const notFound = await _events.difference({eventHashes, ledgerNode});
  if(notFound.length === 0) {
    return [];
  }
  // the order of eventHashes must be preserved
  const needSet = new Set(notFound);
  return eventHashes.filter(h => needSet.has(h));
}

async function _getNeeded({needed, peerId}) {
  /* Note: Here we connect to the peer and download all needed events. The
  events currently must be added in order (TODO: handle ordering later and
  just put them into the cache as quickly as possible to optimize).

  If an error occurs with adding the events (since they must be added
  in order), we should terminate the connection to the peer immediately and
  bail out. */
  let addEventError;
  let peerError;
  const events = [];

  // request events in chunks for size `maxEvents`
  const chunks = _.chunk(needed, maxEvents);
  for(const [i, eventHash] of chunks.entries()) {
    let streamDone = false;
    await new Promise(async (resolve, reject) => {
      // connect to peer and get event stream...
      let stream;
      try {
        stream = await _client.getEvents({eventHash, peerId});
      } catch(error) {
        if(error.response) {
          // The request was made and the server responded with a status code
          // that falls out of the range of 2xx
          return _handleGossipError({reject, response: error.response});
        }
        // no response was received or something happened in setting up the
        // request that triggered an Error
        const message = error.message || 'An HTTP Error occurred.';
        return reject(new BedrockError(message, 'NetworkError'));
      }
      stream
        .on('response', res => {
          const {body, statusCode} = res;
          if(statusCode !== 200) {
            // set `peerError` because `error` handler will be called and
            // it should not do anything
            peerError = new BedrockError(
              'An error occurred contacting a peer.', 'NetworkError',
              {body, needed, peerId, statusCode});
            stream.destroy();
            // we couldn't have queued any events yet, so reject
            reject(peerError);
          }
        })

        // TODO: *might* be worth saving the stringified version of the incoming
        // event which would save a `stringify` operation in the case where
        // the event validates successfully. Just a matter of associating
        // the raw data coming in now with the eventHash calculated later.
        .pipe(ndjson.parse())
        .on('data', data => {
          // if an add event error previously occurred then we can't queue
          // another event -- and other code will handle destroying stream
          if(addEventError) {
            return;
          }

          if(!(data && data.event)) {
            stream.destroy();
            logger.debug('MALFORMED EVENT', {data});
            peerError = new BedrockError(
              'Malformed event detected during gossip.',
              'AbortError', {data, peerId});
            // if queue is idle then nothing else will trigger reject so we must
            return reject(peerError);
          }
          const result = validate('continuity.webLedgerEvents', data.event);
          if(!result.valid) {
            addEventError = result.error;
            if(!streamDone) {
              // there is no reason to continue receiving events since they
              // must be added in order and we've hit an error adding this one
              // so destroy the stream
              stream.destroy();
            }
            return reject(addEventError);
          }
          events.push(data.event);
        })
        .on('end', () => {
          // track that stream is done so we know when to `resolve()` later or
          // that we don't need to destroy it should an add event error occur
          streamDone = true;
          // if an add event error previously occurred then other code will
          // handle destroying the stream
          if(addEventError) {
            return;
          }

          // not on the last chunk or
          // last chunk and the correct number of events have been received
          if(i !== chunks.length - 1 || events.length === needed.length) {
            return resolve();
          }
          reject(new BedrockError(
            'The peer did not provide all the requested events.',
            'AbortError', {needed, peerId}));
        })
        .on('error', err => {
          if(peerError || addEventError) {
            // nothing to do, previous error already occurred
            return;
          }
          peerError = err;
          reject(peerError);
        });
    });
  }
  // return the accumulated events
  return events;
}

function _handleGossipError({reject, response}) {
  // the error message from the server is returned as a stream
  let t;
  return response.data.on('data', res => {
    if(!t) {
      return t = res;
    }
    t = Buffer.concat([t, res]);
  }).on('end', () => {
    // res is a Buffer
    const errorString = t.toString();
    let errorJson;
    try {
      errorJson = JSON.parse(errorString);
    } catch(e) {
      // error is not JSON
    }
    // error is a BedrockError
    if(errorJson && errorJson.message && errorJson.type) {
      errorJson.details.httpStatusCode = response.status;
      errorJson.details.public = true;
      return reject(new BedrockError(
        errorJson.message, errorJson.type, errorJson.details,
        errorJson.cause));
    }
    return reject(new Error(errorString));
  });
}
