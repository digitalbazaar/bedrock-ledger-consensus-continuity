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
const {validate} = require('bedrock-validation');
const {'ledger-consensus-continuity': {gossip: {maxEvents}}} = config;

const api = {};
module.exports = api;

api._getNeeded = _getNeeded;

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
    // peer.success will be recorded in gossip-agent after successful
    // gossip processing
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
  const events = [];

  // request events in chunks for size `maxEvents`
  const chunks = _.chunk(needed, maxEvents);
  for(const [i, eventHash] of chunks.entries()) {
    try {
      const stream = await _client.getEvents({eventHash, peerId});
      const readline = require('readline');
      const readInterface = readline.createInterface({
        input: stream,
        crlfDelay: Infinity
      });
      for await (const e of readInterface) {
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
        await new Promise((resolve, reject) => {
          _handleGossipError({error, reject});
        });
      }
      // no response was received or something happened in setting up the
      // request that triggered an Error
      const message = error.message || 'An HTTP Error occurred.';
      throw new BedrockError(message, 'NetworkError');
    }
  }
  // return the accumulated events
  return events;
}

function _handleGossipError({error, reject}) {
  // the error message from the server is returned as a stream
  let t;
  return error.response.data.on('data', res => {
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
      errorJson.details.httpStatusCode = error.response.status;
      errorJson.details.public = true;
      return reject(new BedrockError(
        errorJson.message, errorJson.type, errorJson.details,
        errorJson.cause));
    }
    return reject(new BedrockError(
      error.message || 'A network error occurred.',
      'NetworkError', {
        httpStatusCode: error.response.status,
      }, errorString));
  });
}
