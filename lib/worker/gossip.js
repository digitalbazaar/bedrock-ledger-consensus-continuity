/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _client = require('../client');
const _history = require('../history');
const _peers = require('../peers');
const _peerEvents = require('../peerEvents');
const bedrock = require('bedrock');
const {config, util: {BedrockError}} = bedrock;
const logger = require('../logger');
const {pipeline} = require('stream');
const split2 = require('split2');
const {validate} = require('bedrock-validation');

exports.runGossipCycle = async ({worker, priorityPeers, needsGossip}) => {
  // always run the gossip cycle at least once, but continue running it
  // as long as gossip is needed and no merge events have been received
  let mergeEventsReceived = 0;
  // FIXME: use more than just the `priorityPeers`
  const priorityPeerSet = new Set(priorityPeers);
  const {peerSelector} = worker;
  do {
    // get a set of peers to communicate with during this cycle
    const peers = await peerSelector.getPeers({priorityPeers});
    if(peers.length === 0) {
      // FIXME: this should not happen
      // no peers to communicate with
      break;
    }
    for(const peer of peers) {
      // we must try to contact the peer if we need gossip and they are a
      // priority peer
      const mustContact = needsGossip && priorityPeerSet.has(peer.creatorId);

      // if we don't have to contact the peer and they are not recommended,
      // then skip communicating with them
      if(!mustContact && !await peer.isRecommended()) {
        continue;
      }

      // gossip with `peer`
      const {mergePermitsReceived: received} = await _gw({worker, peer});
      mergeEventsReceived += received;
    }
  } while(!worker.halt() && needsGossip && mergeEventsReceived === 0);

  return {mergeEventsReceived};
};

exports.sendNotification = async ({creatorId, priorityPeers, peerSelector}) => {
  let sent = 0;
  let attempts = 0;
  const maxRetries = 10;
  // attempt to send notifications to two distinct peers
  const peers = await peerSelector.getNotifyPeers({priorityPeers});
  while(peers.length > 0 && sent < 2 && attempts < maxRetries) {
    attempts++;
    const peer = peers.shift();
    if(!peer) {
      // either there are no peers, or they are all currently failed out
      break;
    }
    const {creatorId: peerId} = peer;
    try {
      await _client.notifyPeer({callerId: creatorId, peerId});
      // FIXME: need to track success/fail network requests separate from
      // success/fail related to gossip validation, for now, do not reset
      // the peer on a successful notification
      // await peer.success();
      sent++;
    } catch(e) {
      await peer.fail(e);
      peers.push(peer);
    }
  }
};

async function _gw({worker, peer}) {
  const {creatorId, ledgerNode} = worker;

  let result;
  let err;

  try {
    result = await _download({callerId: creatorId, ledgerNode, peer});
  } catch(e) {
    err = e;
    // if there is an error with one peer, do not stop cycle
    logger.debug('non-critical error in gossip', {err, peer});
  }

  // process any events acquired from peer
  let mergeEventsReceived = 0;
  if(result && result.events) {
    const {events, needed} = result;
    try {
      const blockHeight = await _cache.blocks.blockHeight(ledgerNode.id);

      ({mergeEventsReceived} = await _peerEvents.addBatch({
        worker, blockHeight, events, ledgerNode, needed
      }));
    } catch(error) {
      logger.error('An error occurred in gossip batch processing.', {error});
      result = {done: true, err: error};
    }
  }

  if(err && err.name !== 'TimeoutError') {
    await peer.fail(err);
  } else {
    let backoff = 0;
    if(result && result.done) {
      // no more gossip from the peer, add `coolDownPeriod` to backoff
      const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
      backoff = coolDownPeriod;
    }
    // FIXME: replace `detectedBlockHeight` with declared block height from
    // the peer
    const detectedBlockHeight = 0;
    await peer.success({backoff, detectedBlockHeight});
  }

  return {mergeEventsReceived};
}

async function _download({callerId, ledgerNode, peer}) {
  // communicate the very latest heads to the peer
  callerId = callerId || (await _peers.get({ledgerNodeId: ledgerNode.id})).id;
  const creator = {id: callerId};
  const {creatorId: peerId} = peer;
  logger.verbose('Start download', {peerId});
  const startTime = Date.now();
  try {
    const creatorHeads = await _history.getCreatorHeads(
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
      const timer = new _cache.Timer();
      timer.start({
        name: 'eventsDownloadDurationMs',
        ledgerNodeId: ledgerNode.id
      });
      events = await _getNeeded({needed, peerId});
      timer.stop();
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
    logger.verbose('End download', {
      duration: Date.now() - startTime
    });
  }
}

async function _diff({eventHashes, ledgerNode}) {
  // first check worker state
  // FIXME: add a method to `Worker` to check its `historyMap` instead of
  // using redis cache here
  /*let notFound = await _cache.events.difference(
    {eventHashes, ledgerNodeId: ledgerNode.id});
  if(notFound.length === 0) {
    return notFound;
  }*/
  let notFound = eventHashes;
  // ...of the events not found in the event queue (redis), return those that
  // are also not in storage (mongo), i.e. we haven't stored them at all
  notFound = await ledgerNode.storage.events.difference(notFound);
  if(notFound.length === 0) {
    return [];
  }
  // FIXME: is it still true we need to preserve this order? we should not
  // needless scramble it, of course, but check to see if order still matters
  // and if we can leverage that to increase security

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
  const {'ledger-consensus-continuity': {gossip: {maxEvents}}} = config;
  const chunks = _.chunk(needed, maxEvents);
  for(const eventHash of chunks) {
    try {
      const stream = await _client.getEvents({eventHash, peerId});
      const eventIterator = pipeline(
        stream,
        split2(),
        () => {}
      );
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
  if(needed.length !== events.length) {
    // we did not receive the events that what we needed
    throw new BedrockError(
      'The peer did not fulfill the request for needed events.',
      'DataError', {
        neededLength: needed.length,
        receivedLength: events.length,
        peerId,
      });
  }
  // return the accumulated events
  return events;
}
