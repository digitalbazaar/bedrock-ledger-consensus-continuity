/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _client = require('../client');
const _history = require('../history');
const _peerEvents = require('../peerEvents');
const bedrock = require('bedrock');
const {config} = bedrock;
const logger = require('../logger');

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
  let mergeEventsReceived = 0;

  // get needed events from server
  let result;
  try {
    const {creatorId: peerId} = peer;
    result = await _getNeeded({worker, peerId});
  } catch(e) {
    // do not throw; an error communicating with a peer does not prevent a
    // critical change in state, so it should not terminate the work session
    logger.debug(
      'A non-critical error occurred during gossip', {peer, error: e});

    if(e.name !== 'TimeoutError') {
      // record non-timeout errors as gossip session failures
      await peer.fail(e);
      return {mergeEventsReceived};
    }

    // normalize a timeout to a gossip session that produced no events and
    // should result in a backoff
    result = {events: [], needed: [], done: true};
  }

  // at this point, download was successful (or timed out with no events),
  // but we need to process what was downloaded next...
  const {events, needed} = result;
  if(events.length > 0) {
    // try to add the batch of events received (this will validate the events
    // and the operations therein)
    const batchResult = await _peerEvents.addBatch({worker, events, needed});
    const {valid, error} = batchResult;
    if(!valid) {
      // record gossip session as a failure due to invalid events
      logger.debug(
        'A non-critical error occurred during gossip batch processing.',
        {peer, error});
      await peer.fail(error);
      return {mergeEventsReceived};
    }
    // update merge events received
    ({mergeEventsReceived} = batchResult);
  }

  // consider gossip session a success
  let backoff = 0;
  if(result.done) {
    // no more gossip from the peer, add `coolDownPeriod` to backoff
    const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
    backoff = coolDownPeriod;
  }
  // FIXME: replace `detectedBlockHeight` with declared block height from
  // the peer
  const detectedBlockHeight = 0;
  await peer.success({backoff, detectedBlockHeight});
  return {mergeEventsReceived};
}

async function _getNeeded({worker, peerId}) {
  const {creatorId: callerId, ledgerNode} = worker;

  // communicate the latest block height and any creator heads beyond it to
  // the peer
  logger.verbose('Start _getNeeded', {peerId});
  const startTime = Date.now();
  try {
    // FIXME: only get heads for creators that occur after a given
    // block height
    const {heads: creatorHeads} = await _history.getCreatorHeads(
      {latest: true, ledgerNode, localCreatorId: callerId, peerId});

    // FIXME: send block height to server
    const history = await _client.getHistory({
      callerId, creatorHeads, peerId});

    const {history: eventHashes, truncated} = history;
    if(eventHashes.length === 0) {
      // peer has nothing to share
      return {events: [], needed: [], done: true};
    }

    // check to see what's needed from the peer by diffing with the cache
    // and local storage
    const needed = await _diff({eventHashes, ledgerNode});
    if(needed.length === 0) {
      // we already have what we need from other peers, so no need to download
      // any events
      return {events: [], needed, done: true};
    }

    let events;
    try {
      const timer = new _cache.Timer();
      timer.start({
        name: 'eventsDownloadDurationMs',
        ledgerNodeId: ledgerNode.id
      });
      events = await _client.getEvents({eventHashes: needed, peerId});
      timer.stop();
    } catch(e) {
      if(e.details && e.details.httpStatusCode === 404) {
        // peer has nothing to share
        return {events: [], needed: [], done: true};
      }
      throw e;
    }
    return {events, needed, done: !truncated};
  } catch(e) {
    throw e;
  } finally {
    logger.verbose('End _getNeeded', {duration: Date.now() - startTime});
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
