/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _client = require('../client');
const _gossip = require('../gossip');
const _peerEvents = require('../peerEvents');
const bedrock = require('bedrock');
const {config} = bedrock;
const logger = require('../logger');

exports.runGossipCycle = async ({
  ledgerNode, priorityPeers, creatorId, peerSelector, needsGossip, halt
}) => {
  // always run the gossip cycle at least once, but continue running it
  // as long as gossip is needed and no merge events have been received
  let mergeEventsReceived = 0;
  // FIXME: use more than just the `priorityPeers`
  const priorityPeerSet = new Set(priorityPeers);
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
      const {mergePermitsReceived: received} = await _gw(
        {ledgerNode, creatorId, peer});
      mergeEventsReceived += received;
    }
  } while(!halt() && needsGossip && mergeEventsReceived === 0);

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

async function _gw({ledgerNode, creatorId, peer}) {
  let result;
  let err;

  try {
    result = await _gossip.gossipWith(
      {callerId: creatorId, ledgerNode, peer});
  } catch(e) {
    err = e;
    // if there is an error with one peer, do not stop cycle
    logger.debug('non-critical error in gossip', {err, peer});
  }

  // process any events acquired from peer
  let mergeEventsReceived = 0;
  let deferredEvents = [];
  if(result && result.events) {
    const {events, needed} = result;
    try {
      const blockHeight = await _cache.blocks.blockHeight(ledgerNode.id);

      ({deferredEvents, mergeEventsReceived} = await _peerEvents.addBatch({
        blockHeight, events, ledgerNode, needed
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
    let detectedBlockHeight = 0;
    if(deferredEvents.length > 0) {
      [{requiredBlockHeight: detectedBlockHeight}] = deferredEvents;
    }
    await peer.success({backoff, detectedBlockHeight});
  }

  return {mergeEventsReceived};
}
