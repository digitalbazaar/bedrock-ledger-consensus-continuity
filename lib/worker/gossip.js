/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _client = require('../client');
const _events = require('../events');
const _gossip = require('../gossip');
const bedrock = require('bedrock');
const {config} = bedrock;
const logger = require('../logger');

const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;

exports.runGossipCycle = async ({
  ledgerNode, priorityPeers, creatorId, peerSelector, mergePermits,
  needsGossip, halt
}) => {
  // attempt to use all merge permits given
  const startingPermits = mergePermits;
  let remainingPermits = startingPermits;
  let mergePermitsConsumed = 0;

  // get a set of peers to communicate with during this cycle
  const peers = await peerSelector.getPeers({priorityPeers});
  const priorityPeerSet = new Set(priorityPeers);
  while(peers.length > 0 && remainingPermits > 0 && !halt()) {
    const peer = peers.shift();

    // if we need gossip and the peer is not a priority peer or if the peer
    // is not recommended, skip it
    if((needsGossip && !priorityPeerSet.has(peer.creatorId)) ||
      !await peer.isRecommended()) {
      continue;
    }

    // use up to 50% of remaining merge permits on a non-priority peer
    const mergePermits = priorityPeerSet.has(peer.creatorId) ?
      remainingPermits : Math.ceil(remainingPermits / 2);

    // gossip with `peer`
    ({mergePermitsConsumed} = await _gw(
      {ledgerNode, mergePermits, creatorId, peer}));
    remainingPermits -= mergePermitsConsumed;
  }

  return {mergePermitsConsumed: startingPermits - remainingPermits};
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
  let mergePermitsConsumed = 0;
  let deferredEvents = [];
  if(result && result.events) {
    const {events, needed} = result;
    try {
      const blockHeight = await _cache.blocks.blockHeight(ledgerNode.id);

      ({deferredEvents, mergePermitsConsumed} = await _events.addBatch({
        blockHeight, events, ledgerNode, needed
      }));
    } catch(error) {
      logger.error(
        'An error occurred in gossip batch processing.', {error});
      result = {done: true, err: error};
    }
  }

  if(err && err.name !== 'TimeoutError') {
    await peer.fail(err);
  } else {
    let backoff = 0;
    if(result && result.done) {
      // no more gossip from the peer, add `coolDownPeriod` to backoff
      backoff = coolDownPeriod;
    }
    let detectedBlockHeight = 0;
    if(deferredEvents.length > 0) {
      [{requiredBlockHeight: detectedBlockHeight}] = deferredEvents;
    }
    await peer.success({backoff, detectedBlockHeight});
  }

  return {mergePermitsConsumed};
}
