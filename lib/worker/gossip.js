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
  logger.verbose('Start gossip cycle...');
  while(peers.length > 0 && remainingPermits > 0 && !halt()) {
    const peer = peers.shift();

    // if we don't need gossip or the peer isn't a priority peer, then don't
    // contact the peer if not recommended
    logger.verbose('Check if peer needs to be contacted...');
    if(!(needsGossip && priorityPeerSet.has(peer.creatorId)) &&
      !await peer.isRecommended()) {
      logger.verbose('Peer not conctacted.');
      continue;
    }

    // use up to 50% of remaining merge permits on a non-priority peer
    const mergePermits = priorityPeerSet.has(peer.creatorId) ?
      remainingPermits : Math.ceil(remainingPermits / 2);

    // gossip with `peer`
    logger.verbose('Gossip with peer....', {peer});
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
  logger.verbose('Attempt to send notifications to two distinct peers....');
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
      logger.verbose('Notify peer...', {callerId: creatorId, peerId});
      await _client.notifyPeer({callerId: creatorId, peerId});
      // FIXME: need to track success/fail network requests separate from
      // success/fail related to gossip validation, for now, do not reset
      // the peer on a successful notification
      // await peer.success();
      sent++;
    } catch(e) {
      logger.verbose('Peer notifcation failed...', {error: e});
      await peer.fail(e);
      peers.push(peer);
    }
  }
};

async function _gw({ledgerNode, mergePermits, creatorId, peer}) {
  let mergePermitsConsumed = 0;
  let result;
  let err;

  logger.verbose('Start gossip with...');
  try {
    result = await _gossip.gossipWith(
      {callerId: creatorId, ledgerNode, peer});
    logger.verbose('Received result from gossip with...');
  } catch(e) {
    err = e;
    // if there is an error with one peer, do not stop cycle
    logger.debug('non-critical error in gossip', {err, peer});
  }

  // process any events acquired from peer
  if(result && result.events) {
    const {events, needed} = result;
    try {
      // hash events and validate ancestry of all the events in the DAG
      logger.verbose('Prep event batch...');

      const {eventMap} = await _events.prepBatch(
        {events, ledgerNode, needed});

      // add events
      logger.verbose('Add events...');
      const blockHeight = await _cache.blocks.blockHeight(ledgerNode.id);
      ({/*deferredEvents,*/mergePermitsConsumed} =
        await _events.addBatch({
          blockHeight, events, eventMap, ledgerNode, mergePermits
        }));
      // TODO: cache `deferredEvents` and `processedEventHashes` for
      // next cycle so they don't get wasted
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
    await peer.success({backoff});
  }

  logger.verbose('Merge permits consumed', {mergePermitsConsumed});
  return {mergePermitsConsumed};
}
