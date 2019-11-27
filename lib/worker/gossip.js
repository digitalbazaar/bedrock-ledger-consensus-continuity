/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _client = require('../client');
const _events = require('../events');
const _gossip = require('../gossip');
const logger = require('../logger');

exports.runGossipCycle = async ({
  ledgerNode, priorityPeers, creatorId, peerSelector, mergePermits, halt
}) => {
  // use up to all but 1 merge permit, leaving it to create a local merge event
  const startingPermits = mergePermits - 1;
  let remainingPermits = startingPermits;
  let mergePermitsConsumed;

  // get a set of peers to communicate with during this cycle
  const peers = await peerSelector.getPeers({priorityPeers});
  while(peers.length > 0 && remainingPermits > 0 && !halt()) {
    const peer = peers.shift();

    // use up to 50% of remaining merge permits on a non-priority peer
    const mergePermits = priorityPeers.includes(peer.creatorId) ?
      remainingPermits : Math.ceil(remainingPermits / 2);

    // gossip with `peer`
    ({mergePermitsConsumed} = await _gw(
      {ledgerNode, mergePermits, creatorId, peer}));
    remainingPermits -= mergePermitsConsumed;
  }

  const used = startingPermits - remainingPermits;
  return {mergePermitsConsumed: mergePermits - used};
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

async function _gw({ledgerNode, mergePermits, creatorId, peer}) {
  let mergePermitsConsumed = 0;
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
  if(result && result.events) {
    const {events, needed} = result;
    try {
      // hash events and validate ancestry of all the events in the DAG
      const {eventMap} = await _events.prepBatch(
        {events, ledgerNode, needed});

      // add events
      const {eventBlock: {block: {blockHeight}}} =
        await ledgerNode.blocks.getLatestSummary();
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
    // TODO: do not fail peers exceeding maxRetries, but in the future
    // back-off peers that repeatedly fail max retries
    await peer.success();
  }

  return {mergePermitsConsumed};
}
