/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const GossipPeer = require('./GossipPeer');

module.exports = class GossipPeerSelector {
  constructor({peerId, worker}) {
    // peerId for the local node
    this.peerId = peerId;
    this.worker = worker;
    this.ledgerNode = worker.ledgerNode;
    this.ledgerNodeId = worker.ledgerNode.id;
    this.candidateMap = new Map();
  }

  async selectPeers({peerIds} = {}) {
    // get peers to communicate with, removing self from list
    const {peerId, ledgerNodeId} = this;
    const candidates = _.shuffle(peerIds.filter(p => p !== peerId));

    // pull a notification off the queue and put it at the head of the line
    // FIXME: update notification to have more rich information
    const notification = await _cache.gossip.getGossipNotification(
      {ledgerNodeId});
    if(notification && !candidates.includes(notification)) {
      candidates.unshift(notification);
      // TODO: we need to be able to distinguish good peers from bad ones
      // ...so we can ignore `backoff` for good peers that have more to
      // ...say and not ignore it for bad peers... also we may still timeout
      // ...and never contact this peer meaning no one will get their
      // ...operations if they are not a priority peer; they should resend
      // ...notifications if no one has pulled from them
      const peer = this.getGossipPeer({peerId: notification});
      await peer.clearBackoff();
    }

    return candidates.map(peerId => this.getGossipPeer({peerId}));
  }

  async selectNotifyPeers({peerIds} = {}) {
    // get priority peers to notify, removing self from list
    const {peerId} = this;
    const candidates = _.shuffle(peerIds.filter(p => p !== peerId));
    return candidates.map(peerId => this.getGossipPeer({peerId}));
  }

  getGossipPeer({peerId} = {}) {
    const {candidateMap, worker} = this;
    let gossipPeer = candidateMap.get(peerId);
    if(!gossipPeer) {
      gossipPeer = new GossipPeer({id: peerId, worker});
      candidateMap.set(peerId, gossipPeer);
    }
    return gossipPeer;
  }
};
