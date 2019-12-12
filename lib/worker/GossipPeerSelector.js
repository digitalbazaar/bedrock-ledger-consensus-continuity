/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const GossipPeer = require('./GossipPeer');

module.exports = class GossipPeerSelector {
  constructor({creatorId, ledgerNode}) {
    // creatorId / voterId for the local node
    this.creatorId = creatorId;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.candidateMap = new Map();
  }

  async getPeers({priorityPeers}) {
    // get priority peers to communicate with, removing self from list
    const {creatorId, ledgerNodeId} = this;
    const candidates = _.shuffle(priorityPeers.filter(p => p !== creatorId));

    // pull a notification off the queue and put it at the head of the line
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
      const peer = this.getGossipPeer({creatorId: notification});
      peer.clearBackoff();
    }

    return candidates.map(creatorId => this.getGossipPeer({creatorId}));
  }

  async getNotifyPeers({priorityPeers}) {
    // get priority peers to notify, removing self from list
    const {creatorId} = this;
    const candidates = _.shuffle(priorityPeers.filter(p => p !== creatorId));
    return candidates.map(creatorId => this.getGossipPeer({creatorId}));
  }

  getGossipPeer({creatorId}) {
    const {candidateMap, ledgerNodeId} = this;
    let gossipPeer = candidateMap.get(creatorId);
    if(!gossipPeer) {
      gossipPeer = new GossipPeer({creatorId, ledgerNodeId});
      candidateMap.set(creatorId, gossipPeer);
    }
    return gossipPeer;
  }
};
