/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const bedrock = require('bedrock');
const {config} = bedrock;
const GossipPeer = require('./GossipPeer');

const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;

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
    }

    return this._filterPeers({candidates, priorityPeers, coolDownPeriod});
  }

  async getNotifyPeers({priorityPeers}) {
    // get priority peers to notify, removing self from list
    const {creatorId} = this;
    const candidates = _.shuffle(priorityPeers.filter(p => p !== creatorId));
    if(candidates.length === 0) {
      // no one to notify
      return;
    }

    // Note: `coolDownPeriod` does not apply to notifications
    return this._filterPeers({candidates, priorityPeers, coolDownPeriod: 0});
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

  async _filterPeers({candidates, priorityPeers, coolDownPeriod = 0}) {
    // filter set of peers to notify by earliest next contact
    const filtered = [];
    const now = Date.now();
    for(const creatorId of candidates) {
      const gossipPeer = this.getGossipPeer({creatorId});
      const {backoff, lastContactDate} = await gossipPeer.getStatus();
      let earliestNextContact = lastContactDate + backoff;
      if(!priorityPeers.includes(creatorId)) {
        // TODO: need to be smarter about cool down period
        earliestNextContact += coolDownPeriod;
      }
      if(earliestNextContact < now) {
        filtered.push(gossipPeer);
      }
    }
    return filtered;
  }
};
