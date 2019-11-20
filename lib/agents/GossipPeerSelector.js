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

  async getPeer({toNotify = false} = {}) {
    const {candidateMap, creatorId, ledgerNodeId} = this;

    // get priority peers to communicate with, removing self from list
    const peers = (await _cache.consensus.getPriorityPeers(
      {ledgerNodeId})).filter(p => p !== creatorId);

    const candidates = _.shuffle(peers);
    if(!toNotify) {
      // do not pull a notification off the stack if looking for a peer
      // to send a notification to
      const notification = await _cache.gossip.getGossipNotification(
        {ledgerNodeId});
      // put a notification at the head of the line
      if(notification) {
        candidates.unshift(notification);
      }
    }
    if(candidates.length === 0) {
      return;
    }
    let selected;
    const now = Date.now();
    for(const creatorId of candidates) {
      let gossipPeer = candidateMap.get(creatorId);
      if(!gossipPeer) {
        gossipPeer = new GossipPeer({creatorId, ledgerNodeId});
        candidateMap.set(creatorId, gossipPeer);
      }
      const {backoff, lastContactDate} =
        await gossipPeer.getStatus();
      if(toNotify && (lastContactDate + backoff < now)) {
        // coolDownPeriod does not apply to notifications
        selected = gossipPeer;
        break;
      }
      if(lastContactDate + backoff + coolDownPeriod < now) {
        selected = gossipPeer;
        break;
      }
    }
    return selected;
  }
};
