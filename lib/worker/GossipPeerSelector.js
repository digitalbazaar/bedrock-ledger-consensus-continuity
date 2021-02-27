/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const GossipPeer = require('./GossipPeer');

module.exports = class GossipPeerSelector {
  constructor({worker} = {}) {
    this.worker = worker;
    // FIXME: consolidate to `candidateMap`
    this.candidateMap = new Map();
    this._candidateMap = new Map();
  }

  async refreshCandidates() {
    // fetch recommended peers and least recently updated other peers
    const {
      worker,
      worker: {
        ledgerNode,
        consensusState: {blockHeight: localBlockHeight},
        localPeerId
      }
    } = this;
    const [recommended, lru] = await Promise.all([
      ledgerNode.peers.getRecommended(),
      ledgerNode.peers.getLRU({recommended: false, localBlockHeight, limit: 10})
    ]);

    // build candidate map
    const candidateMap = this._candidateMap = new Map();
    for(const peer of recommended) {
      // the local peer should never be retrievable via `ledgerNode.peers`,
      // but in case it does, skip it here
      if(peer.id === localPeerId) {
        continue;
      }
      candidateMap.set(peer.id, new GossipPeer({peer, worker}));
    }
    for(const peer of lru) {
      // the local peer should never be retrievable via `ledgerNode.peers`,
      // but in case it does, skip it here
      if(peer.id === localPeerId) {
        continue;
      }
      candidateMap.set(peer.id, new GossipPeer({peer, worker}));
    }
  }

  async selectPeers({peerIds} = {}) {
    // Create randomized sets to select from:
    // 1. recommended peers that have notified
    // 2. recommended peers
    // 3. non-recommended LRU peers
    const recommendedNotifiers = [];
    const recommended = [];
    const lru = [];
    // FIXME: change to `candidateMap`
    const {_candidateMap} = this;
    for(const gossipPeer of _candidateMap.values()) {
      // skip any withheld peers
      if(gossipPeer.isWithheld()) {
        continue;
      }

      // change to `isRecommended` once it is renamed
      if(gossipPeer._isRecommended()) {
        if(gossipPeer.isNotifier()) {
          recommendedNotifiers.push(gossipPeer);
        } else {
          recommended.push(gossipPeer);
        }
      } else {
        lru.push(gossipPeer);
      }
    }

    // randomly shuffle groups and prep for weighted selection
    const groups = [
      // select 60% of the time
      _.shuffle(recommendedNotifiers),
      // select 20% of the time
      _.shuffle(recommended),
      // select 20% of the time
      _.shuffle(lru)
    ];
    // make up to 3 weighted selections
    const selections = [];
    for(let i = 0; i < 2; ++i) {
      const r = Math.random();
      let groupIndex = 0;
      if(r >= 0.6) {
        groupIndex++;
      }
      if(r >= 0.8) {
        groupIndex++;
      }
      // ensure group is non-empty, using a fallback by cycling the groups
      let group = groups[groupIndex];
      for(let j = 0; group.length === 0 && j < 2; ++j) {
        groupIndex = (groupIndex + 1) % 3;
        group = groups[groupIndex];
      }
      if(group.length === 0) {
        break;
      }
      selections.push(group.pop());
    }
    // FIXME: enable
    //return selections;

    // FIXME: old selection code below
    // get peers to communicate with, removing self from list
    const {worker: {localPeerId, ledgerNodeId}} = this;
    const candidates = _.shuffle(peerIds.filter(p => p !== localPeerId));

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
    // prepare candidates from one non-recommended LRU peer and all
    // recommended peers
    const recommended = [];
    const lru = [];
    // FIXME: rename to `candidateMap`
    const {_candidateMap} = this;
    for(const gossipPeer of _candidateMap.values()) {
      // skip any withheld peers
      if(gossipPeer.isWithheld()) {
        continue;
      }

      // change to `isRecommended` once it is renamed
      if(gossipPeer._isRecommended()) {
        recommended.push(gossipPeer);
      } else {
        lru.push(gossipPeer);
      }
    }

    // randomly choose one LRU peer and all recommended peers
    const selections = [];
    if(lru.length > 0) {
      selections.push(_.shuffle(lru).pop());
    }
    if(recommended.length > 0) {
      selections.push(..._.shuffle(recommended));
    }
    // FIXME: return selections

    // get priority peers to notify, removing self from list
    const {peerId} = this;
    const candidates = _.shuffle(peerIds.filter(p => p !== peerId));
    return candidates.map(peerId => this.getGossipPeer({peerId}));
  }

  getGossipPeer({peerId} = {}) {
    const {candidateMap, worker} = this;
    let gossipPeer = candidateMap.get(peerId);
    if(!gossipPeer) {
      // FIXME: limit the map size; switch to LRU cache
      gossipPeer = new GossipPeer({id: peerId, worker});
      candidateMap.set(peerId, gossipPeer);
    }
    return gossipPeer;
  }
};
