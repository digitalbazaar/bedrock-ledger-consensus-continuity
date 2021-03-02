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
    this.highCandidates = new Map();
    this.lowCandidates = new Map();
  }

  async refreshCandidates() {
    // fetch:
    // 1. most reputable peers that notified recently
    // 2. most least recently updated peers with reputations above 50
    // 3. most least recently updated peers with reputations below 50
    const {
      worker,
      worker: {
        ledgerNode,
        consensusState: {blockHeight: localBlockHeight},
        localPeerId
      }
    } = this;
    const [recentNotifiers, high, low] = await Promise.all([
      // high rep peers that have notified recently, regardless of last update;
      // this ensures we always have a chance to gossip with the most
      // productive peers when the network is very busy
      ledgerNode.peers.getAll({
        localBlockHeight, backoffUntil: Date.now(),
        sortReputation: -1, sortLastPushAt: -1,
        limit: 10
      }),
      // high rep peers
      ledgerNode.peers.getLRU({localBlockHeight, minReputation: 50, limit: 10}),
      // low rep peers
      ledgerNode.peers.getLRU({maxReputation: 49, localBlockHeight, limit: 10})
    ]);

    // build candidate maps and gather withheld peers
    const withheld = [];
    const highCandidateMap = this.highCandidateMap = new Map();
    const lowCandidateMap = this.lowCandidateMap = new Map();
    high.push(...recentNotifiers);
    for(const peer of high) {
      // the local peer should never be retrievable via `ledgerNode.peers`,
      // but in case it does, skip it here
      if(peer.id === localPeerId) {
        continue;
      }
      const gossipPeer = new GossipPeer({peer, worker});
      if(gossipPeer.isWithheld()) {
        // do not contact withheld peers, but track them so they can be marked
        // updated because they'd been "seen" recently but can't be contacted
        withheld.push(gossipPeer.id);
      } else {
        highCandidateMap.set(peer.id, gossipPeer);
      }
    }
    for(const peer of low) {
      if(peer.id === localPeerId) {
        continue;
      }
      const gossipPeer = new GossipPeer({peer, worker});
      if(gossipPeer.isWithheld()) {
        withheld.push(gossipPeer.id);
      } else {
        lowCandidateMap.set(peer.id, gossipPeer);
      }
    }

    // update all withheld peers so they won't be fetched next time using
    // `getLRU`
    await ledgerNode.peers.markUpdated({ids: withheld});
  }

  async selectPeers({peerIds} = {}) {
    // Create randomized sets to select from:
    // 1. high reputation peers that have notified
    // 2. low reputation peers that have notified
    // 3. high reputation peers
    // 4. low reputation peers
    const highNotifiers = [];
    const lowNotifiers = [];
    const high = [];
    const low = [];
    const {highCandidateMap, lowCandidateMap} = this;
    for(const gossipPeer of highCandidateMap.values()) {
      /* Note: We need to skip withheld peers. We can't store that they are
      withheld in the database because this status is not persistent, so they
      haven't been filtered out here. Peers may be withheld up to the length of
      a work session afterwhich they will be released with the exception of up
      to one peer, if the local peer is in the middle of committing to it,
      after which it will be released. */
      if(gossipPeer.isWithheld() || gossipPeer.isDeleted()) {
        continue;
      }
      if(gossipPeer.isNotifier()) {
        highNotifiers.push(gossipPeer);
      } else {
        high.push(gossipPeer);
      }
    }
    for(const gossipPeer of lowCandidateMap.values()) {
      if(gossipPeer.isWithheld() || gossipPeer.isDeleted()) {
        continue;
      }
      if(gossipPeer.isNotifier()) {
        lowNotifiers.push(gossipPeer);
      } else {
        low.push(gossipPeer);
      }
    }

    // randomly shuffle groups and prep for weighted selection
    // FIXME: make percentages configurable
    const weights = [0.65, 0.20, 0.10, 0.05];
    const groups = [
      // select 65% of the time
      _.shuffle(highNotifiers),
      // select 20% of the time
      _.shuffle(lowNotifiers),
      // select 10% of the time
      _.shuffle(high),
      // select 5% of the time
      _.shuffle(low)
    ];
    // make up to 3 weighted selections
    const maxSelections = 3;
    const selections = [];
    for(let i = 0; i < maxSelections; ++i) {
      const r = Math.random();
      let groupIndex = 0;
      let totalWeight = 0;
      for(const weight of weights) {
        totalWeight += weight;
        if(r < totalWeight) {
          break;
        }
        groupIndex++;
      }
      // ensure group is non-empty, using a fallback by cycling the groups
      let group = groups[groupIndex];
      for(let j = 0; group.length === 0 && j < (groups.length - 1); ++j) {
        groupIndex = (groupIndex + 1) % groups.length;
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

    // FIXME: remove this, notification queue no longer used
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
      //const peer = this.getGossipPeer({peerId: notification});
      //await peer.clearBackoff();
    }

    return candidates.map(peerId => this.getGossipPeer({peerId}));
  }

  async selectNotifyPeers({peerIds} = {}) {
    // prepare to select high and low reputation candidates
    const high = [];
    const low = [];
    const {highCandidateMap, lowCandidateMap} = this;
    for(const gossipPeer of highCandidateMap.values()) {
      // skip any withheld peers
      if(gossipPeer.isWithheld()) {
        continue;
      }
      high.push(gossipPeer);
    }
    for(const gossipPeer of lowCandidateMap.values()) {
      // skip any withheld peers
      if(gossipPeer.isWithheld()) {
        continue;
      }
      low.push(gossipPeer);
    }

    // randomly choose one low rep peer and all high rep peers
    const selections = [];
    if(low.length > 0) {
      selections.push(_.shuffle(low).pop());
    }
    if(high.length > 0) {
      selections.push(..._.shuffle(high));
    }
    // FIXME: return selections

    // get priority peers to notify, removing self from list
    const {peerId} = this;
    const candidates = _.shuffle(peerIds.filter(p => p !== peerId));
    return candidates.map(peerId => this.getGossipPeer({peerId}));
  }

  // FIXME: consider eliminating this method entirely
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
