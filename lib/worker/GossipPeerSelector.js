/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _peers = require('../peers');
const GossipPeer = require('./GossipPeer');

module.exports = class GossipPeerSelector {
  constructor({worker} = {}) {
    this.worker = worker;
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
        consensusState: {blockHeight: maxRequiredBlockHeight},
        localPeerId
      }
    } = this;
    const {records} = await _peers.getCached({ledgerNode});

    // only use peer if:
    // 1. Its required block height can be processed.
    // 2. It is not backed off.
    const filtered = [];
    const now = Date.now();
    for(const record of records) {
      const {peer} = record;
      if(peer.status.requiredBlockHeight <= maxRequiredBlockHeight &&
        peer.status.backoffUntil <= now) {
        filtered.push(record);
      }
    }

    // sort acceptable records by reputation
    const byReputation = records.slice().sort(_sortByHighestReputation);

    // include high rep peers that have notified recently, regardless of
    // last update; this ensures we always have a chance to gossip with the
    // most productive peers when the network is very busy
    const recentNotifiers = [];
    const high = [];
    const low = [];
    const middleIndex = Math.ceil(byReputation.length / 2);
    for(const record of byReputation) {
      // build sets of high/low reputation peer records
      if(high.length < middleIndex) {
        high.push(record);
        // add peer as a recent notifier if we haven't pulled since the last
        // push notification, up to 10 max
        const {meta} = record;
        if(recentNotifiers.length < 10 && !meta.pulledAfterPush) {
          recentNotifiers.push(record);
        }
      } else {
        low.push(record);
      }
    }

    // sort high and low peer records by least recently updated and limit
    // them to 10 a piece
    high.sort(_sortByLRU);
    low.sort(_sortByLRU);
    if(high.length > 10) {
      high.length = 10;
    }
    if(low.length > 10) {
      low.length = 10;
    }

    // build candidate maps and gather withheld peers
    const withheld = [];
    const highCandidateMap = this.highCandidateMap = new Map();
    const lowCandidateMap = this.lowCandidateMap = new Map();
    const allHigh = recentNotifiers.concat(high);
    for(const {peer} of allHigh) {
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
    for(const {peer} of low) {
      // skip local peer and peers also already added to high candidate map
      if(peer.id === localPeerId || highCandidateMap.has(peer.id)) {
        continue;
      }
      const gossipPeer = new GossipPeer({peer, worker});
      if(gossipPeer.isWithheld()) {
        withheld.push(gossipPeer.id);
      } else {
        lowCandidateMap.set(peer.id, gossipPeer);
      }
    }
  }

  async selectPullPeers() {
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
    // make up to 2 weighted selections
    const maxSelections = 2;
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

    return selections;
  }

  async selectNotifyPeers() {
    // prepare to select high and low reputation candidates
    let high = [];
    let low = [];
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

    // randomly choose one low rep peer and two high rep peers
    high = _.shuffle(high);
    low = _.shuffle(low);
    const selections = [];
    if(low.length > 0) {
      selections.push(low.pop());
    }
    if(high.length >= 2) {
      selections.push(high.pop());
      selections.push(high.pop());
    } else if(high.length > 0) {
      selections.push(high.pop());
    }
    // return selections
    return selections;
  }
};

function _sortByHighestReputation(a, b) {
  return a.peer.reputation - b.peer.reputation;
}

function _sortByLRU(a, b) {
  return a.meta.updated - b.meta.updated;
}
