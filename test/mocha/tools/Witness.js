/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const consensusApi =
  require('bedrock-ledger-consensus-continuity/lib/consensus');
const helpers = require('./helpers');

class Witness {
  constructor({nodeId, pipeline, graph, witnesses}) {
    this.tickId = null;
    this.witnesses = witnesses;
    this.nodeId = nodeId;
    this.pipeline = pipeline;
    this.graph = graph;
    this.outstandingMergeEvents = [];
    this.activityLog = [];
    this.hasConsensus = false;
    this.totalMergeEventsCreated = 0;
    this.seenPeers = new Set();
  }

  async tick({id}) {
    this.tickId = id;
    await this.pipeline(this);
  }

  async run({type, fn} = {}) {
    const EXPECTED_TYPES = new Set(['merge', 'gossip', 'consensus']);
    if(!EXPECTED_TYPES.has(type)) {
      throw new Error('Unexpected type.');
    }

    let details;
    if(type !== 'consensus') {
      fn = fn.bind(this);
      details = await fn(this);
    } else {
      details = await this.findConsensus();
    }
    details = {...details, tick: this.tickId};

    const activity = {type, details, nodeId: this.nodeId};

    this.activityLog.push(activity);

    return details;
  }

  async findConsensus() {
    const consensusInput = await this.getConsensusInput();

    const timer = helpers.getTimer();
    const result = await consensusApi.findConsensus(consensusInput);
    const consensusDuration = timer.elapsed();

    const nodeHistory = await this.getHistory();

    if(result.consensus) {
      this.hasConsensus = true;
    }

    const report = {
      consensusDuration,
      totalMergeEventsCreated: this.totalMergeEventsCreated,
      totalMergeEvents: nodeHistory.events.length
    };

    Object.keys(report).forEach(metric => {
      console.log(`${metric}:`, report[metric]);
    });

    return {
      ...result,
      ...report
    };
  }

  async getConsensusInput() {
    const {nodeId: ledgerNodeId} = this;

    const consensusInput = {
      ledgerNodeId,
      history: await this.getHistory(),
      electors: this.graph.getElectors(),
      recoveryElectors: [],
      mode: 'first'
    };

    return consensusInput;
  }

  async merge({events = []} = {}) {
    const eventHash = await helpers.generateId();
    const {nodeId} = this;

    this.graph.mergeEvent({
      eventHash,
      to: nodeId,
      from: [nodeId, ...events]
    });
    this.totalMergeEventsCreated++;

    const history = await this.getHistory({includeOutstanding: false});
    const mergedEvents = history.events.map(({eventHash}) => eventHash);

    const outstandingEventsMap = new Map();
    this.outstandingMergeEvents.forEach(event => {
      outstandingEventsMap.set(event.eventHash, event);
    });

    mergedEvents.forEach(eventHash => {
      outstandingEventsMap.delete(eventHash);
    });

    this.outstandingMergeEvents = Array.from(outstandingEventsMap.values());
  }

  async getHead() {
    const history = await this.getHistory();
    return history.localBranchHead.eventHash;
  }

  async getLocalPeerHead(peer) {
    const {nodeId: peerNodeId} = peer;
    if(!this.seenPeers.has(peerNodeId)) {
      throw new Error(`NotFoundError: "Peer ${peerNodeId}" not seen locally ` +
        `by "Peer ${this.nodeId}"`);
    }
    const history = await this.getHistory();
    const peerHistory = await peer.getHistory();

    const events = history.events.map(({eventHash}) => eventHash);
    const peerEvents = peerHistory.events.map(({eventHash}) => eventHash);

    const peerEventsSeenLocally = _.intersection(events, peerEvents);
    const localPeerEvents = new Set(peerEventsSeenLocally);

    const localPeerBranch = this.graph.getBranch({nodeId: peerNodeId});
    const orderedLocalPeerEvents = [];
    localPeerBranch.forEach(eventHash => {
      if(localPeerEvents.has(eventHash)) {
        orderedLocalPeerEvents.push(eventHash);
      }
    });

    console.log('peerEvents:', peerEvents.length);
    console.log('localPeerEventsLength:', localPeerEvents.size);
    console.log('orderedLocalPeerEventsLength:', orderedLocalPeerEvents.length);
    // console.log('orderedLocalPeerEvents:', orderedLocalPeerEvents);

    return orderedLocalPeerEvents[orderedLocalPeerEvents.length - 1];
  }

  async getHistory({includeOutstanding = true} = {}) {
    const {nodeId} = this;
    const extraEvents = includeOutstanding ? this.outstandingMergeEvents : [];
    return this.graph.getHistory({nodeId, extraEvents});
  }

  async addEvents({events}) {
    // update seen peers
    events.forEach(({meta}) => this.seenPeers.add(meta.continuity2017.creator));

    // update outstanding merge events
    this.outstandingMergeEvents.push(...events);
  }

  async getEvents({events}) {
    const history = await this.getHistory();
    return history.events.filter(({eventHash}) => events.includes(eventHash));
  }

  async getPeers() {
    const peers = new Map(this.witnesses);
    peers.delete(this.nodeId);
    return Array.from(peers.values());
  }

  async getLocalPeers() {
    const peers = [];

    this.seenPeers.forEach(peerNodeId => {
      peers.push(this.witnesses.get(peerNodeId));
    });

    return peers;
  }
}

module.exports = Witness;
