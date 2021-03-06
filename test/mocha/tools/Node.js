/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const continuityApi =
  require('bedrock-ledger-consensus-continuity/lib/continuity');
const helpers = require('./helpers');

const INITIAL_CONSENSUS_STATE = {
  init: false,
  eventMap: new Map(),
  blockHeight: -1,
  hashToMemo: new Map(),
  symbolToMemo: new Map(),
  supportCache: new Map()
};

class Node {
  constructor({nodeId, pipeline, graph, nodes, witnesses, isWitness}) {
    this.tickId = null;
    this.nodes = nodes;
    this.witnesses = witnesses;
    this.nodeId = nodeId;
    this.isWitness = isWitness;
    this.pipeline = pipeline;
    this.graph = graph;
    this.outstandingMergeEvents = [];
    this.activityLog = [];
    this.consensusState = {...INITIAL_CONSENSUS_STATE};
    this.hasConsensus = false;
    this.totalMergeEventsCreated = 0;
    this.seenPeers = new Set();
  }

  async tick({id}) {
    this.tickId = id;
    await this.pipeline(this);
  }

  async run({type, fn, args} = {}) {
    const EXPECTED_TYPES = new Set(['merge', 'gossip', 'consensus']);
    if(!EXPECTED_TYPES.has(type)) {
      throw new Error('Unexpected type.');
    }

    let details;
    if(type !== 'consensus') {
      fn = fn.bind(this);
      details = await fn(args);
    } else {
      details = await this.findConsensus();
    }
    details = {...details, tick: this.tickId};

    if(Array.isArray(details)) {
      details.forEach(detail => {
        const activity = {type, details: detail, nodeId: this.nodeId};
        this.activityLog.push(activity);
      });
    } else {
      const activity = {type, details, nodeId: this.nodeId};
      this.activityLog.push(activity);
    }

    return details;
  }

  async findConsensus() {
    const consensusInput = await this.getConsensusInput();

    const timer = helpers.getTimer();
    const result = await continuityApi.findConsensus(consensusInput);
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
    const {nodeId: ledgerNodeId, consensusState: state} = this;

    const consensusInput = {
      ledgerNodeId,
      history: await this.getHistory(),
      witnesses: this.graph.getWitnesses(),
      state
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

    await this._updateOutstandingMergeEvents();
  }

  async _updateOutstandingMergeEvents() {
    const history = await this.getHistory({includeOutstanding: false});
    const mergedEvents = history.events.map(_eventHashMapper);

    const outstandingEventsMap = new Map();

    for(const event of this.outstandingMergeEvents) {
      outstandingEventsMap.set(event.eventHash, event);
    }

    for(const eventHash of mergedEvents) {
      outstandingEventsMap.delete(eventHash);
    }

    this.outstandingMergeEvents = Array.from(outstandingEventsMap.values());
  }

  async getLocalPeerHead(peer) {
    const {nodeId: peerNodeId} = peer;
    if(!this.seenPeers.has(peerNodeId)) {
      throw new Error(`NotFoundError: "Peer ${peerNodeId}" not seen locally ` +
        `by "Peer ${this.nodeId}"`);
    }
    const history = await this.getHistory();
    const peerHistory = await peer.getHistory();

    const events = history.events.map(_eventHashMapper);
    const peerEvents = peerHistory.events.map(_eventHashMapper);

    const peerEventsSeenLocally = _.intersection(events, peerEvents);
    const localPeerEvents = new Set(peerEventsSeenLocally);

    const localPeerBranch = this.graph.getBranch({nodeId: peerNodeId});
    const orderedLocalPeerEvents = [];

    for(const eventHash of localPeerBranch) {
      if(localPeerEvents.has(eventHash)) {
        orderedLocalPeerEvents.push(eventHash);
      }
    }

    // console.log('peerEvents:', peerEvents.length);
    // console.log('localPeerEventsLength:', localPeerEvents.size);
    // console.log(
    //  'orderedLocalPeerEventsLength:', orderedLocalPeerEvents.length);
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
    const localEventsMap = new Map();

    for(const event of history.events) {
      localEventsMap.set(event.eventHash, event);
    }

    const eventsInLocalHistory = [];
    for(const event of events) {
      const localEvent = localEventsMap.get(event);
      if(localEvent) {
        eventsInLocalHistory.push(localEvent);
      }
    }

    return eventsInLocalHistory;
  }

  async getPeers() {
    const peers = new Map(this.nodes);
    peers.delete(this.nodeId);
    return Array.from(peers.values());
  }

  async getLocalPeers() {
    const peers = [];
    for(const peerNodeId of this.seenPeers) {
      peers.push(this.nodes.get(peerNodeId));
    }
    return peers;
  }

  async getWitnessPeers() {
    const peers = new Map(this.witnesses);
    peers.delete(this.nodeId);
    return Array.from(peers.values());
  }

  async getLocalWitnessPeers() {
    const peers = [];

    for(const peerNodeId of this.seenPeers) {
      const peer = this.witnesses.get(peerNodeId);
      if(!peer) {
        continue;
      }
      peers.push(peer);
    }
    return peers;
  }
}

function _eventHashMapper({eventHash}) {
  return eventHash;
}

module.exports = Node;
