/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const consensusApi =
  require('bedrock-ledger-consensus-continuity/lib/consensus');
const helpers = require('./helpers');
const Graph = require('./Graph');

class Witness {
  constructor({nodeId, pipeline, graph, witnesses}) {
    this.witnesses = witnesses;
    this.nodeId = nodeId;
    this.pipeline = pipeline;
    this.graph = graph;
    this.outstandingMergeEvents = [];
    this.activityLog = [];
    this.hasConsensus = false;
  }

  async tick() {
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

    const activity = {type, details};

    this.activityLog.push(activity);

    return details;
  }

  async findConsensus() {
    const consensusInput = await this.getConsensusInput();

    const result = await consensusApi.findConsensus(consensusInput);
    if(result.consensus) {
      this.hasConsensus = true;
    }
    return result;
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

    const history = await this.getHistory({includeOutstanding: false});
    const mergedEvents = history.events.map(({eventHash}) => eventHash);
    this.outstandingMergeEvents = this.outstandingMergeEvents
      .filter(({eventHash}) => !mergedEvents.includes(eventHash));
  }

  async getHead() {
    const history = await this.getHistory({includeOutstanding: false});
    return history.localBranchHead.eventHash;
  }

  async getHistory({includeOutstanding = true} = {}) {
    const {nodeId} = this;
    const extraEvents = includeOutstanding ? this.outstandingMergeEvents : [];
    return this.graph.getHistory({nodeId, extraEvents});
  }

  async addEvents({events}) {
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
}

class Simulator {
  constructor({witnessCount, pipeline, init} = {}) {
    this.graph = new Graph();
    this.witnesses = new Map();
    this.ticks = [];

    for(let i = 0; i < witnessCount; i++) {
      const nodeId = this._nodeId(i);

      // add witness to map
      const witness = new Witness({
        nodeId, pipeline, graph: this.graph, witnesses: this.witnesses
      });
      this.witnesses.set(nodeId, witness);
    }

    if(init) {
      return init(this);
    }

    // default initialization
    for(let i = 0; i < witnessCount; i++) {
      const nodeId = this._nodeId(i);

      // create nodes in graph
      this.graph.addNode(nodeId);
      this.graph.mergeEvent({eventHash: `y${nodeId}`, to: nodeId, from: []});
    }
  }

  continue() {
    let stop = true;
    for(const witness of this.witnesses.values()) {
      stop = stop && witness.hasConsensus;
    }
    return !stop;
  }

  async start() {
    let tick = 0;
    const MAX_TICKS = 1000;
    while(this.continue() && tick < MAX_TICKS) {
      console.log(`========== TICK ${tick + 1} ==========`);
      for(let i = 0; i < this.witnesses.size; i++) {
        const nodeId = this._nodeId(i);
        const witness = this.witnesses.get(nodeId);
        await witness.tick();
      }
      ++tick;
    }
    console.log('\nTotal Ticks:', tick);
  }

  _nodeId(num) {
    return `${num}`;
  }
}

module.exports = Simulator;
