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
    this.tickId = null;
    this.witnesses = witnesses;
    this.nodeId = nodeId;
    this.pipeline = pipeline;
    this.graph = graph;
    this.outstandingMergeEvents = [];
    this.activityLog = [];
    this.hasConsensus = false;
    this.totalEventsMerged = 0;
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

    const activity = {type, details};

    this.activityLog.push(activity);

    return details;
  }

  async findConsensus() {
    const consensusInput = await this.getConsensusInput();

    const timer = helpers.getTimer();
    const result = await consensusApi.findConsensus(consensusInput);
    const duration = timer.elapsed();

    const nodeHistory = await this.getHistory();

    if(result.consensus) {
      this.hasConsensus = true;
    }

    console.log({
      duration,
      totalEventsMerged: this.totalEventsMerged,
      totalMergeEvents: nodeHistory.events.length
    });

    return {
      ...result,
      duration,
      totalEventsMerged: this.totalEventsMerged,
      totalMergeEvents: nodeHistory.events.length
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
    this.totalEventsMerged++;

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

      const witness = this.witnesses.get(nodeId);
      witness.totalEventsMerged++;
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
    while(this.continue() && tick <= MAX_TICKS) {
      tick++;
      console.log(`\n========== TICK ${tick} ==========`);
      for(let i = 0; i < this.witnesses.size; i++) {
        const nodeId = this._nodeId(i);
        console.log(`\n++++++++++ Node ${nodeId} ++++++++++`);
        const witness = this.witnesses.get(nodeId);
        await witness.tick({id: tick});
      }
    }

    // Activity Log
    // for(let i = 0; i < this.witnesses.size; i++) {
    //   const nodeId = this._nodeId(i);
    //   const witness = this.witnesses.get(nodeId);
    //   console.log(witness.activityLog);
    // }
    console.log('\nTotal Ticks:', tick);
  }

  _nodeId(num) {
    return `${num}`;
  }
}

module.exports = Simulator;
