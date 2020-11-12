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
    this.totalMergeEventsCreated = 0;
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
      consensusDuration,
      totalMergeEventsCreated: this.totalMergeEventsCreated,
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
    this.totalMergeEventsCreated++;

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
  constructor({id, creator, witnessCount, pipeline, init} = {}) {
    this.id = id;
    this.runId = Date.now();
    this.creator = creator;
    this.graph = new Graph();
    this.witnesses = new Map();
    this.tick = 0;

    this._createWitnesses({count: witnessCount, pipeline});

    if(init) {
      return init(this);
    }

    this._defaultInitialization({count: witnessCount});
  }

  continue() {
    let stop = true;
    for(const witness of this.witnesses.values()) {
      stop = stop && witness.hasConsensus;
    }
    return !stop;
  }

  async start() {
    this.tick = 0;
    const MAX_TICKS = 1000;
    while(this.continue() && this.tick <= MAX_TICKS) {
      this.tick++;
      console.log(`\n========== TICK ${this.tick} ==========`);
      for(let i = 0; i < this.witnesses.size; i++) {
        const nodeId = this._nodeId(i);
        console.log(`\n++++++++++ Node ${nodeId} ++++++++++`);
        const witness = this.witnesses.get(nodeId);
        await witness.tick({id: this.tick});
      }
    }

    // console.log('\nTotal Ticks:', tick);
    return this._generateReport();
  }

  _generateReport() {
    const {id, runId, creator, tick} = this;

    const consensusReports = [];
    const gossipSessions = {};

    // Activity Log
    for(let i = 0; i < this.witnesses.size; i++) {
      const nodeId = this._nodeId(i);
      const witness = this.witnesses.get(nodeId);

      // find earliest consensus
      const earliestConsensus = witness.activityLog.find(({type, details}) => {
        if(type === 'consensus' && details.consensus === true) {
          return true;
        }
      });

      consensusReports.push(earliestConsensus);

      // calculate total gossip sessions
      const gossips = witness.activityLog.filter(({type}) => type === 'gossip');
      gossipSessions[nodeId] = gossips.length;
    }

    // calculate network averages
    const results = consensusReports.reduce((acc, curr, idx) => {
      Object.keys(acc).forEach(key => {
        acc[key] += curr.details[key];
      });

      const {length} = consensusReports;

      if(idx === length - 1) {
        Object.keys(acc).forEach(key => {
          acc[key] /= length;
        });
      }

      return acc;
    }, {consensusDuration: 0, totalMergeEvents: 0, totalMergeEventsCreated: 0});

    return {
      id,
      runId,
      creator,
      totalTicks: tick,
      average: results,
      gossipSessions
    };
  }

  _defaultInitialization({count}) {
    for(let i = 0; i < count; i++) {
      const nodeId = this._nodeId(i);

      // create nodes in graph
      this.graph.addNode(nodeId);
      this.graph.mergeEvent({eventHash: `y${nodeId}`, to: nodeId, from: []});

      const witness = this.witnesses.get(nodeId);
      witness.totalMergeEventsCreated++;
    }
  }

  _createWitnesses({count, pipeline}) {
    for(let i = 0; i < count; i++) {
      const nodeId = this._nodeId(i);

      // add witness to map
      const witness = new Witness({
        nodeId, pipeline, graph: this.graph, witnesses: this.witnesses
      });
      this.witnesses.set(nodeId, witness);
    }
  }

  _nodeId(num) {
    return `${num}`;
  }
}

module.exports = Simulator;
