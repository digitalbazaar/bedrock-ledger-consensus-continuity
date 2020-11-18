/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./Graph');
const Node = require('./Node');
const uuid = require('uuid-random');

class Simulator {
  constructor({
    id, name, creator, run, witnessCount, nodeCount = witnessCount,
    pipeline, init
  } = {}) {
    if((witnessCount - 1) % 3 !== 0) {
      throw new Error(`Witness Count must equal "3f + 1".`);
    }
    if(nodeCount < witnessCount) {
      throw new Error(
        'Node count must not be less than "witnessCount"; it includes ' +
        'witnesses.');
    }

    this.id = uuid();
    this.name = name || id;
    this.run = run || uuid();
    this.timestamp = Date.now();
    this.creator = creator;
    this.graph = new Graph();
    this.nodes = new Map();
    this.witnesses = new Map();
    this.tick = 0;

    this._createNodes({nodeCount, witnessCount, pipeline});

    if(init) {
      return init(this);
    }

    this._defaultInitialization({nodeCount, witnessCount});
  }

  continue() {
    let stop = true;
    for(const node of this.nodes.values()) {
      stop = stop && node.hasConsensus;
    }
    return !stop;
  }

  async start() {
    this.tick = 0;
    const MAX_TICKS = 1000;
    while(this.continue() && this.tick <= MAX_TICKS) {
      this.tick++;
      console.log(`\n========== TICK ${this.tick} ==========`);
      for(let i = 0; i < this.nodes.size; i++) {
        const nodeId = this._nodeId(i);
        console.log(`\n++++++++++ Node ${nodeId} ++++++++++`);
        const node = this.nodes.get(nodeId);
        await node.tick({id: this.tick});
      }
    }

    // console.log('\nTotal Ticks:', tick);
    return this._generateReport();
  }

  _generateReport() {
    const {id, name, run, creator, tick, timestamp} = this;

    const consensusReports = [];
    const gossipSessions = {};

    // Activity Log
    for(let i = 0; i < this.nodes.size; i++) {
      const nodeId = this._nodeId(i);
      const node = this.nodes.get(nodeId);

      // find earliest consensus
      const earliestConsensus = node.activityLog.find(({type, details}) => {
        if(type === 'consensus' && details.consensus === true) {
          return true;
        }
      });

      consensusReports.push(earliestConsensus);

      // calculate total gossip sessions
      const gossips = node.activityLog.filter(({type}) => type === 'gossip');
      const downloadedEventsTotal = gossips.reduce((acc, curr) => {
        acc += curr.details.events;
        return acc;
      }, 0);

      gossipSessions[nodeId] = {
        total: gossips.length,
        downloadedEventsTotal
      };
    }

    const baseResult = {
      consensusDuration: 0,
      totalMergeEvents: 0,
      totalMergeEventsCreated: 0
    };
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
    }, baseResult);

    const nodeCount = this.nodes.size;
    const witnessCount = this.witnesses.size;

    const f = (witnessCount - 1) / 3;
    const targetMergeEvents = (9 * f) + 3;

    return {
      id,
      name,
      run,
      creator,
      timestamp,
      totalTimeSlices: tick,
      targetMergeEvents,
      average: results,
      gossipSessions,
      nodeCount,
      witnessCount,
    };
  }

  _defaultInitialization({nodeCount, witnessCount}) {
    for(let i = 0; i < nodeCount; i++) {
      const nodeId = this._nodeId(i);

      // create nodes in graph
      this.graph.addNode(nodeId);
      const node = this.nodes.get(nodeId);
      // `y` means little `y` from the consensus algorithm, only witnesses
      // make these; `i` means initial event, for non-witnesses always make
      // one of these to get started as well
      const eventHash = node.isWitness ? `y${nodeId}` : `i${nodeId}`;
      this.graph.mergeEvent({eventHash, to: nodeId, from: []});

      node.totalMergeEventsCreated++;
    }
  }

  _createNodes({nodeCount, witnessCount, pipeline}) {
    // create nodes
    for(let i = 0; i < nodeCount; i++) {
      const nodeId = this._nodeId(i);
      const isWitness = i < witnessCount;

      // add node to map
      const node = new Node({
        nodeId, pipeline, graph: this.graph, witnesses: this.witnesses,
        isWitness
      });
      this.nodes.set(nodeId, node);

      // add witness to map
      if(isWitness) {
        this.witnesses.set(nodeId, node);
      }
    }
  }

  _nodeId(num) {
    return `${num}`;
  }
}

module.exports = Simulator;
