/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./Graph');
const Witness = require('./Witness');
const uuid = require('uuid-random');

class Simulator {
  constructor({id, name, creator, run, witnessCount, pipeline, init} = {}) {
    if((witnessCount - 1) % 3 !== 0) {
      throw new Error(`Witness Count must equal "3f + 1".`);
    }

    this.id = uuid();
    this.name = name || id;
    this.run = run || uuid();
    this.timestamp = Date.now();
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
    const {id, name, run, creator, tick, timestamp} = this;

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
      witnessCount,
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
