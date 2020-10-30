/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const uuid = require('uuid-random');
const yallist = require('yallist');
const helpers = require('./helpers');

const {strfy} = helpers;

class Graph {
  constructor() {
    this.nodes = new Map();
    this.eventMap = {};
  }

  addNode(id, options = {}) {
    const {isElector = true} = options;
    if(this.nodes.has(id)) {
      throw new Error(`Duplicate Error: Node with specified id exists "${id}"`);
    }

    const node = {
      id,
      isElector,
      branch: yallist.create([])
    };

    this.nodes.set(id, node);

    return this;
  }

  mergeEvent({eventHash, to, from = [], fork = false, treeHash} = {}) {
    if(fork && !treeHash) {
      throw new Error(`"treeHash" must be specified: ${eventHash}`);
    }
    if(typeof to === 'string') {
      to = {
        nodeId: to
      };
    }
    if(!Array.isArray(from)) {
      from = [from];
    }

    from = from.map(f => {
      if(typeof f === 'string') {
        return from = {
          nodeId: f
        };
      } else {
        // validate object and ensure it's the proper format
        return f;
      }
    });

    const toBranch = this._getBranch({nodeId: to.nodeId});

    const parents = from.map(({nodeId, eventHash}) => {
      // return specified eventHash
      if(eventHash) {
        return eventHash;
      }
      // return latest eventHash on branch
      const fromBranch = this._getBranch({nodeId});
      return this.eventMap[fromBranch.tail.value].eventHash;
    });

    if(!fork) {
      treeHash = toBranch.tail ?
        this.eventMap[toBranch.tail.value].eventHash : uuid();
    }

    if(parents.length === 0) {
      parents.push(treeHash);
    }

    if(this.eventMap[eventHash]) {
      throw new Error(`Duplicate Error: EventHash "${eventHash}" exists.`);
    }

    this.eventMap[eventHash] = {
      _children: [],
      _parents: [],
      eventHash,
      event: {
        parentHash: parents,
        treeHash,
        type: 'ContinuityMergeEvent',
      },
      meta: {
        continuity2017: {
          creator: to.nodeId
        }
      }
    };

    toBranch.push(eventHash);

    return this;
  }

  getHistory({nodeId}) {
    const node = this.nodes.get(nodeId);

    const tails = [];

    tails.push(node.branch.tail.value);

    const results = new Map();

    tails.forEach(tail => this._traverseBFS({tail, results}));

    const events = Array.from(results.values())
      .sort((a, b) => (a.order < b.order) ? 1 : -1)
      .map(({event}) => event);

    const history = JSON.parse(JSON.stringify({
      events,
      eventMap: {},
      localBranchHead: {
        eventHash: tails[0],
        generation: node.branch.length
      }
    }));

    history.events.forEach(e => history.eventMap[e.eventHash] = e);

    return history;
  }

  getElectors() {
    const electors = [];
    this.nodes.forEach(node => electors.push(node));

    return electors.filter(({isElector}) => isElector).map(({id}) => ({id}));
  }

  debug() {
    const {nodes, eventMap} = this;

    const nodeData = [];
    nodes.forEach(node => {
      nodeData.push({
        nodeId: node.id,
        branch: node.branch.toArray()
      });
    });
    console.log('nodes', nodes);
    console.log('nodeData', strfy({nodeData}));
    console.log('eventMap', strfy({eventMap}));
  }

  _traverseBFS({tail, results}) {
    const queue = [tail];
    let counter = 0;
    while(queue.length > 0) {
      const eventHash = queue.shift();
      const event = this.eventMap[eventHash];

      if(!event) {
        continue;
      }

      results.set(eventHash, {event, order: ++counter});

      const {event: {parentHash}} = event;

      queue.push(...parentHash);
    }
  }

  _getBranch({nodeId}) {
    const node = this.nodes.get(nodeId);
    if(!node) {
      throw new Error(`The node does not exist: "${nodeId}"`);
    }
    return node.branch;
  }
}

module.exports = Graph;
