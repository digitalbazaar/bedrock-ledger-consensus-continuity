/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Denque = require('denque');
const LRU = require('lru-cache');
const uuid = require('uuid-random');
const yallist = require('yallist');
const v8 = require('v8');
const helpers = require('./helpers');

const {strfy} = helpers;

const MAX_CACHE_SIZE = 4096;

class Graph {
  constructor() {
    this.nodes = new Map();
    this.bfsCache = new LRU({max: MAX_CACHE_SIZE});
    this.eventMap = {};
  }

  static traverseBFS(options) {
    return _traverseBFS(options);
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

    const toBranch = this.getBranch({nodeId: to.nodeId});

    const parents = from.map(({nodeId, eventHash}) => {
      // return specified eventHash
      if(eventHash) {
        return eventHash;
      }
      // return latest eventHash on branch
      const fromBranch = this.getBranch({nodeId});
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

  getBranch({nodeId}) {
    const node = this.nodes.get(nodeId);
    if(!node) {
      throw new Error(`The node does not exist: "${nodeId}"`);
    }
    return node.branch;
  }

  getHistory({nodeId, extraEvents = []} = {}) {
    const node = this.nodes.get(nodeId);

    const tail = node.branch.tail.value;

    const results = this._traverseBFS({tail});

    const events = Array.from(results.values());

    const history = v8.deserialize(v8.serialize({
      events: [...events, ...extraEvents],
      eventMap: {},
      localBranchHead: {
        eventHash: tail,
        generation: node.branch.length
      }
    }));

    for(const event of history.events) {
      history.eventMap[event.eventHash] = event;
    }

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

  _traverseBFS({tail}) {
    return _traverseBFS({
      tail, bfsCache: this.bfsCache, eventMap: this.eventMap
    });
  }
}

function _traverseBFS({tail, bfsCache = new LRU(), eventMap} = {}) {
  const cachedResults = bfsCache.get(tail);
  if(cachedResults) {
    return cachedResults;
  }

  const results = new Set();
  const queue = new Denque([tail]);

  while(queue.length > 0) {
    const eventHash = queue.shift();

    const cachedEvents = bfsCache.get(eventHash);
    if(cachedEvents) {
      for(const cachedEvent of cachedEvents.values()) {
        results.add(cachedEvent);
      }
      continue;
    }

    const event = eventMap[eventHash];

    if(!event) {
      continue;
    }

    results.add(event);

    const {event: {parentHash}} = event;

    for(const hash of parentHash) {
      queue.push(hash);
    }
  }

  bfsCache.set(tail, results);
  return results;
}

module.exports = Graph;
