/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Denque = require('denque');
const LRU = require('lru-cache');
const uuid = require('uuid-random');
const yallist = require('yallist');
const helpers = require('./helpers');

const {strfy} = helpers;

const MAX_CACHE_SIZE = 4096;

class Graph {
  constructor() {
    this.nodes = new Map();
    this.bfsCache = new LRU({max: MAX_CACHE_SIZE});
    this.eventMap = new Map();
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
        return {
          nodeId: f,
          eventHash: null
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
      return this.eventMap.get(fromBranch.tail.value).eventHash;
    });

    if(!fork && !treeHash) {
      treeHash = toBranch.tail ?
        this.eventMap.get(toBranch.tail.value).eventHash : uuid();
    }

    if(parents.length === 0) {
      parents.push(treeHash);
    }

    if(this.eventMap.has(eventHash)) {
      throw new Error(`Duplicate Error: EventHash "${eventHash}" exists.`);
    }

    const event = {
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
    this.eventMap.set(eventHash, event);

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

    const history = deepCopy({
      events: [...events, ...extraEvents],
      eventMap: {},
      localBranchHead: {
        eventHash: tail,
        generation: node.branch.length
      }
    });

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
    console.log('eventMap', eventMap);
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

    const event = eventMap.get(eventHash);

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

// Fastest implementation of deep copy
// https://gist.github.com/c7x43t/38afee87bb7391efb9ac27a3c282e5ed/
function deepCopy(o) {
  // if not array or object or is null return self
  if(typeof o !== 'object' || o === null) {
    return o;
  }
  let newO, i;
  // handle case: array
  if(o instanceof Array) {
    let l;
    newO = [];
    for(i = 0, l = o.length; i < l; i++) {
      newO[i] = deepCopy(o[i]);
    }
    return newO;
  }
  // handle case: object
  newO = {};
  for(i in o) {
    if(o.hasOwnProperty(i)) {
      newO[i] = deepCopy(o[i]);
    }
  }
  return newO;
}

module.exports = Graph;
