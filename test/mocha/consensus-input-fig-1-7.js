/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./tools/Graph');

const graph = new Graph();

// create initial nodes
graph
  .addNode('1')
  .addNode('h')
  .addNode('b')
  .addNode('2')
  .addNode('alpha')
  .addNode('beta')
  .addNode('gamma');

graph
  // pi_1 y1
  .mergeEvent({eventHash: 'y1', to: '1', from: []})
  // pi_h yh
  .mergeEvent({eventHash: 'yh', to: 'h', from: []})
  // pi_b yb
  .mergeEvent({eventHash: 'yb', to: 'b', from: []})
  // pi_2 y2
  .mergeEvent({eventHash: 'y2', to: '2', from: []})
  // pi_alpha yalpha
  .mergeEvent({eventHash: 'yalpha', to: 'alpha', from: []})
  // pi_beta ybeta
  .mergeEvent({eventHash: 'ybeta', to: 'beta', from: []})
  // pi_gamma ygamma
  .mergeEvent({eventHash: 'ygamma', to: 'gamma', from: []})
  // pi_h "extra" history 1st event
  .mergeEvent({
    eventHash: 'h-xt-1',
    to: 'h',
    from: [
      'h',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '2', eventHash: 'y2'},
      {nodeId: 'b', eventHash: 'yb'},
      {nodeId: 'gamma', eventHash: 'ygamma'},

    ]
  })
  // pi_alpha "extra" history 1st event
  .mergeEvent({
    eventHash: 'alpha-xt-1',
    to: 'alpha',
    from: [
      'alpha',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '2', eventHash: 'y2'},
      {nodeId: 'b', eventHash: 'yb'},
      {nodeId: 'h', eventHash: 'yh'},
      {nodeId: 'beta', eventHash: 'ybeta'},
      {nodeId: 'gamma', eventHash: 'ygamma'},
    ]
  })
  // pi_beta "extra" history 1st event
  .mergeEvent({
    eventHash: 'beta-xt-1',
    to: 'beta',
    from: [
      'beta',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '2', eventHash: 'y2'},
      {nodeId: 'b', eventHash: 'yb'},
      {nodeId: 'h', eventHash: 'yh'},
      {nodeId: 'alpha', eventHash: 'yalpha'},
      {nodeId: 'gamma', eventHash: 'ygamma'},
    ]
  })
  // pi_2 "extra" history 1st event
  .mergeEvent({
    eventHash: '2-xt-1',
    to: '2',
    from: [
      '2',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: 'gamma', eventHash: 'ygamma'},
      {nodeId: 'b', eventHash: 'yb'},
      {nodeId: 'h', eventHash: 'yh'}
    ]
  })
  // pi_gamma "extra" history 1st event
  .mergeEvent({
    eventHash: 'gamma-xt-1',
    to: 'gamma',
    from: [
      'gamma',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '2', eventHash: 'y2'},
      {nodeId: 'b', eventHash: 'yb'},
      {nodeId: 'h', eventHash: 'yh'}
    ]
  })
  // pi_gamma "extra" history 2nd event
  .mergeEvent({
    eventHash: 'gamma-xt-2',
    to: 'gamma',
    from: [
      'gamma',
      {nodeId: 'alpha', eventHash: 'yalpha'},
      {nodeId: 'beta', eventHash: 'ybeta'},
    ]
  })
  // pi_b 1st event
  .mergeEvent({
    eventHash: 'b-1',
    to: 'b',
    from: [
      'b',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '2', eventHash: '2-xt-1'},
      {nodeId: 'gamma', eventHash: 'gamma-xt-1'},
      {nodeId: 'h', eventHash: 'yh'},
    ]
  })
  // pi_b forks (supports Y1)
  .mergeEvent({
    eventHash: 'b1-1',
    to: 'b',
    fork: true,
    treeHash: 'b-1',
    from: [
      {nodeId: 'b', eventHash: 'b-1'},
      {nodeId: '1', eventHash: 'y1'},
    ]
  })
  // pi_b forks (supports Y2)
  .mergeEvent({
    eventHash: 'b2-1',
    to: 'b',
    fork: true,
    treeHash: 'b-1',
    from: [
      {nodeId: 'b', eventHash: 'b-1'},
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '2', eventHash: 'y2'},
      {nodeId: 'alpha', eventHash: 'alpha-xt-1'},
      {nodeId: 'beta', eventHash: 'beta-xt-1'},
      {nodeId: 'gamma', eventHash: 'gamma-xt-2'},
    ]
  })
  // pi_b fork-1 2nd event
  .mergeEvent({
    eventHash: 'b1-2',
    to: 'b',
    fork: true,
    treeHash: 'b1-1',
    from: [
      {nodeId: 'b', eventHash: 'b1-1'}
    ]
  })
  // pi_b fork-2 2nd event
  .mergeEvent({
    eventHash: 'b2-2',
    to: 'b',
    fork: true,
    treeHash: 'b2-1',
    from: [
      {nodeId: 'b', eventHash: 'b2-1'}
    ]
  })
  // pi_1 1st event (merge event m, supports Y1)
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', eventHash: 'b1-2'},
      {nodeId: 'h', eventHash: 'h-xt-1'},
    ]
  })
  // pi_gamma "extra" history 3rd event
  .mergeEvent({
    eventHash: 'gamma-xt-3',
    to: 'gamma',
    from: [
      'gamma',
      {nodeId: '1', eventHash: '1-1'},
    ]
  })
  // pi_h 1st event (merge event m1)
  .mergeEvent({
    eventHash: 'h-1',
    to: 'h',
    from: [
      'h',
      {nodeId: '1', eventHash: '1-1'},
    ]
  })
  // pi_b fork-1 "extra" history 1st event
  .mergeEvent({
    eventHash: 'b1-xt-1',
    to: 'b',
    fork: true,
    treeHash: 'b1-2',
    from: [
      {nodeId: 'b', eventHash: 'b1-2'},
      {nodeId: '1', eventHash: '1-1'},
    ]
  })
  // pi_alpha "extra" history 2nd event
  .mergeEvent({
    eventHash: 'alpha-xt-2',
    to: 'alpha',
    from: [
      'alpha',
      {nodeId: '1', eventHash: '1-1'}
    ]
  })
  // pi_1 "extra" history 1st event
  .mergeEvent({
    eventHash: '1-xt-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', eventHash: 'b1-xt-1'},
      {nodeId: 'gamma', eventHash: 'gamma-xt-3'},
      {nodeId: 'alpha', eventHash: 'alpha-xt-2'},
    ]
  })
  // pi_1 2nd event (merge event m1, supports Y1, endorsement of m)
  .mergeEvent({
    eventHash: '1-2',
    to: '1',
    from: [
      '1',
      {nodeId: 'h', eventHash: 'h-1'}
    ]
  })
  // pi_2 1st event (merge event m', supports Y2)
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: 'h', eventHash: 'h-xt-1'},
      {nodeId: 'b', eventHash: 'b2-2'}
    ]
  })
  // pi_alpha "extra" history 3rd event
  .mergeEvent({
    eventHash: 'alpha-xt-3',
    to: 'alpha',
    from: [
      'alpha',
      {nodeId: '2', eventHash: '2-1'}
    ]
  })
  // pi_beta "extra" history 2nd event
  .mergeEvent({
    eventHash: 'beta-xt-2',
    to: 'beta',
    from: [
      'beta',
      {nodeId: 'alpha', eventHash: 'alpha-xt-3'}
    ]
  })
  // pi_h 2nd event (merge event m2)
  .mergeEvent({
    eventHash: 'h-2',
    to: 'h',
    from: [
      'h',
      {nodeId: '2', eventHash: '2-1'}
    ]
  })
  // pi_b fork-2 "extra" history 1st event
  .mergeEvent({
    eventHash: 'b2-xt-1',
    to: 'b',
    fork: true,
    treeHash: 'b2-2',
    from: [
      {nodeId: 'b', eventHash: 'b2-2'},
      {nodeId: '2', eventHash: '2-1'}
    ]
  })
  // pi_2 "extra" history 1st event
  .mergeEvent({
    eventHash: '2-xt-2',
    to: '2',
    from: [
      '2',
      {nodeId: 'b', eventHash: 'b2-xt-1'},
    ]
  })
  // pi_2 2nd event (merge event m', supports Y2, endorsement of m')
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: 'h', eventHash: 'h-2'},
      {nodeId: 'beta', eventHash: 'beta-xt-2'},

    ]
  });

const ledgerNodeId = '2';
const input = {
  ledgerNodeId,
  history: graph.getHistory({nodeId: ledgerNodeId}),
  electors: graph.getElectors(),
  recoveryElectors: [],
  mode: 'first'
};

const display = {
  title: 'Figure 1.7',
  nodeOrder: ['gamma', '1', 'h', 'b', '2', 'alpha', 'beta']
};

module.exports = {input, display, graph};
