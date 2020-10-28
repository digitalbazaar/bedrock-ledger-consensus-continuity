/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./tools/Graph');

const figure1_7 = new Graph();

// create initial nodes
figure1_7
  .addNode('1')
  .addNode('h')
  .addNode('b')
  .addNode('2')
  .addNode('alpha')
  .addNode('beta')
  .addNode('gamma');

figure1_7
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
  // pi_h 1st "extra" history event
  .mergeEvent({
    eventHash: 'h-extra-1',
    to: 'h',
    from: [
      'h',
      {nodeId: '1', hash: 'y1'},
      {nodeId: 'b', hash: 'yb'}
    ]
  })
  // pi_alpha 1st "extra" history event
  .mergeEvent({
    eventHash: 'alpha-extra-1',
    to: 'alpha',
    from: [
      'alpha',
      {nodeId: '1', hash: 'y1'},
      {nodeId: '2', hash: 'y2'},
      {nodeId: 'b', hash: 'yb'},
      {nodeId: 'h', hash: 'yh'},
      {nodeId: 'beta', hash: 'ybeta'},
      {nodeId: 'gamma', hash: 'ygamma'},
    ]
  })
  // pi_beta 1st "extra" history event
  .mergeEvent({
    eventHash: 'beta-extra-1',
    to: 'beta',
    from: [
      'beta',
      {nodeId: '1', hash: 'y1'},
      {nodeId: '2', hash: 'y2'},
      {nodeId: 'b', hash: 'yb'},
      {nodeId: 'h', hash: 'yh'},
      {nodeId: 'alpha', hash: 'yalpha'},
      {nodeId: 'gamma', hash: 'ygamma'},
    ]
  })
  // pi_b 1st event
  .mergeEvent({
    eventHash: 'b-1',
    to: 'b',
    from: [
      'b',
      {nodeId: 'h', hash: 'yh'},
    ]
  })
  // pi_b forks (supports Y1)
  .mergeEvent({
    eventHash: 'b1-1',
    to: 'b',
    fork: true,
    treeHash: 'b-1',
    from: [
      {nodeId: 'b', hash: 'b-1'},
      {nodeId: '1', hash: 'y1'},
    ]
  })
  // pi_b forks (supports Y2)
  .mergeEvent({
    eventHash: 'b2-1',
    to: 'b',
    fork: true,
    treeHash: 'b-1',
    from: [
      {nodeId: 'b', hash: 'b-1'},
      {nodeId: '1', hash: 'y1'},
      {nodeId: '2', hash: 'y2'},
      {nodeId: 'alpha', hash: 'alpha-extra-1'},
      {nodeId: 'beta', hash: 'beta-extra-1'}
    ]
  })
  // pi_b fork-1 2nd event
  .mergeEvent({
    eventHash: 'b1-2',
    to: 'b',
    fork: true,
    treeHash: 'b1-1',
    from: [
      {nodeId: 'b', hash: 'b1-1'}
    ]
  })
  // pi_b fork-2 2nd event
  .mergeEvent({
    eventHash: 'b2-2',
    to: 'b',
    fork: true,
    treeHash: 'b2-1',
    from: [
      {nodeId: 'b', hash: 'b2-1'}
    ]
  })
  // pi_1 1st event (merge event m, supports Y1)
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', hash: 'b1-2'}
    ]
  })
  // pi_h 1st event (merge event m1)
  .mergeEvent({
    eventHash: 'h-1',
    to: 'h',
    from: [
      'h',
      {nodeId: '1', hash: '1-1'},
    ]
  })
  // pi_b fork-1 "extra" history 1st event
  .mergeEvent({
    eventHash: 'b1-extra-1',
    to: 'b',
    fork: true,
    treeHash: 'b1-2',
    from: [
      {nodeId: 'b', hash: 'b1-2'},
      {nodeId: '1', hash: '1-1'},
    ]
  })
  // pi_1 "extra" history 1st
  .mergeEvent({
    eventHash: '1-extra-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', hash: 'b1-extra-1'}
    ]
  })
  // pi_1 2nd event (merge event m1, supports Y1, endorsement of m)
  .mergeEvent({
    eventHash: '1-2',
    to: '1',
    from: [
      '1',
      {nodeId: 'h', hash: 'h-1'}
    ]
  })
  // pi_2 1st event (merge event m', supports Y2)
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: 'h', hash: 'h-extra-1'},
      {nodeId: 'b', hash: 'b2-2'}
    ]
  })
  // pi_h 2nd event (merge event m2)
  .mergeEvent({
    eventHash: 'h-2',
    to: 'h',
    from: [
      'h',
      {nodeId: '2', hash: '2-1'}
    ]
  })
  // pi_b fork-2 "extra" history 1st event
  .mergeEvent({
    eventHash: 'b2-extra-1',
    to: 'b',
    fork: true,
    treeHash: 'b2-2',
    from: [
      {nodeId: 'b', hash: 'b2-2'},
      {nodeId: '2', hash: '2-1'}
    ]
  })
  // pi_alpha "extra" history 2nd event
  .mergeEvent({
    eventHash: 'alpha-extra-2',
    to: 'alpha',
    from: [
      'alpha',
      {nodeId: '2', hash: '2-1'}
    ]
  })
  // pi_beta "extra" history 2nd event
  .mergeEvent({
    eventHash: 'beta-extra-2',
    to: 'beta',
    from: [
      'beta',
      {nodeId: '2', hash: '2-1'}
    ]
  })
  // pi_gamma "extra" history 1st event
  .mergeEvent({
    eventHash: 'gamma-extra-1',
    to: 'gamma',
    from: [
      'gamma',
      {nodeId: '2', hash: '2-1'}
    ]
  })
  // pi_2 "extra" history 1st event
  .mergeEvent({
    eventHash: '2-extra-1',
    to: '2',
    from: [
      '2',
      {nodeId: 'b', hash: 'b2-extra-1'},
      {nodeId: 'alpha', hash: 'alpha-extra-2'},
      {nodeId: 'beta', hash: 'beta-extra-2'},
      {nodeId: 'gamma', hash: 'gamma-extra-1'},
    ]
  })
  // pi_2 2nd event (merge event m', supports Y2, endorsement of m')
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: 'h', hash: 'h-2'}
    ]
  });

const ledgerNodeId = '2';
const input = {
  ledgerNodeId,
  history: figure1_7.getHistory({nodeId: ledgerNodeId}),
  electors: figure1_7.getElectors(),
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);

module.exports = input;
