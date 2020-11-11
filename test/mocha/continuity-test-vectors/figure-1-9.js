/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('../tools/Graph');

const graph = new Graph();

// create initial nodes
graph
  .addNode('1')
  .addNode('b')
  .addNode('2')
  .addNode('3');

graph
  // pi_1 y1
  .mergeEvent({eventHash: 'y1', to: '1', from: []})
  // pi_b yb
  .mergeEvent({eventHash: 'yb', to: 'b', from: []})
  // pi_2 y2
  .mergeEvent({eventHash: 'y2', to: '2', from: []})
  // pi_3 y3
  .mergeEvent({eventHash: 'y3', to: '3', from: []})
  // pi_b 1st event (supports Y1)
  .mergeEvent({
    eventHash: 'b-1',
    to: 'b',
    from: [
      'b',
      {nodeId: '3', eventHash: 'y3'},
      {nodeId: '2', eventHash: 'y2'}
    ]
  })
  // pi_3 1st event (supports Y1)
  .mergeEvent({
    eventHash: '3-1',
    to: '3',
    from: [
      '3',
      {nodeId: 'b', eventHash: 'yb'},
      {nodeId: '2', eventHash: 'y2'}
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
    ]
  })
  // pi_1 1st event (supports Y2)
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', eventHash: 'b2-1'}
    ]
  })
  // pi_2 1st event (supports Y1, proposes Y1)
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: 'b', eventHash: 'b1-1'},
      {nodeId: '3', eventHash: '3-1'}
    ]
  })
  // b1 2nd event (supports Y1, proposes Y1)
  .mergeEvent({
    eventHash: 'b1-2',
    to: 'b',
    from: [
      {nodeId: 'b', eventHash: 'b1-1'},
      {nodeId: '2', eventHash: '2-1'}
    ]
  })
  // pi_3 2nd event (supports Y1, proposes Y1)
  .mergeEvent({
    eventHash: '3-2',
    to: '3',
    from: [
      '3',
      {nodeId: '2', eventHash: '2-1'}
    ]
  })
  // pi_2 2nd event (supports Y1, proposes Y1, endorses Y1)
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: 'b', eventHash: 'b1-2'},
      {nodeId: '3', eventHash: '3-2'}
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
  title: 'Figure 1.9',
  nodeOrder: ['1', 'b', '2', '3']
};

module.exports = {input, display, graph};
