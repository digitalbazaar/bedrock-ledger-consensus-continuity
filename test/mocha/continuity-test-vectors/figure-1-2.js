/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('../tools/Graph');

const graph = new Graph();

// create initial nodes
graph
  .addNode('1')
  .addNode('2')
  .addNode('3')
  .addNode('4');

graph
  // pi_1 y1
  .mergeEvent({eventHash: 'y1', to: '1', from: []})
  // pi_2 y2
  .mergeEvent({eventHash: 'y2', to: '2', from: []})
  // pi_3 y3
  .mergeEvent({eventHash: 'y3', to: '3', from: []})
  // pi_4 y4
  .mergeEvent({eventHash: 'y4', to: '4', from: []})
  // pi_1 1st event
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: '3', eventHash: 'y3'},
      {nodeId: '4', eventHash: 'y4'}
    ]
  })
  // pi_2 1st event
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '3', eventHash: 'y3'},
      {nodeId: '4', eventHash: 'y4'}
    ]
  })
  // pi_3 1st event
  .mergeEvent({
    eventHash: '3-1',
    to: '3',
    from: [
      '3',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '4', eventHash: 'y4'}
    ]
  })
  // pi_4 1st event
  .mergeEvent({
    eventHash: '4-1',
    to: '4',
    from: [
      '4',
      {nodeId: '1', eventHash: 'y1'},
      {nodeId: '3', eventHash: 'y3'}
    ]
  })
  // pi_4 2nd event
  .mergeEvent({
    eventHash: '4-2',
    to: '4',
    from: [
      '4',
      {nodeId: '2', eventHash: '2-1'}
    ]
  })
  // pi_2 2nd event
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: '1', eventHash: '1-1'},
      {nodeId: '4', eventHash: '4-1'}
    ]
  })
  // pi_1 2nd event
  .mergeEvent({
    eventHash: '1-2',
    to: '1',
    from: [
      '1'
    ]
  })
  // pi_3 2nd event
  .mergeEvent({
    eventHash: '3-2',
    to: '3',
    from: [
      '3',
      {nodeId: '2', eventHash: '2-1'}
    ]
  })
  // pi_4 3rd event
  .mergeEvent({
    eventHash: '4-3',
    to: '4',
    from: [
      '4',
      {nodeId: '3', eventHash: '3-2'}
    ]
  })
  // pi_3 3rd event
  .mergeEvent({
    eventHash: '3-3',
    to: '3',
    from: [
      '3',
      {nodeId: '1', eventHash: '1-2'},
      {nodeId: '4', eventHash: '4-3'}
    ]
  })
  // pi_2 3rd event
  .mergeEvent({
    eventHash: '2-3',
    to: '2',
    from: [
      '2',
      {nodeId: '3', eventHash: '3-2'}
    ]
  })
  // pi_1 3rd event
  .mergeEvent({
    eventHash: '1-3',
    to: '1',
    from: [
      '1',
      {nodeId: '2', eventHash: '2-3'},
      {nodeId: '3', eventHash: '3-3'}
    ]
  })
  // pi_4 4th event
  .mergeEvent({
    eventHash: '4-4',
    to: '4',
    from: [
      '4',
      {nodeId: '3', eventHash: '3-3'}
    ]
  })
  // pi_3 4th event (eY_2*)
  .mergeEvent({
    eventHash: '3-4',
    to: '3',
    from: [
      '3',
      {nodeId: '1', eventHash: '1-3'},
      {nodeId: '4', eventHash: '4-4'}
    ]
  })
  // pi_4 5th event (Decision)
  .mergeEvent({
    eventHash: '4-5',
    to: '4',
    from: [
      '4',
      {nodeId: '3', eventHash: '3-4'}
    ]
  });

const ledgerNodeId = '4';
const input = {
  ledgerNodeId,
  history: graph.getHistory({nodeId: ledgerNodeId}),
  electors: graph.getWitnesses(),
  recoveryElectors: [],
  mode: 'first'
};

const display = {
  title: 'Figure 1.2',
  nodeOrder: ['1', '2', '3', '4']
};

module.exports = {input, display, graph};
