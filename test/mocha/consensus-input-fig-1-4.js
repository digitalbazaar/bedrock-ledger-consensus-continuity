/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./tools/Graph');

const figure1_4 = new Graph();

// create initial nodes
figure1_4
  .addNode('1')
  .addNode('b')
  .addNode('2');

figure1_4
  // pi_1 y1
  .mergeEvent({eventHash: 'y1', to: '1', from: []})
  // pi_b yb
  .mergeEvent({eventHash: 'yb', to: 'b', from: []})
  // pi_2 y2
  .mergeEvent({eventHash: 'y2', to: '2', from: []})
  // pi_b 1st event
  .mergeEvent({
    eventHash: 'b-1',
    to: 'b',
    from: [
      'b'
    ]
  })
  // pi_1 1st event
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', hash: 'b-1'}
    ]
  })
  // pi_b forks and creates m1
  .mergeEvent({
    eventHash: 'b1-1',
    to: 'b',
    fork: true,
    treeHash: 'b-1',
    from: [
      {nodeId: 'b', hash: 'b-1'}
    ]
  })
  // pi_b forks and creates m2
  .mergeEvent({
    eventHash: 'b2-1',
    to: 'b',
    fork: true,
    treeHash: 'b-1',
    from: [
      {nodeId: 'b', hash: 'b-1'}
    ]
  })
  // pi_b forks and creates m3
  .mergeEvent({
    eventHash: 'b3-1',
    to: 'b',
    fork: true,
    treeHash: 'b-1',
    from: [
      {nodeId: 'b', hash: 'b-1'}
    ]
  })
  // pi_1 2nd event
  .mergeEvent({
    eventHash: '1-2',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', hash: 'b1-1'}
    ]
  })
  // pi_1 3rd event
  .mergeEvent({
    eventHash: '1-3',
    to: '1',
    from: [
      '1'
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
  // pi_b fork-3 2nd event
  .mergeEvent({
    eventHash: 'b3-2',
    to: 'b',
    fork: true,
    treeHash: 'b3-1',
    from: [
      {nodeId: 'b', hash: 'b3-1'}
    ]
  })
  // pi_2 1st event
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: 'b', hash: 'b3-2'}
    ]
  })
  // pi_2 2nd event
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: '1', hash: '1-3'}
    ]
  });

const ledgerNodeId = '2';
const input = {
  ledgerNodeId,
  history: figure1_4.getHistory({nodeId: ledgerNodeId}),
  electors: figure1_4.getElectors(),
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);

module.exports = input;
