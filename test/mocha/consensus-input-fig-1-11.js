/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./tools/Graph');

const figure1_11 = new Graph();

// create initial nodes
figure1_11
  .addNode('1')
  .addNode('b')
  .addNode('2')
  .addNode('3');

figure1_11
  // pi_1 y1
  .mergeEvent({eventHash: 'y1', to: '1', from: []})
  // pi_b yb
  .mergeEvent({eventHash: 'yb', to: 'b', from: []})
  // pi_2 y2
  .mergeEvent({eventHash: 'y2', to: '2', from: []})
  // pi_3 y3
  .mergeEvent({eventHash: 'y3', to: '3', from: []})
  // pi_1 1st event (supports Y1)
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', hash: 'yb'},
      {nodeId: '2', hash: 'y2'},
    ]
  })
  // pi_b 1st event (supports Y1)
  .mergeEvent({
    eventHash: 'b-1',
    to: 'b',
    from: [
      'b',
      {nodeId: '1', hash: 'y1'},
      {nodeId: '2', hash: 'y2'},
    ]
  })
  // pi_2 1st event (supports Y1)
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: 'b', hash: 'yb'},
      {nodeId: '1', hash: 'y1'},
    ]
  })
  // pi_3 1st event (supports Y2)
  .mergeEvent({
    eventHash: '3-1',
    to: '3',
    from: [
      '3',
      {nodeId: '2', hash: 'y2'},
      {nodeId: 'b', hash: 'yb'},
      {nodeId: '1', hash: 'y1'},
    ]
  })
  // pi_b forks (supports Y1)
  .addNode('b1', {isElector: false})
  .mergeEvent({
    eventHash: 'b1-1',
    forked: true,
    to: 'b1',
    from: [
      'b'
    ]
  })
  // pi_b forks (supports Y2)
  .addNode('b2', {isElector: false})
  .mergeEvent({
    eventHash: 'b2-1',
    forked: true,
    to: 'b2',
    from: [
      'b',
      {nodeId: '3', hash: '3-1'}
    ]
  })
  // pi_2 2nd event (supports Y1, proposes Y1)
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: '1', hash: '1-1'},
      {nodeId: 'b', hash: 'b-1'},
      {nodeId: '3', hash: '3-1'},
    ]
  })
  // pi_1 2nd event (supports Y2)
  .mergeEvent({
    eventHash: '1-2',
    to: '1',
    from: [
      '1',
      {nodeId: 'b2', hash: 'b2-1'},
      {nodeId: '2', hash: '2-2'}
    ]
  })
  // pi_3 2nd event (supports Y1, proposes Y1)
  .mergeEvent({
    eventHash: '3-2',
    to: '3',
    from: [
      '3',
      {nodeId: '2', hash: '2-2'}
    ]
  })
  // pi_2 3rd event (supports Y1, proposes Y1, endorses Y1)
  .mergeEvent({
    eventHash: '2-3',
    to: '2',
    from: [
      '2',
      {nodeId: '1', hash: '1-2'},
      {nodeId: '3', hash: '3-2'}
    ]
  });

const ledgerNodeId = '2';
const input = {
  ledgerNodeId,
  history: figure1_11.getHistory({nodeId: ledgerNodeId}),
  electors: figure1_11.getElectors(),
  recoveryElectors: [],
  mode: 'first'
};

const display = {
  title: 'Figure 1.11',
  nodeOrder: ['1', 'b', 'b2', '2', '3']
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);

module.exports = {input, display};
