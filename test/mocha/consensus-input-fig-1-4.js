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
  // pi_1 1st event
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b', hash: 'yb'}
    ]
  })
  // pi_b forks and creates m1
  .addNode('b1', {isElector: false})
  .mergeEvent({
    eventHash: 'b1-1',
    forked: true,
    to: 'b1',
    from: [
      'b'
    ]
  })
  // pi_b forks and creates m2
  .addNode('b2', {isElector: false})
  .mergeEvent({
    eventHash: 'b2-1',
    forked: true,
    to: 'b2',
    from: [
      'b'
    ]
  })
  // pi_b forks and creates m3
  .addNode('b3', {isElector: false})
  .mergeEvent({
    eventHash: 'b3-1',
    forked: true,
    to: 'b3',
    from: [
      'b'
    ]
  })
  // pi_1 2nd event
  .mergeEvent({
    eventHash: '1-2',
    to: '1',
    from: [
      '1',
      {nodeId: 'b1', hash: 'b1-1'}
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
  // pi_b1 2nd event
  .mergeEvent({
    eventHash: 'b1-2',
    to: 'b1',
    from: [
      'b1'
    ]
  })
  // pi_b2 2nd event
  .mergeEvent({
    eventHash: 'b2-2',
    to: 'b2',
    from: [
      'b2'
    ]
  })
  // pi_b3 2nd event
  .mergeEvent({
    eventHash: 'b3-2',
    to: 'b3',
    from: [
      'b3'
    ]
  })
  // pi_2 1st event
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: 'b3', hash: 'b3-2'}
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
