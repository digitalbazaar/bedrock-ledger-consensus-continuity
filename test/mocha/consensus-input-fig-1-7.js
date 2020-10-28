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
  .addNode('alpha', {isElector: false});

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
  // pi_b 1st event
  .mergeEvent({
    eventHash: 'b-1',
    to: 'b',
    from: [
      'b'
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
  // b1 2nd event
  .mergeEvent({
    eventHash: 'b1-2',
    to: 'b1',
    from: [
      'b1'
    ]
  })
  // b2 creates merge event from pi_alpha and self parent and supports Y2
  .mergeEvent({
    eventHash: 'b2-2',
    to: 'b2',
    from: [
      'b2',
      {nodeId: 'alpha', hash: 'yalpha'}
    ]
  })
  // pi_1 1st event (merge event m, supports Y1)
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'b1', hash: 'b1-2'}
    ]
  })
  // pi_h 1st event (merge event m1)
  .mergeEvent({
    eventHash: 'h-1',
    to: 'h',
    from: [
      'h',
      {nodeId: '1', hash: '1-1'}
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
      {nodeId: 'b2', hash: 'b2-2'}
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

const display = {
  title: 'Figure 1.7',
  nodeOrder: ['1', 'h', 'b', 'b1', 'b2', 'alpha', '2']
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);

module.exports = {input, display};
