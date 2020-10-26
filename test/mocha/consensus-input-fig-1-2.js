/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./tools/Graph');

const figure1_2 = new Graph();

// create initial nodes
figure1_2
  .addNode('1')
  .addNode('2')
  .addNode('3')
  .addNode('4');

figure1_2
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
      {nodeId: '3', hash: 'y3'},
      {nodeId: '4', hash: 'y4'}
    ]
  })
  // pi_2 1st event
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: '1', hash: 'y1'},
      {nodeId: '3', hash: 'y3'},
      {nodeId: '4', hash: 'y4'}
    ]
  })
  // pi_3 1st event
  .mergeEvent({
    eventHash: '3-1',
    to: '3',
    from: [
      '3',
      {nodeId: '1', hash: 'y1'},
      {nodeId: '4', hash: 'y4'}
    ]
  })
  // pi_4 1st event
  .mergeEvent({
    eventHash: '4-1',
    to: '4',
    from: [
      '4',
      {nodeId: '1', hash: 'y1'},
      {nodeId: '3', hash: 'y3'}
    ]
  })
  // pi_4 2nd event
  .mergeEvent({
    eventHash: '4-2',
    to: '4',
    from: [
      '4',
      {nodeId: '2', hash: '2-1'}
    ]
  })
  // pi_2 2nd event
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: '1', hash: '1-1'},
      {nodeId: '4', hash: '4-1'}
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
      {nodeId: '2', hash: '2-1'}
    ]
  })
  // pi_4 3rd event
  .mergeEvent({
    eventHash: '4-3',
    to: '4',
    from: [
      '4',
      {nodeId: '3', hash: '3-2'}
    ]
  })
  // pi_3 3rd event
  .mergeEvent({
    eventHash: '3-3',
    to: '3',
    from: [
      '3',
      {nodeId: '1', hash: '1-2'},
      {nodeId: '4', hash: '4-3'}
    ]
  })
  // pi_2 3rd event
  .mergeEvent({
    eventHash: '2-3',
    to: '2',
    from: [
      '2',
      {nodeId: '3', hash: '3-2'}
    ]
  })
  // pi_1 3rd event
  .mergeEvent({
    eventHash: '1-3',
    to: '1',
    from: [
      '1',
      {nodeId: '2', hash: '2-3'},
      {nodeId: '3', hash: '3-3'}
    ]
  })
  // pi_4 4th event
  .mergeEvent({
    eventHash: '4-4',
    to: '4',
    from: [
      '4',
      {nodeId: '3', hash: '3-3'}
    ]
  })
  // pi_3 4th event (eY_2*)
  .mergeEvent({
    eventHash: '3-4',
    to: '3',
    from: [
      '3',
      {nodeId: '1', hash: '1-3'},
      {nodeId: '4', hash: '4-4'}
    ]
  })
  // pi_4 5th event (Decision)
  .mergeEvent({
    eventHash: '4-5',
    to: '4',
    from: [
      '4',
      {nodeId: '3', hash: '3-4'}
    ]
  });

const ledgerNodeId = '4';
const input = {
  ledgerNodeId,
  history: figure1_2.getHistory({nodeId: ledgerNodeId}),
  electors: figure1_2.getElectors(),
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);

module.exports = input;
