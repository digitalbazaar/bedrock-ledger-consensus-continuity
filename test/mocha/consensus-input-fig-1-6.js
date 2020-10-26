/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Graph = require('./tools/Graph');

const figure1_6 = new Graph();

// create initial nodes
figure1_6
  .addNode('1')
  .addNode('2')
  .addNode('3')
  .addNode('pi')
  .addNode('4')
  .addNode('5')
  .addNode('6');

figure1_6
  // pi_1 y1
  .mergeEvent({eventHash: 'y1', to: '1', from: []})
  // pi_2 y2
  .mergeEvent({eventHash: 'y2', to: '2', from: []})
  // pi_3 y3
  .mergeEvent({eventHash: 'y3', to: '3', from: []})
  // pi ypi
  .mergeEvent({eventHash: 'ypi', to: 'pi', from: []})
  // pi_4 y4
  .mergeEvent({eventHash: 'y4', to: '4', from: []})
  // pi_5 y5
  .mergeEvent({eventHash: 'y5', to: '5', from: []})
  // pi_6 y6
  .mergeEvent({eventHash: 'y6', to: '6', from: []})
  // pi 1st event (merge event m on node pi)
  .mergeEvent({
    eventHash: 'pi-1',
    to: 'pi',
    from: [
      'pi'
    ]
  })
  // pi_1 1st event
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'pi', hash: 'pi-1'}
    ]
  })
  // pi_3 1st event
  .mergeEvent({
    eventHash: '3-1',
    to: '3',
    from: [
      '3',
      {nodeId: 'pi', hash: 'pi-1'}
    ]
  })
  // pi_2 1st event (merge event m_1)
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: '3', hash: '3-1'}
    ]
  })
  // pi_4 1st event
  .mergeEvent({
    eventHash: '4-1',
    to: '4',
    from: [
      '4',
      {nodeId: 'pi', hash: 'pi-1'}
    ]
  })
  // pi_5 1st event
  .mergeEvent({
    eventHash: '5-1',
    to: '5',
    from: [
      '5',
      {nodeId: 'pi', hash: 'pi-1'}
    ]
  })
  // pi 2nd event (pi's endorsement e_m of merge event m on node pi)
  .mergeEvent({
    eventHash: 'pi-2',
    to: 'pi',
    from: [
      'pi',
      {nodeId: '1', hash: '1-1'},
      {nodeId: '2', hash: '2-1'},
      {nodeId: '4', hash: '4-1'},
      {nodeId: '5', hash: '5-1'}
    ]
  })
  // pi_2 2nd event (merge event m_2)
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: 'pi', hash: 'pi-2'}
    ]
  })
  // pi_6 1st event (merge event m_3)
  .mergeEvent({
    eventHash: '6-1',
    to: '6',
    from: [
      '6',
      {nodeId: 'pi', hash: 'pi-2'}
    ]
  });

const ledgerNodeId = '6';
const input = {
  ledgerNodeId,
  history: figure1_6.getHistory({nodeId: ledgerNodeId}),
  electors: figure1_6.getElectors(),
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);

module.exports = input;
