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
  .addNode('pi')
  .addNode('4')
  .addNode('5')
  .addNode('6');

graph
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
  // pi_1 1st "extra" history event
  .mergeEvent({
    eventHash: '1-xt-1',
    to: '1',
    from: [
      '1',
      {nodeId: '2', eventHash: 'y2'},
      {nodeId: '3', eventHash: 'y3'},
      {nodeId: 'pi', eventHash: 'ypi'},
      {nodeId: '4', eventHash: 'y4'},
      {nodeId: '5', eventHash: 'y5'},
    ]
  })
  // pi_2 1st "extra" history event
  .mergeEvent({
    eventHash: '2-xt-1',
    to: '2',
    from: [
      '2',
      {nodeId: '1', eventHash: '1-xt-1'}
    ]
  })
  // pi_3 1st "extra" history event
  .mergeEvent({
    eventHash: '3-xt-1',
    to: '3',
    from: [
      '3',
      {nodeId: '2', eventHash: '2-xt-1'}
    ]
  })
  // pi_4 1st "extra" history event
  .mergeEvent({
    eventHash: '4-xt-1',
    to: '4',
    from: [
      '4',
      {nodeId: '3', eventHash: '3-xt-1'}
    ]
  })
  // pi_5 1st "extra" history event
  .mergeEvent({
    eventHash: '5-xt-1',
    to: '5',
    from: [
      '5',
      {nodeId: '4', eventHash: '4-xt-1'}
    ]
  })
  // pi 1st event (merge event m on node pi)
  .mergeEvent({
    eventHash: 'pi-1',
    to: 'pi',
    from: [
      'pi',
      {nodeId: '5', eventHash: '5-xt-1'}
    ]
  })
  // pi_1 1st event
  .mergeEvent({
    eventHash: '1-1',
    to: '1',
    from: [
      '1',
      {nodeId: 'pi', eventHash: 'pi-1'}
    ]
  })
  // pi_3 1st event
  .mergeEvent({
    eventHash: '3-1',
    to: '3',
    from: [
      '3',
      {nodeId: 'pi', eventHash: 'pi-1'}
    ]
  })
  // pi_2 1st event (merge event m_1)
  .mergeEvent({
    eventHash: '2-1',
    to: '2',
    from: [
      '2',
      {nodeId: '3', eventHash: '3-1'}
    ]
  })
  // pi_4 1st event
  .mergeEvent({
    eventHash: '4-1',
    to: '4',
    from: [
      '4',
      {nodeId: 'pi', eventHash: 'pi-1'}
    ]
  })
  // pi_5 1st event
  .mergeEvent({
    eventHash: '5-1',
    to: '5',
    from: [
      '5',
      {nodeId: 'pi', eventHash: 'pi-1'}
    ]
  })
  // pi 2nd event (pi's endorsement e_m of merge event m on node pi)
  .mergeEvent({
    eventHash: 'pi-2',
    to: 'pi',
    from: [
      'pi',
      {nodeId: '1', eventHash: '1-1'},
      {nodeId: '2', eventHash: '2-1'},
      {nodeId: '4', eventHash: '4-1'},
      {nodeId: '5', eventHash: '5-1'}
    ]
  })
  // pi_2 2nd event (merge event m_2)
  .mergeEvent({
    eventHash: '2-2',
    to: '2',
    from: [
      '2',
      {nodeId: 'pi', eventHash: 'pi-2'}
    ]
  })
  // pi_6 1st event (merge event m_3)
  .mergeEvent({
    eventHash: '6-1',
    to: '6',
    from: [
      '6',
      {nodeId: 'pi', eventHash: 'pi-2'}
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
  title: 'Figure 1.6',
  nodeOrder: ['1', '2', '3', 'pi', '4', '5', '6']
};

module.exports = {input, display, graph};
