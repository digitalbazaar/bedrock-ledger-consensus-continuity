/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

// const mongoClient = require('./mongo-client');
const promClient = require('./prometheus-client');
const Simulator = require('../mocha/tools/Simulator');

async function load({
  nosend,
  pipelineJs,
  user,
  nonwitnessCount = 0,
  witnessCount,
  run
}) {
  const creator = user;
  const pipelineApi = require(pipelineJs);
  const {pipeline, name} = pipelineApi;

  const nodeCount = nonwitnessCount + witnessCount;
  const simulator = new Simulator({
    name, creator, nodeCount, witnessCount, pipeline, run
  });

  const report = await simulator.start();
  const {graph} = simulator;

  const ledgerNodeId = '1';

  const input = {
    ledgerNodeId,
    history: graph.getHistory({nodeId: ledgerNodeId}),
    witnesses: graph.getWitnesses()
  };

  const display = {
    title: name,
    nodeOrder: ['0', '1', '2', '3']
  };

  const visualizer = {};
  for(const nodeId of graph.nodes.keys()) {
    visualizer[nodeId] = {
      ledgerNodeId: nodeId,
      history: graph.getHistory({nodeId}),
      witnesses: graph.getWitnesses()
    };
  }

  if(!nosend) {
    await Promise.all([
      promClient.send({report}),
      // mongoClient.send({payload: {report, visualizer}}),
    ]);
  }
  console.log('Simulation report', report);
  return {input, display, report};
}

module.exports = {load};
