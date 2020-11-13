/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const mongoClient = require('./mongo-client');
const promClient = require('./prometheus-client');
const Simulator = require('../tools/Simulator');

async function load({
  nosend,
  pipelineJs,
  user,
  witnessCount,
  run
}) {
  const creator = user;
  const pipelineApi = require(pipelineJs);
  const {pipeline, name} = pipelineApi;

  const simulator = new Simulator({
    name, creator, witnessCount, pipeline, run
  });

  const report = await simulator.start();
  const {graph} = simulator;

  const ledgerNodeId = '1';

  const input = {
    ledgerNodeId,
    history: graph.getHistory({nodeId: ledgerNodeId}),
    electors: graph.getElectors(),
    recoveryElectors: [],
    mode: 'first'
  };

  const display = {
    title: name,
    nodeOrder: ['0', '1', '2', '3']
  };

  const visualizer = {};
  for(const elector of graph.getElectors()) {
    const ledgerNodeId = elector.id;
    visualizer[ledgerNodeId] = {
      ledgerNodeId,
      history: graph.getHistory({nodeId: ledgerNodeId}),
      electors: graph.getElectors(),
      recoveryElectors: [],
      mode: 'first'
    };
  }

  if(!nosend) {
    await Promise.all([
      promClient.send({report}),
      mongoClient.send({payload: {report, visualizer}}),
    ]);
  }
  console.log(report);
  return {input, display, report};
}

module.exports = {load};
