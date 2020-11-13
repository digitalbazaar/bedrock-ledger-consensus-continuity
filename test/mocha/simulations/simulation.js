/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

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
    title: 'Simulation 01',
    nodeOrder: ['0', '1', '2', '3']
  };

  if(!nosend) {
    await promClient.send({report});
  }
  console.log(report);
  return {input, display, report};
}

module.exports = {load};
