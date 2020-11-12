/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const promClient = require('./prometheus-client');
const Simulator = require('../tools/Simulator');

async function load({
  pipelineJs,
  user,
  witnessCount,
}) {
  // FIXME: Get from params
  const id = 'simulation-01';
  const creator = user;
  const pipelineApi = require(pipelineJs);

  const simulator = new Simulator({
    id, creator, witnessCount, pipeline: pipelineApi.run
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

  await promClient.send({report});
  console.log(report);
  return {input, display, report};
}

module.exports = {load};
