/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const Simulator = require('../mocha/tools/Simulator');

const PIPELINE_FILE = './pipelines/pipeline-reference.js';
const WITNESS_COUNT = 31;

const USER = 'add-user';

async function load() {
  const creator = USER;
  const pipelineApi = require(PIPELINE_FILE);
  const {pipeline, name} = pipelineApi;

  const simulator = new Simulator({
    name, creator, witnessCount: WITNESS_COUNT, pipeline
  });

  const report = await simulator.start();
  const {graph} = simulator;

  const ledgerNodeId = '1';

  const input = {
    ledgerNodeId,
    history: graph.getHistory({nodeId: ledgerNodeId}),
    electors: graph.getWitnesses()
  };

  const display = {
    title: name,
    nodeOrder: ['0', '1', '2', '3']
  };

  console.log(report);
  return {input, display, report};
}

load();

module.exports = {load};
