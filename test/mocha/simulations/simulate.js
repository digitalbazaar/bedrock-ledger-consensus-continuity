#!/usr/bin/env node

'use strict';

const {load} = require('./simulation-01');
const path = require('path');
const workerpool = require('workerpool');
const yargs = require('yargs');

main().catch(console.error);

async function main() {
  yargs
    .help('help', 'Show help.')
    .option('name', {
      describe: 'The pet name for the simulation.',
      alias: 'n',
    })
    .option('pipelineJs', {
      describe: 'Path to the pipeline.',
      alias: 'p',
      default: './pipeline-reference.js',
    })
    .option('trials', {
      describe: 'The number of trials.',
      alias: 't',
      default: 1,
    })
    .option('user', {
      describe: 'The user.',
      alias: 'u',
    })
    .option('witnessCount', {
      describe: 'Number of witnesses.',
      alias: 'w',
      default: 4,
    });

  const {pipelineJs, trials, user, witnessCount} = yargs.argv;
  if(!(trials && user && witnessCount)) {
    throw new Error('Ensure proper options are specified. See --help.');
  }

  const simulationOptions = {pipelineJs, user, witnessCount};

  if(trials === 1) {
    // do not use workerpool for a single trial, allows for profiling
    return load(simulationOptions);
  }

  const simulationPool = workerpool.pool(
    path.join(__dirname, 'simulation-worker.js'));
  const simulationWorker = await simulationPool.proxy();

  const promises = [];
  for(let i = 0; i < trials; ++i) {
    promises.push(simulationWorker.runSimulation(simulationOptions));
  }

  await Promise.all(promises);

  simulationPool.terminate();
}
