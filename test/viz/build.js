/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const consensusApi =
  require('bedrock-ledger-consensus-continuity/lib/consensus');
const fs = require('fs').promises;
const path = require('path');

const {consensusInput} = require('../mocha/mock.data');

const vizHelpers = require('./viz-helpers.js');

const outputDirectory = path.join(__dirname, 'data');

async function main() {
  // save the known 'input' style histories.
  console.log('SAVING INPUT DATA');

  //console.log('INPUT', consensusInput);

  // make a data dir
  await fs.mkdir(outputDirectory, {recursive: true});

  // track data for indexes
  const inputInfo = [];
  const outputInfo = [];

  for(const [id, data] of Object.entries(consensusInput)) {
    //console.log(`INPUT[${id}]', data);
    const inputResult = await vizHelpers.saveTestInputDataForD3({
      directory: outputDirectory,
      // ledger history for d3
      tag: 'lh-d3',
      historyId: id,
      nodeId: data.ledgerNodeId,
      history: data.history
    });
    // record filenames
    inputInfo.push({
      label: `history="${id}" node="${data.ledgerNodeId}"`,
      url: path.join('data', path.basename(inputResult.filename))
    });

    //console.log('INPUT', input);
    const consensusResult = consensusApi.findConsensus(data);
    console.log('RESULT', consensusResult);
    const outputResult = await vizHelpers.saveTestOutputDataForTimeline({
      directory: outputDirectory,
      // ledger history for timeline
      tag: 'lh-tl',
      historyId: id,
      nodeId: data.ledgerNodeId,
      history: data.history,
      consensus: consensusResult
    });
    // record filenames
    outputInfo.push({
      label: `history="${id}" node="${data.ledgerNodeId}"`,
      url: path.join('data', path.basename(outputResult.filename))
    });
  }

  // save test input index for D3
  await vizHelpers.saveIndexJS({
    directory: outputDirectory,
    tag: 'lh-d3',
    jsName: '_indexForD3',
    info: inputInfo
  });

  // save test output index for Timeline
  await vizHelpers.saveIndexJS({
    directory: outputDirectory,
    tag: 'lh-tl',
    jsName: '_indexForTimeline',
    info: outputInfo
  });
}

main().catch(e => console.error(e));
