/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const consensusApi =
  require('bedrock-ledger-consensus-continuity/lib/consensus');
const fs = require('fs').promises;
const path = require('path');

const {consensusInput} = require('../mocha/mock.data');

const {
  saveInputVisualizationDataD3,
  saveInputVisualizationIndexesD3
} = require('./viz-helpers.js');

const outputDirectory = path.join(__dirname, 'data');
// tag as 'ledger history' => 'lh'
const outputTag = 'lh';

async function main() {
  // save the known 'input' style histories.
  console.log('SAVING INPUT DATA');

  //console.log('INPUT', consensusInput);

  // make a data dir
  await fs.mkdir(outputDirectory, {recursive: true});

  // track filenames for index
  const filenames = [];

  for(const [id, data] of Object.entries(consensusInput)) {
    //console.log(`INPUT[${id}]', data);
    const result = await saveInputVisualizationDataD3({
      directory: outputDirectory,
      tag: outputTag,
      historyId: id,
      nodeId: data.ledgerNodeId,
      history: data.history
    });
    // record filenames
    filenames.push(result);

    //console.log('INPUT', input);
    //const result = consensusApi.findConsensus(input);
    //console.log('RESULT', result);
  }

  // save index
  const result = await saveInputVisualizationIndexesD3({
    directory: outputDirectory,
    tag: outputTag,
    filenames: filenames.map(f => path.join('data', path.basename(f.filename)))
  });
}

main().catch(e => console.error(e));
