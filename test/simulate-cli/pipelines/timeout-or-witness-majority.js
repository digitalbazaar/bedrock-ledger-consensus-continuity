/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const gossipStrategy = require('../gossip-strategies/previous-peer');
const mergeStrategy =
  require('../merge-strategies/timeout-or-witness-majority-merge');

// NOTE: no spaces allowed, must be safe for prometheus metrics
module.exports.name = 'non-zero-spam-cost';
module.exports.pipeline = async function() {
  const count = 1;

  for(let i = 0; i < count; i++) {
    await this.run({type: 'gossip', fn: gossipStrategy.run});
  }
  await this.run({type: 'merge', fn: mergeStrategy.run});
  const consensusResults = await this.run({type: 'consensus'});
  if(consensusResults.consensus) {
    console.log(`Found Consensus - Node ${this.nodeId}`);
  }
};
