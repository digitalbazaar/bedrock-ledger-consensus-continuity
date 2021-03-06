/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const gossipStrategy = require('../gossip-strategies/reference');
const mergeStrategy =
  require('../merge-strategies/threshold-merge');

// NOTE: no spaces allowed, must be safe for prometheus metrics
module.exports.name = 'threshold-wt2f-wmf-pm1-or100';
module.exports.pipeline = async function() {
  const count = 2;

  for(let i = 0; i < count; i++) {
    await this.run({type: 'gossip', fn: gossipStrategy.run});
  }
  const mergeArgs = {
    witnessTargetThreshold: '2f',
    witnessMinimumThreshold: 'f',
    peerMinimumThreshold: '1',
    operationReadyChance: 1.0
  };
  await this.run({type: 'merge', fn: mergeStrategy.run, args: mergeArgs});
  const consensusResults = await this.run({type: 'consensus'});
  if(consensusResults.consensus) {
    console.log(`Found Consensus - Node ${this.nodeId}`);
  }
};
