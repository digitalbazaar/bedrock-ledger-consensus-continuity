/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const gossipStrategy = require('../gossip-strategies/random');
const mergeStrategy = require('../merge-strategies/threshold-merge');

// NOTE: no spaces allowed, must be safe for prometheus metrics
module.exports.name = 'grandom-mthreshold-wm1-pm0';
module.exports.pipeline = async function() {
  const count = 2;

  for(let i = 0; i < count; i++) {
    await this.run({type: 'gossip', fn: gossipStrategy.run});
  }
  const mergeArgs = {
    witnessTargetThreshold: '2f',
    witnessMinimumThreshold: '1',
    peerMinimumThreshold: '0',
    operationReadyChance: 0.2
  };
  await this.run({type: 'merge', fn: mergeStrategy.run, args: mergeArgs});
  const consensusResults = await this.run({type: 'consensus'});
  if(consensusResults.consensus) {
    console.log(`Found Consensus - Node ${this.nodeId}`);
  }
};
