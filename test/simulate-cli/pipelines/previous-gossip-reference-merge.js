/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const gossipStrategy = require('../gossip-strategies/previous-peer');
const mergeStrategy = require('../merge-strategies/merge-strategy-reference');

module.exports.name = 'previous_gossip_reference_merge';
module.exports.pipeline = async function() {
  const count = 1;

  for(let i = 0; i < count; i++) {
    // await this.run({type: 'merge', fn: mergeStrategy});
    await this.run({type: 'gossip', fn: gossipStrategy.run});
  }
  await this.run({type: 'merge', fn: mergeStrategy.run});
  const consensusResults = await this.run({type: 'consensus'});
  if(consensusResults.consensus) {
    console.log(`Found Consensus - Node ${this.nodeId}`);
  }
};
