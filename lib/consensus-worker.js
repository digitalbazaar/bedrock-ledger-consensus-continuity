/*!
 * Copyright (c) 2018-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const workerpool = require('workerpool');
const consensus = require('./consensus');

workerpool.worker({
  findConsensus: ({
    ledgerNodeId, history, blockHeight, electors,
    recoveryElectors, recoveryGenerationThreshold, mode,
    logger
  }) => {
    return consensus.findConsensus({
      ledgerNodeId, history, blockHeight, electors,
      recoveryElectors, recoveryGenerationThreshold, mode,
      logger
    });
  }
});
