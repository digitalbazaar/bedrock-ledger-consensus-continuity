/*!
 * Copyright (c) 2018-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const workerpool = require('workerpool');
const continuity = require('./continuity');

workerpool.worker({
  findConsensus: ({
    ledgerNodeId, history, blockHeight, witnesses, logger
  }) => {
    return continuity.findConsensus({
      ledgerNodeId, history, blockHeight, witnesses, logger
    });
  }
});
