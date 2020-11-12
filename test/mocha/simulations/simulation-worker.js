/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const workerpool = require('workerpool');
const {load} = require('./simulation-01');

workerpool.worker({
  runSimulation: ({
    pipelineJs,
    user,
    witnessCount,
  }) => {
    return load({pipelineJs, user, witnessCount});
  }
});
