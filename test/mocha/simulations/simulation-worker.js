/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const workerpool = require('workerpool');
const {load} = require('./simulation-01');

workerpool.worker({
  runSimulation: ({
    user,
    witnessCount,
  }) => {
    console.log('111111111', witnessCount);
    return load({user, witnessCount});
  }
});
