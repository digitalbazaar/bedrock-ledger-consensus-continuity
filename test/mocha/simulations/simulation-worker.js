/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const workerpool = require('workerpool');
const {load} = require('./simulation');

workerpool.worker({
  runSimulation: options => load(options)
});
