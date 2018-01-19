/*
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
const workerpool = require('workerpool');
const consensus = require('./consensus');

workerpool.worker({
  findConsensus: consensus.findConsensus
});
