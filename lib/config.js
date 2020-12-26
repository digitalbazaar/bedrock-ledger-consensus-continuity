/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const config = bedrock.config;
const c = bedrock.util.config.main;
const cc = c.computer();
const path = require('path');
const os = require('os');

const cfg = config['ledger-consensus-continuity'] = {};

cfg.routes = {};
cfg.routes.root = '/consensus/continuity2017/voters/:voterId';
cfg.routes.eventsQuery = cfg.routes.root + '/events-query';
cfg.routes.gossip = cfg.routes.root + '/gossip';
cfg.routes.notify = cfg.routes.root + '/notify';

cfg.keyParameters = {};

cfg.worker = {
  session: {
    // 1 minute
    maxTime: 60 * 1000
  }
};

cfg.client = {};
// connection timeout in ms
cfg.client.timeout = 2500;

cfg.gossip = {};

const backoff = cfg.gossip.backoff = {};
// starting backoff period ms
backoff.min = 5000;
// backoff multiplier
backoff.factor = 2;

// options for gossip batch processing
const batchProcess = cfg.gossip.batchProcess = {};
// this Bedrock process should process gossip batches
batchProcess.enable = true;
// the number of gossip events to process concurrently
batchProcess.concurrentEventsPerWorker = 1;

// how long to wait (ms) before contacting the same peer again
cfg.gossip.coolDownPeriod = 500;
// this is used in the server to limit the number of events returned in
// response to a single request; it must be larger than the maximum size for
// a single merge event (i.e., if a merge event can only descend from at most
// N parents, then this must be > N)
cfg.gossip.maxEvents = 1000;

cfg.consensus = {};
cfg.consensus.workerpool = {};
cfg.consensus.workerpool.enabled = false;
cc('ledger-consensus-continuity.consensus.workerpool.maxWorkers', () =>
  Math.max(os.cpus().length - 1, 1));

// TODO: the limit here is aimed at preventing the event document
// containing dereferenced operations from exceeding mongo's 16MB limit
// a limit of 500 ops per event means ops can be ~32K on average
// the ops/event ratio can also be modulated by adjusting the
// `operations.debounce` config value

cfg.operations = {};
// ttl (sec) for cached counter
cfg.operations.counter = {ttl: 6000};
cfg.operations.debounce = 0;

cfg.writer = {};
cfg.writer.maxEvents = 1000;

// common validation schemas
config.validation.schema.paths.push(
  path.join(__dirname, '..', 'schemas')
);
