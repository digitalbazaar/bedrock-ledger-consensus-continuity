/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
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
  },
  election: {
    // delay in ms between gossip sessions
    gossipInterval: 10000
  }
};

cfg.client = {};
// connection timeout in ms
cfg.client.timeout = 2500;

cfg.gossip = {};

const backoff = cfg.gossip.backoff = {};
backoff.min = 5000; // ms
backoff.factor = 2; // backoff multiplier

// maximum number of peers to gossip with at once
// not currently used, gossip occurs in series, there is no benefit in speaking
// to multiple peers simultaneously with current algo, just leads to dups
cfg.gossip.concurrentPeers = 3;
cfg.gossip.requestPool = {};

// this is an important setting, can't be too big
cfg.gossip.requestPool.maxSockets = 5;
cfg.gossip.cache = {};
cfg.gossip.cache.enabled = false;
cfg.gossip.cache.ttl = 5;

// how long to wait (ms) before contacting the same peer again
cfg.gossip.coolDownPeriod = 500;
// very important setting
cfg.gossip.maxDepth = 6;
// this is used in the server to limit the number of events requested in a
// single operation which is also a function of gossip.maxDepth
cfg.gossip.maxEvents = 250;

cfg.consensus = {};
// delay after event write in ms
cfg.consensus.debounce = 0;

cfg.consensus.workerpool = {};
cfg.consensus.workerpool.enabled = true;
cc('ledger-consensus-continuity.consensus.workerpool.maxWorkers', () =>
  Math.max(os.cpus().length - 1, 1));

cfg.events = {};
// ttl (sec) for cached counter
cfg.events.counter = {ttl: 6000};

// TODO: the limit here is aimed at preventing the event document
// containing dereferenced operations from exceeding mongo's 16MB limit
// a limit of 500 ops per event means ops can be ~32K on average
// the ops/event ratio can also be modulated by adjusting the
// `operations.debounce` config value

cfg.merge = {};
// use a fixed debounce (ms)
cfg.merge.fixedDebounce = 0;

cfg.operations = {};
// ttl (sec) for cached counter
cfg.operations.counter = {ttl: 6000};
cfg.operations.debounce = 0;

cfg.writer = {};
cfg.writer.debounce = 0;
cfg.writer.maxEvents = 1000;

// common validation schemas
config.validation.schema.paths.push(
  path.join(__dirname, '..', 'schemas')
);
