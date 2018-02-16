/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const config = bedrock.config;
const c = bedrock.util.config.main;
const cc = c.computer();
const path = require('path');
const os = require('os');

const cfg = config['ledger-consensus-continuity'] = {};

cfg.routes = {};
cfg.routes.root = '/consensus/continuity2017/voters/:voterId';
cfg.routes.events = cfg.routes.root + '/events';
cfg.routes.eventsQuery = cfg.routes.root + '/events-query';
cfg.routes.eventsCompressed = cfg.routes.root + '/events-compressed';
cfg.routes.gossip = cfg.routes.root + '/gossip';
cfg.routes.gossipCompressed = cfg.routes.root + '/gossip-compressed';
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
cfg.client.timeout = 60000;

cfg.gossip = {};
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

// compressed gossip is currently not being used
cfg.gossip.compression = {};
cfg.gossip.compression.threshold = 100;
cfg.gossip.compression.maxEvents = 10000;

// how long to wait after gossip has determined there is no more to do
cfg.gossip.coolDownPeriod = 20;
// very important setting
cfg.gossip.maxDepth = 6;

cfg.consensus = {};
// delay after event write in ms
cfg.consensus.debounce = 5000;

cfg.consensus.workerpool = {};
cfg.consensus.workerpool.enabled = true;
cc('ledger-consensus-continuity.consensus.workerpool.maxWorkers', () =>
  Math.max(os.cpus().length - 1, 1));

cfg.merge = {};
cfg.merge.debounce = 1000;
cfg.merge.maxEvents = 250;

cfg.writer = {};
cfg.writer.debounce = 500;
cfg.writer.maxEvents = 1000;

// common validation schemas
config.validation.schema.paths.push(
  path.join(__dirname, '..', 'schemas')
);
