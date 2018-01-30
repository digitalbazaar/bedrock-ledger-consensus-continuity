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
cfg.gossip.concurrentPeers = 3;
cfg.gossip.requestPool = {};
cfg.gossip.requestPool.maxSockets = 100;
cfg.gossip.cache = {};
cfg.gossip.cache.enabled = false;
cfg.gossip.cache.ttl = 5;
cfg.gossip.compression = {};
cfg.gossip.compression.threshold = 100;
cfg.gossip.compression.maxEvents = 10000;

cfg.consensus = {};
cfg.consensus.workerpool = {};
cfg.consensus.workerpool.enabled = false;//true;
cc('ledger-consensus-continuity.consensus.workerpool.maxWorkers', () =>
  Math.max(os.cpus().length - 1, 1));

// common validation schemas
config.validation.schema.paths.push(
  path.join(__dirname, '..', 'schemas')
);
