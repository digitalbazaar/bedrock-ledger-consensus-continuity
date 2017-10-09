/*!
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const config = require('bedrock').config;
const path = require('path');

const cfg = config['ledger-consensus-continuity'] = {};

cfg.routes = {};
cfg.routes.root = '/consensus/continuity2017/voters/:voterId';
cfg.routes.blockStatus = cfg.routes.root + '/blocks/:blockHeight/status';
cfg.routes.events = cfg.routes.root + '/events';
cfg.routes.manifests = cfg.routes.root + '/manifests';
cfg.routes.votes = cfg.routes.root + '/votes';

cfg.keyParameters = {
  RSA: {
    modulusBits: 2048
  }
};

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
cfg.gossip.requestPool.maxSockets = 1000;

// common validation schemas
config.validation.schema.paths.push(
  path.join(__dirname, '..', 'schemas')
);
