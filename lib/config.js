/*!
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const config = require('bedrock').config;

const cfg = config['ledger-continuity'] = {};

// TODO: add more routes
cfg.routes = {};
cfg.routes.root = '/consensus/continuity2017/voters/:voterId';
cfg.routes.blockStatus = cfg.routes.root + '/blocks/:blockHeight/status';
cfg.routes.events = cfg.routes.root + '/events';
cfg.routes.manifests = cfg.routes.root + '/manifests';

cfg.keyParameters = {
  RSA: {
    modulusBits: 2048
  }
};

cfg.worker = {
  session: {
    // 1 minute
    maxTime: 60 * 1000
  }
};

cfg.gossip = {};
// maximum number of peers to gossip with at once
cfg.gossip.concurrentPeers = 3;
cfg.gossip.requestPool = {};
cfg.gossip.requestPool.maxSockets = 1000;
