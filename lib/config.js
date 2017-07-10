/*!
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const config = require('bedrock').config;
const cc = bedrock.util.config.main.computer();

require('bedrock-ledger-agent');

const cfg = config['ledger-continuity'] = {};

// TODO: add more routes
cfg.routes = {};
cc('ledger-continuity.routes.status', () =>
  config['ledger-agent'].routes.agent + '/continuity2017/status');
cc('ledger-continuity.routes.event', () =>
  config['ledger-agent'].routes.agent + '/continuity2017/events/:eventHash');
cc('ledger-continuity.routes.manifest', () =>
  config['ledger-agent'].routes.agent +
    '/continuity2017/manifests/:manifestHash');

cfg.keyParameters = {
  RSA: {
    modulusBits: 2048
  }
};

cfg.gossip = {};
cfg.gossip.requestPool = {};
cfg.gossip.requestPool.maxSockets = 1000;
