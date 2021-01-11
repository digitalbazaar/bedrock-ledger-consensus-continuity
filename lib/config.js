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
cfg.routes.eventsValidation = cfg.routes.root + '/events-validation';
cfg.routes.gossip = cfg.routes.root + '/gossip';
cfg.routes.notify = cfg.routes.root + '/notify';

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

// options for events validation service
const eventsValidation = cfg.gossip.eventsValidation = {};
// the base url for the events validation service
eventsValidation.baseUrl = '';
// the https agent options to be used with the events validation service
eventsValidation.httpsAgentOpts = undefined;

// how long to wait (ms) before contacting the same peer again
cfg.gossip.coolDownPeriod = 250;
// this is used in the server to limit the number of events and event hashes
// returned in response to a single request; it must be at least as large as
// the maximum size for a single merge event to ensure all event hashes related
// to a full merge events can be sent (i.e., if a merge event can only descend
// from at most N parents, then this must be >= N)
cfg.gossip.maxEvents = 100;

cfg.consensus = {};
cfg.consensus.workerpool = {};
cfg.consensus.workerpool.enabled = false;
cc('ledger-consensus-continuity.consensus.workerpool.maxWorkers', () =>
  Math.max(os.cpus().length - 1, 1));

cfg.operations = {};
// ttl (sec) for cached counter; determines how long an operation will stay
// in the cache waiting to make it into a regular event... if this expires,
// then the operation gets dropped and will never get into the ledger
cfg.operations.counter = {ttl: 6000};

cfg.writer = {};
cfg.writer.maxEvents = 1000;

// common validation schemas
config.validation.schema.paths.push(
  path.join(__dirname, '..', 'schemas')
);
