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
    // 5 minutes
    maxTime: 5 * 60 * 1000
  }
};

cfg.client = {};
// connection timeout in ms
cfg.client.timeout = 2500;

cfg.gossip = {};

const backoff = cfg.gossip.backoff = {};
// starting backoff period ms
backoff.min = 5000;
// backoff multiplier, used to multiply backoff minimum by consecutive failures
// to contact a peer to determine next backoff (up to max backoff)
backoff.factor = 2;
// maximum backoff to employ (wait at most 5 minutes before trying to contact
// a peer again)
backoff.max = 5 * 60 * 1000;

// maximum time spent backing off a max-reputation peer to reach a reputation
// of `0`; i.e., ideally takes 48 hours for a peer with 100 rep to get removed
// from a ledger node's peers collection
backoff.maxGracePeriod = 48 * 60 * 60 * 1000;

// options for events validation service
const eventsValidation = cfg.gossip.eventsValidation = {};
// the max timeout to validate a single operation
eventsValidation.timeout = 5000;
// the base url for the events validation service
eventsValidation.baseUrl = '';
// the https agent options to be used with the events validation service
eventsValidation.httpsAgentOpts = undefined;
// the max number of events a given process will validate concurrently
eventsValidation.concurrency = 10;
// FIXME: this value is dynamic and represents the active number of workers
//        validating events
eventsValidation.workers = 10;

// options for notifications
const notification = cfg.gossip.notification = {};
// the max number of jobs that can add a new peer received via notification
notification.addNewPeerConcurrency = 10;

// how long to wait (ms) before contacting the same peer again
cfg.gossip.coolDownPeriod = 250;
// this is used in the server to limit the number of events and event hashes
// returned in response to a single request; it must be at least as large as
// the maximum size for a single merge event to ensure all event hashes related
// to a full merge events can be sent (i.e., if a merge event can only descend
// from at most N parents, then this must be >= N); this is set to a multiple
// of `16` based on the continuity constants making 16 the max merge event size
cfg.gossip.maxEvents = 64;
cfg.gossip.peerSamplingCache = {
  max: 1000,
  // ensure peers that samples are taken from are updated every 30 seconds
  maxAge: 30 * 1000
};

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
// the maximum number of operations to buffer before rejection
cfg.operations.maxQueueSize = 3750;

// common validation schemas
config.validation.schema.paths.push(
  path.join(__dirname, '..', 'schemas')
);
