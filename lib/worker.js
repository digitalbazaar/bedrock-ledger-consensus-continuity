/*
 * Web Ledger Continuity2017 consensus worker.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const _ = require('lodash');
const _gossip = require('./gossip');
const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const logger = require('./logger');
const ConsensusAgent = require('./consensus-agent');
const EventWriter = require('./event-writer');
const GossipAgent = require('./gossip-agent');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

api._cacheKey = require('./cache-key');
api._client = require('./client');
api._election = require('./election');
api._events = require('./events');
api._hasher = brLedgerNode.consensus._hasher;
api._storage = require('./storage');
api._voters = require('./voters');
// exposed for testing
api._gossipWith = _gossip.gossipWith;
api.EventWriter = EventWriter;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('./temporaryLedgerNodeMeta');

api.scheduleWork = (session) => {
  // start a consensus session for ledgers
  const maxTime =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  session.start(maxTime, _guardedSync, err => {
    if(err) {
      logger.error('Error starting consensus job.', err);
    }
  });
};

// Note: exposed for testing
api._run = (ledgerNode, callback) => {
  const endTime = Date.now() + 1500;
  _sync({
    ledgerNode: ledgerNode,
    isExpired: () => false,
    timeRemaining: () => endTime - Date.now()
  }, callback);
};

function _guardedSync(session, callback) {
  // do not allow sync until `waitUntil` time
  _ledgerNodeMeta.get(session.ledgerNode.id, (err, meta) => {
    if(err) {
      return callback(err);
    }
    const waitUntil = _.get(meta, 'consensus-continuity.waitUntil');
    if(waitUntil && waitUntil > Date.now()) {
      // do not run consensus yet
      logger.verbose('consensus job delaying until ' + new Date(waitUntil),
        {ledgerNodeId: session.ledgerNode.id, waitUntil: waitUntil});
      return callback();
    }
    // ready to run consensus
    _sync(session, callback);
  });
}

function _sync(session, callback) {
  const ledgerNode = session.ledgerNode;

  // const eventWriter = new EventWriter({ledgerNode});
  // eventWriter.start(session);

  logger.verbose('consensus job running', {ledgerNodeId: ledgerNode.id});
  logger.debug('consensus job ledgerNode', {ledgerNodeId: ledgerNode.id});
  logger.debug(
    'consensus job blockCollection',
    {blockCollection: ledgerNode.storage.blocks.collection.s.name});

  // start EventWriter
  const eventWriter = new EventWriter({ledgerNode});
  eventWriter.start();

  const gossipAgent = new GossipAgent({ledgerNode});
  gossipAgent.start();

  const consensusAgent = new ConsensusAgent({gossipAgent, ledgerNode});
  consensusAgent.start();

  const workerInterval = setInterval(() => {
    if(session.timeRemaining() < 1000) {
      clearInterval(workerInterval);
      async.series([
        callback => gossipAgent.stop(callback),
        callback => consensusAgent.stop(callback),
        callback => eventWriter.stop(callback),
      ], err => {
        logger.debug('Work session completed.', {
          session: session.id
        });
        callback(err);
      });
    }
  }, 1000);
}
