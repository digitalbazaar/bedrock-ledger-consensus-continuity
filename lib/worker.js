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
const ConsensusAgent = require('./agents/consensus-agent');
const EventWriter = require('./agents/event-writer-agent');
const GossipAgent = require('./agents/gossip-agent');

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
  // this gives test worker approx 500ms per cycle
  const endTime = Date.now() + 5500;
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
  // logger.debug(
  //   'consensus job blockCollection',
  //   {blockCollection: ledgerNode.storage.blocks.collection.s.name});

  const eventWriter = new EventWriter({ledgerNode});
  const gossipAgent = new GossipAgent({ledgerNode});
  const consensusAgent = new ConsensusAgent({gossipAgent, ledgerNode});

  async.parallel([
    callback => eventWriter.start(callback),
    callback => gossipAgent.start(callback),
    callback => consensusAgent.start(callback),
  ], err => {
    logger.verbose('Work session completed.', {
      session: session.id
    });
    callback(err);
  });

  const workerInterval = setInterval(() => {
    if(session.timeRemaining() < 5000) {
      clearInterval(workerInterval);
      gossipAgent.stop();
      consensusAgent.stop();
      eventWriter.stop();
    }
  }, 1000);
}
