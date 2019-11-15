/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const {callbackify} = bedrock.util;
const logger = require('./logger');
const {promisify} = require('util');
const ConsensusAgent = require('./agents/consensus-agent');
const EventWriter = require('./agents/event-writer-agent');
const GossipAgent = require('./agents/gossip-agent');
const MergeAgent = require('./agents/merge-agent');
const OperationAgent = require('./agents/operation-agent');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

// exposed for testing
api.EventWriter = EventWriter;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('./temporaryLedgerNodeMeta');

api.scheduleWork = ({session}) => {
  // start a consensus session for ledgers
  const maxTime =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  session.start(maxTime, _guardedSync)
    .catch(err => {
      logger.error('Error starting consensus job.', {error: err});
    });
};

// Note: exposed for testing
api._run = callbackify(async ledgerNode => {
  // this gives test worker approx 5500ms per cycle
  const endTime = Date.now() + 10500;
  return _sync({
    ledgerNode,
    isExpired: () => false,
    timeRemaining: () => {
      const x = endTime - Date.now();
      return x;
    }
  });
});

async function _guardedSync(session) {
  // do not allow sync until `waitUntil` time
  const meta = await _ledgerNodeMeta.get(session.ledgerNode.id);
  const waitUntil = _.get(meta, 'consensus-continuity.waitUntil');
  if(waitUntil && waitUntil > Date.now()) {
    // do not run consensus yet
    logger.verbose('consensus job delaying until ' + new Date(waitUntil),
      {ledgerNodeId: session.ledgerNode.id, waitUntil});
    return;
  }
  // ready to run consensus
  return _sync(session);
}

async function _sync(session) {
  const {ledgerNode} = session;

  logger.verbose('consensus job running', {ledgerNodeId: ledgerNode.id});

  const eventWriter = new EventWriter({ledgerNode});
  const consensusAgent = new ConsensusAgent({ledgerNode});
  const gossipAgent = new GossipAgent({ledgerNode});
  const mergeAgent = new MergeAgent({gossipAgent, ledgerNode});
  const operationAgent = new OperationAgent({ledgerNode});

  const workerInterval = setInterval(() => {
    if(session.timeRemaining() < 5000) {
      stopAgents();
    }
  }, 1000);

  try {
    await Promise.all([
      eventWriter.start(),
      gossipAgent.start(),
      mergeAgent.start(),
      consensusAgent.start(),
      operationAgent.start()
    ]);
  } catch(e) {
    stopAgents();
    throw e;
  } finally {
    logger.verbose('Work session completed.', {
      session: session.id
    });
  }

  function stopAgents() {
    clearInterval(workerInterval);
    operationAgent.stop();
    mergeAgent.stop();
    gossipAgent.stop();
    consensusAgent.stop();
    eventWriter.stop();
  }
}
