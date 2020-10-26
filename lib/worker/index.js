/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _voters = require('../voters');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config, util: {callbackify}} = bedrock;
const logger = require('../logger');
const {extendBlockchain} = require('./consensus');
const {merge} = require('./merge');
const {runGossipCycle, sendNotification} = require('./gossip');
const EventWriter = require('./EventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

// load config defaults
require('../config');

// module API
const api = {};
module.exports = api;

// exposed for testing
api.EventWriter = EventWriter;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('../temporaryLedgerNodeMeta');

const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;

api.scheduleWork = ({session}) => {
  // start a consensus session for ledgers
  logger.verbose('start a consensus session for ledgers');
  const maxTime =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  session.start(maxTime, _guardedSync)
    .catch(err => {
      logger.verbose('error starting consensus job', {error: err});
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
    timeRemaining: () => endTime - Date.now()
  });
});

async function _guardedSync(session) {
  // do not allow sync until `waitUntil` time
  try {
    const meta = await _ledgerNodeMeta.get(session.ledgerNode.id);
    const waitUntil = _.get(meta, 'consensus-continuity.waitUntil');
    logger.verbose(`wait until ${waitUntil} > ${Date.now()} is` +
      ` ${waitUntil <= Date.now()}`);
    if(waitUntil && waitUntil > Date.now()) {
      // do not run consensus yet
      logger.verbose('consensus job delaying until ' + new Date(waitUntil),
        {ledgerNodeId: session.ledgerNode.id, waitUntil});
      return;
    }
    // ready to run consensus
    return _sync(session);
  } catch(e) {
    logger.verbose('Failed to get ledger node meta...', {error: e});
    // FIXME: We should handle the error properly and gracefully shutdown
    throw e;
  }
}

async function _sync(session) {
  const {ledgerNode} = session;
  const ledgerNodeId = ledgerNode.id;

  logger.verbose('consensus job running', {ledgerNodeId});

  // listen for new local operation notifications
  let resume = () => {};
  const subscriber = new cache.Client().client;
  try {
    await subscriber.subscribe(`continuity2017|needsMerge|${ledgerNodeId}`);
    subscriber.on('message', () => resume());
  } catch(e) {
    subscriber.quit();
    logger.verbose(
      'Work session failed, could not subscribe to new pub/sub messages.',
      {session: session.id});
    return;
  }

  try {
    // halt if insufficient time remaining in work session
    const halt = () => session.timeRemaining() < 5000;

    const creatorId = (await _voters.get({ledgerNodeId})).id;
    const peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
    const eventWriter = new EventWriter({ledgerNode});
    let needsGossip = true;

    // FIXME: determine if this extra event write is necessary to fix
    // stuck nodes that cannot complete gossip step and thus never write
    // cached events to disk
    // commit any previously cached events to mongo
    await eventWriter.write();

    // run consensus/gossip/merge pipeline until work session expires
    let mergePermits = 0;
    let priorityPeers;
    let halted = false;
    while(!halt()) {
      // 1. if no `mergePermits` are available, extend blockchain until can't
      // anymore
      if(mergePermits <= 0) {
        ({priorityPeers, mergePermits, halted} = await extendBlockchain(
          {ledgerNode, halt}));
      }

      // work session expired
      if(halted) {
        logger.verbose('work session expired (1)');
        break;
      }

      // 2. gossip until `mergePermits - 1` permits are used, saving one for
      // a local merge event
      let {mergePermitsConsumed} = await runGossipCycle({
        ledgerNode, priorityPeers, creatorId, peerSelector,
        mergePermits: mergePermits - 1, needsGossip, halt
      });

      // work session expired
      if(halt()) {
        logger.verbose('work session expired (2)');
        break;
      }

      // 3. commit all cached events to mongo
      await eventWriter.write();

      // 4. merge if possible
      const {merged, hasOutstandingOperations} = await merge(
        {ledgerNode, creatorId, priorityPeers, halt});
      needsGossip = hasOutstandingOperations;

      // determine if peers need to be notified
      let notify;
      try {
        if(merged) {
          logger.verbose('add notify flag', {ledgerNodeId});
          mergePermitsConsumed++;
          await _cache.gossip.notifyFlag({add: true, ledgerNodeId});
          notify = true;
        } else {
          notify = (await _cache.gossip.notifyFlag({ledgerNodeId})) !== null;
        }
      } catch(e) {
        logger.verbose(
          'Error adding notify flag.',
          {error: e});
      }

      if(!halt() && notify) {
        // notify peers of new/previous merge event
        try {
          logger.verbose('send notification', {creatorId});
          await sendNotification({creatorId, priorityPeers, peerSelector});
        } catch(e) {
          logger.verbose(
            'An error occurred while attempting to send merge notification.',
            {error: e});
          // just log the error, another attempt will be made on the next cycle
          logger.error(
            'An error occurred while attempting to send merge notification.',
            {error: e});
        }
      }

      // if no merge permits were consumed, run consensus again in case
      // priority peers have changed
      if(mergePermitsConsumed === 0) {
        mergePermits = 0;
      } else {
        // decrement by consumed permits
        mergePermits -= mergePermitsConsumed;
      }

      // if no merge permits were consumed and no gossip is needed, delay for
      // cool down period or until a peer notification or a local operation
      // notification comes in
      if(!halt() && mergePermitsConsumed === 0 && !needsGossip) {
        logger.verbose('delay for cool down period or until a peer ' +
          'notification or a local operation');
        await new Promise(resolve => {
          resume = resolve;
          setTimeout(() => resolve(), coolDownPeriod);
        });
      }
    }
  } finally {
    // unsubscribe from new operation messages
    subscriber.quit();
    logger.verbose('Work session completed.', {
      session: session.id
    });
  }
}
