/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _events = require('./events');
const _peers = require('./peers');
const _signature = require('./signature');
const _peerEvents = require('./peerEvents');
const _history = require('./history');
const _localPeers = require('./localPeers');
const {asyncHandler} = require('bedrock-express');
const bedrock = require('bedrock');
const bodyParser = require('body-parser');
const brLedgerNode = require('bedrock-ledger-node');
const {config} = bedrock;
const logger = require('./logger');
const {validate} = require('bedrock-validation');

require('bedrock-permission');

require('./config');

let _currentEventValidationJobs = 0;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-express.configure.bodyParser', app => {
  app.use(bodyParser.json({limit: '1mb', type: ['json', '+json']}));
});

bedrock.events.on('bedrock-express.configure.routes', app => {
  const cfg = config['ledger-consensus-continuity'];
  const {routes, gossip: {eventsValidation}} = cfg;

  const MAX_EVENT_VALIDATION_JOBS = eventsValidation.concurrency;

  // get events
  app.post(
    routes.eventsQuery, validate('continuity-server.getEvents'),
    asyncHandler(async (req, res) => {
      const localPeerId = `${config.server.baseUri}/consensus/continuity2017/` +
        `peers/${encodeURIComponent(req.params.peerId)}`;
      const ledgerNodeId = await _localPeers.getLedgerNodeId(
        {peerId: localPeerId});
      const {eventHash} = req.body;
      const events = await _events.getEventsForGossip(
        {eventHashes: eventHash, ledgerNodeId});
      for(const event of events) {
        res.write(`${event}\n`);
      }
      res.end();
    }));

  // validate event
  app.post(
    routes.eventsValidation,
    asyncHandler(async (req, res) => {
      if(_currentEventValidationJobs >= MAX_EVENT_VALIDATION_JOBS) {
        res.status(503).end();
        return;
      }

      _currentEventValidationJobs++;

      try {
        const {event, ledgerNodeId, session} = req.body;

        const validatedEvent = await _peerEvents.validateEvent({
          event,
          ledgerNodeId,
          session
        });

        res.json(validatedEvent);
      } finally {
        _currentEventValidationJobs--;
      }
    }));

  app.post(
    routes.gossip,
    validate('continuity-server.gossip'),
    asyncHandler(async (req, res) => {
      const localPeerId = `${config.server.baseUri}/consensus/` +
        `continuity2017/peers/${encodeURIComponent(req.params.peerId)}`;
      // `basisBlockHeight` is the last block that has reached consensus
      //   on the client
      // `localEventNumber` is optional, only present if the client knows
      //   the next `localEventNumber` to request from the server
      // `peerHeads` all of the non-consensus peer heads the client knows
      //   referenced by merge event hash
      // `peerId` the client's peer ID
      const {body: remoteInfo} = req;
      // return a partition of the DAG history appropriate for the request
      const ledgerNodeId = await _localPeers.getLedgerNodeId(
        {peerId: localPeerId});
      const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
      const [result, samplePeers] = await Promise.all([
        _history.partition({ledgerNode, remoteInfo}),
        _peers.samplePeers({ledgerNode, vetoPeerId: remoteInfo.peerId})
      ]);
      result.samplePeers = [];
      for(const peer of samplePeers) {
        const {id, url} = peer;
        result.samplePeers.push({id, url});
      }
      res.json(result);
    })
  );

  app.post(
    routes.notify,
    validate('continuity-server.notification'),
    asyncHandler(async (req, res) => {
      const {verified, keyId} = await _signature.verifyRequest({req});
      if(!(verified && req.body.peer.id === keyId)) {
        return res.status(403).end();
      }
      // queue handling notification and immediately send response, do not
      // wait for notification handling to complete
      _handleNotification({req}).catch(_logNotificationError);
      res.status(204).end();
    }));
});

async function _handleNotification({req}) {
  const {peer} = req.body;
  const localPeerId = `${config.server.baseUri}/consensus/continuity2017/` +
    `peers/${encodeURIComponent(req.params.peerId)}`;
  const ledgerNodeId = await _localPeers.getLedgerNodeId(
    {peerId: localPeerId});
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
  await _peers.addNotifier({ledgerNode, remotePeer: peer, localPeerId});
}

async function _logNotificationError(error) {
  logger.error('Failed to process notification', {error});
}
