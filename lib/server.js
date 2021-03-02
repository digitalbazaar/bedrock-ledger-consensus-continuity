/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _events = require('./events');
const _peers = require('./peers');
const _peerEvents = require('./peerEvents');
const _history = require('./history');
const _localPeers = require('./localPeers');
const {asyncHandler} = require('bedrock-express');
const bedrock = require('bedrock');
const bodyParser = require('body-parser');
const brLedgerNode = require('bedrock-ledger-node');
const brRest = require('bedrock-rest');
const {config} = bedrock;
const {callbackify} = require('util');
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
    routes.eventsQuery, brRest.when.prefers.ld,
    validate('continuity-server.getEvents'), asyncHandler(async (req, res) => {
      const localPeerId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
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
    routes.eventsValidation, brRest.when.prefers.ld,
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
    routes.gossip, brRest.when.prefers.ld,
    validate('continuity-server.gossip'), brRest.linkedDataHandler({
      // eslint-disable-next-line
      get: callbackify(async (req, res) => {
        const localPeerId = config.server.baseUri +
          '/consensus/continuity2017/voters/' + req.params.voterId;
        const {body: remoteInfo} = req;
        // `basisBlockHeight` is the last block that has reached consensus
        //   on the client
        // `localEventNumber` is optional, only present if the client knows
        //   the next `localEventNumber` to request from the server
        // `peerHeads` all of the non-consensus peer heads the client knows
        //   referenced by merge event hash
        // `peerId` the client's peer ID
        const ledgerNodeId = await _localPeers.getLedgerNodeId(
          {peerId: localPeerId});
        const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
        // clear the notifyFlag to indicate that a gossip session has occurred
        await _cache.gossip.notifyFlag({ledgerNodeId, remove: true});

        // return a partition of the DAG history appropriate for the request
        const [result, samplePeers] = await Promise.all([
          _history.partition({ledgerNode, remoteInfo}),
          _peers.samplePeers({ledgerNode})
        ]);
        result.samplePeers = [];
        for(const peer of samplePeers) {
          const {id, url} = peer;
          result.samplePeers.push({id, url});
        }
        return result;
      })
    }));

  app.post(
    routes.notify, brRest.when.prefers.ld,
    validate('continuity-server.notification'),
    asyncHandler(async (req, res) => {
      const {peer} = req.body;
      const localPeerId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const ledgerNodeId = await _localPeers.getLedgerNodeId(
        {peerId: localPeerId});
      const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
      // FIXME: we need to authenticate `peerId`; perhaps http-signatures
      // should be used on notification endpoints as the simplest method;
      // however, that would not cover indirect sharing of peer URLs elsewhere
      // ... though a signature on shared URLs may end up not being a
      // requirement w/attempting to gossip with the peer may be sufficient
      // in that scenario since it has to happen anyway
      await _peers.addNotifier({ledgerNode, remotePeer: peer});
      // FIXME: remove redis usage
      await _cache.gossip.addNotification({ledgerNodeId, peerId: peer.id});
      res.status(204).end();
    }));
});
