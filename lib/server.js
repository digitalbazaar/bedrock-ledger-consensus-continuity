/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _events = require('./events');
const _history = require('./history');
const _peers = require('./peers');
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

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-express.configure.bodyParser', app => {
  app.use(bodyParser.json({limit: '1mb', type: ['json', '+json']}));
});

bedrock.events.on('bedrock-express.configure.routes', app => {
  const routes = config['ledger-consensus-continuity'].routes;

  // Get events
  app.post(
    routes.eventsQuery, brRest.when.prefers.ld,
    validate('continuity-server.getEvents'), asyncHandler(async (req, res) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const ledgerNodeId = await _peers.getLedgerNodeId(voterId);
      const {eventHash} = req.body;
      const events = await _events.getEventsForGossip(
        {eventHash, ledgerNodeId});
      for(const event of events) {
        res.write(`${event}\n`);
      }
      res.end();
    }));

  app.post(
    routes.gossip, brRest.when.prefers.ld,
    validate('continuity-server.gossip'), brRest.linkedDataHandler({
      // eslint-disable-next-line
      get: callbackify(async (req, res) => {
        const localPeerId = config.server.baseUri +
          '/consensus/continuity2017/voters/' + req.params.voterId;
        const {
          body: {blockHeight, peerHeads, peerId: remotePeerId}
        } = req;
        const ledgerNodeId = await _peers.getLedgerNodeId(localPeerId);
        const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
        // clear the notifyFlag to indicate that a gossip session has occurred
        await _cache.gossip.notifyFlag({ledgerNodeId, remove: true});

        // return a partition of the DAG history appropriate for the request
        return _history.partition({
          ledgerNode, remoteInfo: {blockHeight, peerHeads, peerId: remotePeerId}
        });
      })
    }));

  app.post(
    routes.notify, brRest.when.prefers.ld,
    validate('continuity-server.notification'),
    asyncHandler(async (req, res) => {
      const {peerId} = req.body;
      const receiverId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const ledgerNodeId = await _peers.getLedgerNodeId(receiverId);
      await _cache.gossip.addNotification({ledgerNodeId, peerId});
      res.status(204).end();
    }));
});
