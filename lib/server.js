/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _events = require('./events');
const _gossip = require('./gossip');
const _voters = require('./voters');
const {asyncHandler} = require('bedrock-express');
const bedrock = require('bedrock');
const bodyParser = require('body-parser');
const brRest = require('bedrock-rest');
const {config, util: {callbackify}} = bedrock;
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
      const ledgerNodeId = await _voters.getLedgerNodeId(voterId);
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
      get: callbackify(async req => {
        const creatorId = config.server.baseUri +
          '/consensus/continuity2017/voters/' + req.params.voterId;
        // the schema validator ensures that `creatorHeads` and `headsOnly` are
        // mutually exclusive
        const {body: {creatorHeads, headsOnly, peerId}} = req;
        const ledgerNodeId = await _voters.getLedgerNodeId(creatorId);
        // clear the notifyFlag to indicate that a gossip session has occurred
        await _cache.gossip.notifyFlag({ledgerNodeId, remove: true});

        if(headsOnly) {
          return _gossip.getHeads({creatorId});
        }

        return _events.partitionHistory({creatorHeads, creatorId, peerId});
      })
    }));

  app.post(
    routes.notify, brRest.when.prefers.ld,
    validate('continuity-server.notification'),
    asyncHandler(async (req, res) => {
      const {peerId} = req.body;
      const receiverId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const ledgerNodeId = await _voters.getLedgerNodeId(receiverId);
      await _cache.gossip.addNotification({ledgerNodeId, peerId});
      res.status(204).end();
    }));
});
