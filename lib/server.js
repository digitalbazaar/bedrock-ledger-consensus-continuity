/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
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
const {callbackify, BedrockError} = bedrock.util;
const {config} = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const docs = require('bedrock-docs');

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

  // Get an event
  app.get(routes.events, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: callbackify(async (req) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const eventHash = req.query.id;

      const voter = await _voters.storage.get({voterId});
      const ledgerNode = await brLedgerNode.get(null, voter.ledgerNodeId);
      const {consensusMethod} = ledgerNode.consensus;
      if(consensusMethod !== 'Continuity2017') {
        throw new BedrockError(
          'Ledger node consensus method is not "Continuity2017".',
          'AbortError', {httpStatusCode: 400, public: true});
      }
      const {event} = await ledgerNode.events.get(eventHash);
      return event;
    })
  }));
  docs.annotate.get(routes.events, {
    description: 'Get information about a specific event.',
    securedBy: ['null'],
    responses: {
      200: {
        'application/ld+json': {
          example: 'examples/get.ledger.event.jsonld'
        }
      },
      404: 'Event was not found.'
    }
  });

  // Get events
  app.post(
    routes.eventsQuery, brRest.when.prefers.ld,
    asyncHandler(async (req, res) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      // FIXME: place limits on size of eventHash, this *could* be part of a
      // schema validator on this API
      const ledgerNodeId = await _voters.getLedgerNodeId(voterId);
      const {eventHash} = req.body;
      const events = await _events.getEvents(
        {eventHash, ledgerNodeId, stringify: true});
      for(const event of events) {
        res.write(`${event}\n`);
      }
      res.end();
    }));
  docs.annotate.get(routes.events, {
    description: 'Get information about a specific event.',
    securedBy: ['null'],
    responses: {
      200: {
        'application/ld+json': {
          example: 'examples/get.ledger.event.jsonld'
        }
      },
      404: 'Event was not found.'
    }
  });

  app.post(routes.gossip, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: callbackify(async (req) => {
      const creatorId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      // FIXME: create a validator for this
      const callerId = req.body.callerId;
      if(!callerId) {
        throw new BedrockError(
          '`callerId` is required.', 'DataError',
          {httpStatusCode: 400, public: true});
      }
      const creatorHeads = req.body.creatorHeads;
      const headsOnly = req.body.headsOnly;
      // FIXME: validate creator heads!
      if(!headsOnly && !creatorHeads) {
        throw new BedrockError(
          'One of `creatorHeads` or `headsOnly` is required.', 'DataError',
          {httpStatusCode: 400, public: true});
      }

      const ledgerNodeId = await _voters.getLedgerNodeId(creatorId);
      // clear the notifyFlag to indicate that a gossip session has occurred
      await _cache.gossip.notifyFlag({ledgerNodeId, remove: true});

      if(headsOnly) {
        return _gossip.getHeads({creatorId});
      }

      return _events.partitionHistory(
        {creatorHeads, creatorId, peerId: callerId});
    })
  }));

  // FIXME: add validation
  app.post(routes.notify, brRest.when.prefers.ld, async (req, res, next) => {
    const {callerId: senderId} = req.body;
    if(!senderId) {
      return next(new BedrockError(
        '`callerId` is required.', 'DataError',
        {httpStatusCode: 400, public: true}));
    }
    const receiverId = config.server.baseUri +
      '/consensus/continuity2017/voters/' + req.params.voterId;
    const ledgerNodeId = await _voters.getLedgerNodeId(receiverId);
    // ignore failure to cache notification, not a critical error
    _cache.gossip.addNotification({ledgerNodeId, senderId})
      .catch(() => {});
    res.status(204).end();
  });
});
