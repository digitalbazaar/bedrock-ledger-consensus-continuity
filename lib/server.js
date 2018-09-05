/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _events = require('./events');
const _gossip = require('./gossip');
const _voters = require('./voters');
const bedrock = require('bedrock');
const bodyParser = require('body-parser');
const brRest = require('bedrock-rest');
const cache = require('bedrock-redis');
const {callbackify, BedrockError} = bedrock.util;
const config = require('bedrock').config;
const brLedgerNode = require('bedrock-ledger-node');
const docs = require('bedrock-docs');
const logger = require('./logger');
const {promisify} = require('util');
// const stream = require('stream');
// const JSONStream = require('JSONStream');
// const validate = require('bedrock-validation').validate;

require('bedrock-express');
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

  const urnUuidReg = /([^\:]*)\:*$/;
  const allHyphenReg = /-/g;
  function _lni(ledgerNodeId) {
    // return the uuid portion with hypens removed
    return ledgerNodeId.match(urnUuidReg)[1].replace(allHyphenReg, '');
  }

  // Get events
  app.post(
    routes.eventsQuery, brRest.when.prefers.ld, async (req, res, next) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      // FIXME: place limits on size of eventHash
      let ledgerNodeId;
      // let ledgerNode;
      try {
        ledgerNodeId = await _voters.getLedgerNodeId(voterId);
        // ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
      } catch(e) {
        logger.debug('Error while getting events (gossip).', {error: e});
        return next(e);
      }

      const {eventHash} = req.body;
      const lni = _lni(ledgerNodeId);
      // looping this here so `lni` does not need to be recomputed
      const eventKeys = eventHash.map(eventHash => `eg|${lni}|${eventHash}`);

      const events = await cache.client.mget(eventKeys);

      // if all events were found in the cache, return them and end
      if(!events.includes(null)) {
        for(const event of events) {
          res.write(`${event}\n`);
        }
        res.end();
        return;
      }

      let ledgerNode;
      try {
        ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
      } catch(e) {
        logger.debug('Error while getting events (gossip).', {error: e});
        return next(e);
      }

      const hashMap = new Map();
      const needed = [];
      for(let i = 0; i < eventHash.length; ++i) {
        hashMap.set(eventHash[i], events[i]);
        if(events[i] === null) {
          needed.push(eventHash[i]);
        }
      }

      // TODO: update driver to get `promise` from `.forEach`
      let counter = 0;
      const cursor = ledgerNode.storage.events.getMany({eventHashes: needed});
      const fn = promisify(cursor.forEach.bind(cursor));
      // fill in the missing events in `hashMap`
      await fn(({event}) => {
        hashMap.set(needed[counter], JSON.stringify({event}));
        counter++;
      });

      hashMap.forEach(event => res.write(`${event}\n`));
      res.end();
    });
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
