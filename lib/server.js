/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
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
// const stream = require('stream');
// const JSONStream = require('JSONStream');
// const validate = require('bedrock-validation').validate;

// const cacheConfig = config['ledger-consensus-continuity'].gossip.cache;

require('bedrock-express');
require('bedrock-permission');

require('./config');

const storage = require('./storage');

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-express.configure.bodyParser', app => {
  app.use(bodyParser.json({limit: '1mb', type: ['json', '+json']}));
});

bedrock.events.on('bedrock-express.configure.routes', app => {
  const routes = config['ledger-consensus-continuity'].routes;

  // TODO: remove this endpoint once public keys are simplified
  // get voter information, including public key
  app.get(routes.root, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: callbackify(async (req, res) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const voter = await storage.voters.get({voterId, publicKey: true});
      return {
        // bedrock-ledger-continuity
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        id: voter.id,
        type: ['Identity', 'Continuity2017Peer'],
        publicKey: {
          id: voter.publicKey.id,
          type: 'Ed25519VerificationKey2018',
          owner: voter.id,
          publicKeyBase58: voter.publicKey.publicKeyBase58
        }
      };
    })
  }));
  docs.annotate.get(routes.root, {
    description: 'Get information about a specific voter.',
    securedBy: ['null'],
    responses: {
      200: {
        'application/ld+json': {
          example: 'examples/get.ledger.voter.jsonld'
        }
      },
      404: 'Voter was not found.'
    }
  });

  // Get an event
  app.get(routes.events, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: callbackify(async (req, res) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const eventHash = req.query.id;

      const voter = await storage.voters.get({voterId});
      //const ledgerNodeId = await _voters.getLedgerNodeId(voterId);
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
    routes.eventsQuery, brRest.when.prefers.ld, async (req, res, next) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      // FIXME: place limits on size of eventHash
      let ledgerNodeId;
      let ledgerNode;
      const eventHashes = req.body.eventHash;
      try {
        ledgerNodeId = await _voters.getLedgerNodeId(voterId);
        ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
      } catch(e) {
        logger.debug('Error while getting events (gossip).', {error: e});
        return next(e);
      }

      ledgerNode.storage.events.getMany({eventHashes})
        // false indicates not to wrap records in an array
        // .stream().pipe(JSONStream.stringify(false))
        .on('data', data => {
          // TODO: filter out unused info
          if(data.event.type !== 'WebLedgerOperationEvent') {
            delete data.event.operation;
          }
          res.write(JSON.stringify(data));
        })
        .on('error', err => {
          next(err);
        })
        .once('end', () => {
          res.end();
        });
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
    get: callbackify(async (req, res) => {
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
      if(headsOnly) {
        return _gossip.getHeads({creatorId});
      }

      return _events.partitionHistory(
        {creatorHeads, creatorId, peerId: callerId});
    })
  }));

  // TODO: add validator
  app.post(routes.notify, brRest.when.prefers.ld, (req, res, next) => {
    const creatorId = config.server.baseUri +
      '/consensus/continuity2017/voters/' + req.params.voterId;
    const callerId = req.body.callerId;
    if(!callerId) {
      return next(new BedrockError(
        '`callerId` is required.', 'DataError',
        {httpStatusCode: 400, public: true}));
    }
    const notificationKey = _cacheKey.gossipNotification(creatorId);
    // a set is used here because it only allows unique values
    cache.client.sadd(notificationKey, callerId, () => {
      res.status(204).end();
    });
  });
});
