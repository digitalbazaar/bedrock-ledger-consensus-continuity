/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _gossip = require('./gossip');
const async = require('async');
const bedrock = require('bedrock');
const brRest = require('bedrock-rest');
const cache = require('bedrock-redis');
const config = require('bedrock').config;
const brLedgerNode = require('bedrock-ledger-node');
// const cors = require('cors');
const docs = require('bedrock-docs');
const logger = require('./logger');
const url = require('url');
const validate = require('bedrock-validation').validate;

const BedrockError = bedrock.util.BedrockError;

const cacheConfig = config['ledger-consensus-continuity'].gossip.cache;

require('bedrock-express');
require('bedrock-permission');

require('./config');

const storage = require('./storage');

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-express.configure.routes', app => {
  const routes = config['ledger-consensus-continuity'].routes;

  // get voter information, including public key
  app.get(routes.root, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: function(req, res, callback) {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      storage.voters.get({voterId}, (err, voter) => {
        if(err) {
          return callback(err);
        }
        callback(null, {
          // bedrock-ledger-continuity
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          id: voter.id,
          type: ['Identity', 'Continuity2017Peer'],
          publicKey: {
            id: voter.publicKey.id,
            type: ['CryptographicKey'],
            owner: voter.id,
            publicKeyPem: voter.publicKey.publicKeyPem
          }
        });
      });
    }
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
    get: (req, res, callback) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const eventHash = req.query.id;

      async.auto({
        getVoter: callback => storage.voters.get({voterId}, callback),
        getLedgerNode: ['getVoter', (results, callback) =>
          brLedgerNode.get(null, results.getVoter.ledgerNodeId, callback)],
        getEvent: ['getLedgerNode', (results, callback) =>
          results.getLedgerNode.events.get(eventHash, {}, callback)]
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, results.getEvent.event);
      });
    }
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

  // Add a new event
  app.post(
    routes.events, brRest.when.prefers.ld, validate('continuity.event'),
    (req, res, next) => {
      const {callerId, event, eventHash, mergeHash} = req.body;
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      async.auto({
        cache: callback => {
          if(!cacheConfig.enabled) {
            return callback();
          }
          // callerId hash component of the peerId (43 chars)
          // which is also the hash of the public key
          const cacheKey = `r-${req.params.voterId}-${callerId}`;
          cache.client.set(cacheKey, '', 'EX', cacheConfig.ttl, callback);
        },
        voter: callback => storage.voters.get({voterId}, callback),
        ledgerNode: ['voter', (results, callback) =>
          brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
        exists: ['ledgerNode', (results, callback) => {
          results.ledgerNode.storage.events.exists(
            eventHash, (err, result) => {
              if(err) {
                return callback(err);
              }

              // event does not exist, proceed
              if(!result) {
                return callback();
              }

              // track number of dups per minute
              cache.client.incr(`dup-${Math.round(Date.now() / 60000)}`);

              if(result && eventHash === mergeHash) {
                // FIXME: pick AbortError or DuplicateError for this
                return callback(new BedrockError(
                  'Duplicate event.', 'AbortError',
                  {eventHash, mergeHash, public: true}));
                // return callback(new BedrockError(
                //   'Duplicate event.', 'DuplicateError',
                //   {eventHash, mergeHash, httpStatusCode: 409, public: true}));
              }
              // check if mergeHash exists
              results.ledgerNode.storage.events.exists(
                mergeHash, (err, result) => {
                  if(err) {
                    return callback(err);
                  }
                  // mergeHash does not exist, return 204
                  // signals client to keep sending this bundle
                  if(!result) {
                    return callback(new BedrockError(
                      'Duplicate event.', 'AbortError',
                      {eventHash, mergeHash, public: true}));
                  }
                  // the merge hash already exists, signal client to move to
                  // next bundle
                  callback(new BedrockError(
                    'Duplicate event.', 'DuplicateError',
                    {eventHash, mergeHash, httpStatusCode: 409, public: true}));
                });
            });
        }],
        addEvent: ['exists', (results, callback) => {
          results.ledgerNode.events.add(event, {
            continuity2017: {peer: true}
          }, callback);
        }]
      }, (err, results) => {
        if(err && err.name === 'AbortError') {
          return res.status(204).end();
        }
        if(err) {
          return next(err);
        }
        const eventUrl = url.format({
          protocol: 'https',
          host: config.server.host,
          pathname: url.parse(req.url).pathname,
          query: {
            id: results.addEvent.meta.eventHash
          }
        });
        res.location(eventUrl);
        res.status(201).end();
      });
    });
  docs.annotate.post(routes.events, {
    description: 'Request that a Ledger Agent append a new event to a ledger',
    schema: 'services.ledger.postLedgerEvent',
    securedBy: ['null'],
    responses: {
      201: 'Event was accepted for writing. HTTP Location header ' +
        'contains URL of accepted event.',
      400: 'Request failed due to malformed request.',
      403: 'Request failed due to invalid digital signature',
      409: 'Request failed due to duplicate information.'
    }
  });

  app.post(routes.gossip, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: (req, res, callback) => {
      const creatorId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      // FIXME: create a validator for this
      const callerId = req.body.callerId;
      if(!callerId) {
        return callback(new BedrockError(
          '`callerId` is required.', 'DataError',
          {httpStatusCode: 400, public: true}));
      }
      const creatorHeads = req.body.creatorHeads;
      const headsOnly = req.body.headsOnly;
      if(!headsOnly && !creatorHeads) {
        return callback(new BedrockError(
          'One of `creatorHeads` or `headsOnly` is required.', 'DataError',
          {httpStatusCode: 400, public: true}));
      }
      const cacheKey = `ap-${creatorId}`;
      async.auto({
        cache: callback => cache.client.sismember(
          cacheKey, callerId, (err, result) => {
            if(err) {
              logger.debug('error making cache request', {error: err});
              return callback();
            }
            if(result) {
              // cache hit, return error
              return callback(new BedrockError(
                'A gossip session is in progress. Try again later.',
                'AbortError', {httpStatusCode: 503, public: true}));
            }
            callback();
          }),
        heads: ['cache', (results, callback) => {
          if(!headsOnly) {
            return callback();
          }
          return _gossip.getHeads({creatorId}, callback);
        }],
        history: ['cache', (results, callback) => {
          if(headsOnly) {
            return callback();
          }
          _gossip.partitionHistory({
            creatorHeads, creatorId, eventTypeFilter: 'ContinuityMergeEvent',
            peerId: callerId
          }, (err, result) => {
            if(err) {
              return callback(err);
            }
            callback(null, result);
          });
        }]
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, headsOnly ? results.heads : results.history);
      });
    }
  }));
});
