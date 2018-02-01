/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const _events = require('./events');
const _gossip = require('./gossip');
const _voters = require('./voters');
const async = require('async');
const bedrock = require('bedrock');
const brRest = require('bedrock-rest');
const cache = require('bedrock-redis');
const config = require('bedrock').config;
const brLedgerNode = require('bedrock-ledger-node');
// const cors = require('cors');
const docs = require('bedrock-docs');
const logger = require('./logger');
const stream = require('stream');
// const upload = require('multer')();
// const url = require('url');
// const validate = require('bedrock-validation').validate;

const BedrockError = bedrock.util.BedrockError;

// const cacheConfig = config['ledger-consensus-continuity'].gossip.cache;

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
      storage.voters.get({voterId, publicKey: true}, (err, voter) => {
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
            type: 'Ed25519VerificationKey2018',
            owner: voter.id,
            publicKeyBase58: voter.publicKey.publicKeyBase58
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
        checkMethod: ['getLedgerNode', (results, callback) => {
          const {consensusMethod} = results.getLedgerNode.consensus;
          if(consensusMethod !== 'Continuity2017') {
            return callback(new BedrockError(
              'Ledger node consensus method is not "Continuity2017".',
              'AbortError', {httpStatusCode: 400, public: true}));
          }
          callback();
        }],
        getEvent: ['checkMethod', (results, callback) =>
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

  app.post(
    routes.eventsQuery, brRest.when.prefers.ld, brRest.linkedDataHandler({
      get: (req, res, callback) => {
        const voterId = config.server.baseUri +
          '/consensus/continuity2017/voters/' + req.params.voterId;
        // TODO: place limits on size of eventHash
        const eventHash = req.body.eventHash;
        async.auto({
          ledgerNodeId: callback => _voters.getLedgerNodeId(voterId, callback),
          ledgerNode: ['ledgerNodeId', (results, callback) =>
            brLedgerNode.get(null, results.ledgerNodeId, callback)],
          getEvent: ['ledgerNode', (results, callback) => {
            const collection = results.ledgerNode.storage.events.collection;
            collection.aggregate([
              {$match: {
                eventHash: {$in: eventHash},
                'meta.deleted': {$exists: false},
              }},
              {$addFields: {_order: {$indexOfArray: [eventHash, '$name']}}},
              {$sort: {'_order': 1}}
            ]).toArray(callback);
          }]
        }, (err, results) => {
          if(err) {
            return callback(err);
          }
          callback(null, results.getEvent);
        });
      }
    }));

  // app.get(routes.events, brRest.when.prefers.ld, brRest.linkedDataHandler({
  //   get: (req, res, callback) => {
  //     const voterId = config.server.baseUri +
  //       '/consensus/continuity2017/voters/' + req.params.voterId;
  //     const eventHash = req.query.id;
  //
  //     async.auto({
  //       getVoter: callback => storage.voters.get({voterId}, callback),
  //       getLedgerNode: ['getVoter', (results, callback) =>
  //         brLedgerNode.get(null, results.getVoter.ledgerNodeId, callback)],
  //       getEvent: ['getLedgerNode', (results, callback) =>
  //         results.getLedgerNode.events.get(eventHash, {}, callback)]
  //     }, (err, results) => {
  //       if(err) {
  //         return callback(err);
  //       }
  //       callback(null, results.getEvent.event);
  //     });
  //   }
  // }));
  // docs.annotate.get(routes.events, {
  //   description: 'Get information about a specific event.',
  //   securedBy: ['null'],
  //   responses: {
  //     200: {
  //       'application/ld+json': {
  //         example: 'examples/get.ledger.event.jsonld'
  //       }
  //     },
  //     404: 'Event was not found.'
  //   }
  // });

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
      async.auto({
        heads: callback => {
          if(!headsOnly) {
            return callback();
          }
          return _gossip.getHeads({creatorId}, callback);
        },
        history: callback => {
          if(headsOnly) {
            return callback();
          }
          _events.partitionHistory({
            creatorHeads, creatorId, eventTypeFilter: 'ContinuityMergeEvent',
            fullEvent: true, peerId: callerId
          }, (err, result) => {
            if(err) {
              return callback(err);
            }
            callback(null, result);
          });
        }
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, headsOnly ? results.heads : results.history);
      });
    }
  }));

  // compressed pull gossip
  app.post(routes.gossipCompressed, (req, res, next) => {
    const creatorId = config.server.baseUri +
      '/consensus/continuity2017/voters/' + req.params.voterId;
    // FIXME: create a validator for this
    const callerId = req.body.callerId;
    if(!callerId) {
      return next(new BedrockError(
        '`callerId` is required.', 'DataError',
        {httpStatusCode: 400, public: true}));
    }
    const creatorHeads = req.body.creatorHeads;
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
            return next(new BedrockError(
              'A gossip session is in progress. Try again later.',
              'AbortError', {httpStatusCode: 503, public: true}));
          }
          callback();
        }),
      history: ['cache', (results, callback) => _events.compressedHistory({
        creatorHeads, creatorId, peerId: callerId
      }, callback)],
    }, (err, results) => {
      if(err) {
        return next(err);
      }
      const BOUNDARY = '###c680ff7e49bb###';
      const readStream = new stream.PassThrough();
      readStream.end(results.history.file);
      res.writeHead(200, {
        'Content-Type':
          `multipart/x-mixed-replace; charset=UTF-8; boundary="${BOUNDARY}"`,
        Connection: 'keep-alive',
      });
      // FIXME: make this work in a standard way
      res.write(BOUNDARY);
      // res.write('Content-Type', 'application/json');
      res.write(JSON.stringify(results.history.creatorHeads));
      res.write(BOUNDARY);
      // res.write('Content-disposition', 'attachment; filename=events.gz');
      // res.write('Content-Type', 'application/gzip');
      readStream.pipe(res);
    });
  });

  // TODO: add validator
  app.post(routes.notify, brRest.when.prefers.ld, (req, res, next) => {
    const callerId = req.body.callerId;
    if(!callerId) {
      return next(new BedrockError(
        '`callerId` is required.', 'DataError',
        {httpStatusCode: 400, public: true}));
    }
    const cacheKey = _cacheKey.gossipNotification(req.params.voterId);
    cache.client.sadd(cacheKey, callerId, () => {
      res.status(204).end();
    });
  });
});
