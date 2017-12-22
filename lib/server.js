/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const brRest = require('bedrock-rest');
const config = require('bedrock').config;
const brLedgerNode = require('bedrock-ledger-node');
// const cors = require('cors');
const database = require('bedrock-mongodb');
const docs = require('bedrock-docs');
const events = require('./events');
const url = require('url');
const validate = require('bedrock-validation').validate;

const BedrockError = bedrock.util.BedrockError;

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
      const {event, eventHash} = req.body;
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      async.auto({
        voter: callback => storage.voters.get({voterId}, callback),
        ledgerNode: ['voter', (results, callback) =>
          brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
        exists: ['ledgerNode', (results, callback) =>
          results.ledgerNode.storage.events.exists(eventHash, (err, result) => {
            if(err) {
              return callback(err);
            }
            if(result) {
              return callback(new BedrockError(
                'Duplicate event.', 'DuplicateError',
                {eventHash, httpStatusCode: 409, public: true}));
            }
            callback();
          })],
        addEvent: ['exists', (results, callback) => {
          results.ledgerNode.events.add(event, {
            continuity2017: {peer: true}
          }, callback);
        }]
      }, (err, results) => {
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
      // console.log('77777777777777777777', req.body);
      const creatorHeads = req.body.creatorHeads;
      if(!creatorHeads) {
        return callback(new BedrockError(
          '`creatorHeads` is required.', 'DataError',
          {httpStatusCode: 400, public: true}));
      }
      async.auto({
        voter: callback => storage.voters.get({voterId: creatorId}, callback),
        ledgerNode: ['voter', (results, callback) =>
          brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
        genesisMergeHash: ['ledgerNode', (results, callback) =>
          results.ledgerNode.consensus._worker._events.getGenesisMergeHash(
            {eventsCollection: results.ledgerNode.storage.events.collection},
            callback)],
        // FIXME: use an API
        allCreators: ['ledgerNode', (results, callback) =>
          database.collections.continuity2017_key.find(
            {ledgerNodeId: results.ledgerNode.id},
            {_id: 0, 'publicKey.owner': 1}
          ).toArray((err, result) => err ? callback(err) :
            callback(null, result.map(k => k.publicKey.owner)))],
        localHeads: ['allCreators', (results, callback) => {
          const localCreatorHeads = {};
          // if creator is not found locally, genesisMerge hash will be returned
          async.each(Object.keys(creatorHeads), (creatorId, callback) =>
          // async.each(results.allCreators, (creatorId, callback) =>
            events._getLocalBranchHead({
              eventsCollection: results.ledgerNode.storage.events.collection,
              creator: creatorId
            }, (err, result) => {
              if(err) {
                return callback(err);
              }
              localCreatorHeads[creatorId] = result;
              callback();
            }), err => err ? callback(err) : callback(null, localCreatorHeads));
        }],
        // callerHistory: ['ledgerNode', 'creatorHead', (results, callback) =>
        //   events.aggregateHistory({
        //     eventTypeFilter: 'ContinuityMergeEvent',
        //     ledgerNode: results.ledgerNode,
        //     // treeHash: results.callerHead,
        //     startHash: results.callerHead
        //   }, callback)],
        history: ['allCreators', 'genesisMergeHash', (results, callback) => {
          const treeHash = creatorHeads[creatorId];
          results.allCreators.forEach(c => {
            if(!creatorHeads[c]) {
              creatorHeads[c] = results.genesisMergeHash;
            }
          });
          // exclusion for the local node is handled via treeHash
          delete creatorHeads[creatorId];
          events.getHistory({
            creatorId,
            // eventHashFilter: _.values(results.localHeads),
            eventHashFilter: _.values(creatorHeads),
            // eventHashFilter: results.callerHistory,
            eventTypeFilter: 'ContinuityMergeEvent',
            ledgerNode: results.ledgerNode,
            treeHash
          }, callback);
        }],
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, {
          creatorHeads: results.localHeads,
          history: results.history
        });
      });
    }
  }));
});
