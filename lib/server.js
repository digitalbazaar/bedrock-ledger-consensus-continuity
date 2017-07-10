/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedgerAgent = require('bedrock-ledger-agent');
const brPassport = require('bedrock-passport');
const brRest = require('bedrock-rest');
const config = require('bedrock').config;
const brLedger = require('bedrock-ledger');
const database = require('bedrock-mongodb');
const cors = require('cors');
const docs = require('bedrock-docs');
const URL = require('url');

const ensureAuthenticated = brPassport.ensureAuthenticated;
const validate = require('bedrock-validation').validate;
const BedrockError = bedrock.util.BedrockError;

require('bedrock-express');
require('bedrock-permission');

require('./config');

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-express.configure.routes', app => {
  // TODO: use `config['ledger-continuity'].routes` and computed config
  // based on ledger-agent routes
  const agentRoute = config['ledger-agent'].routes.agents;
  // TODO: use `:voterId` in URL
  const root = agentRoute + '/continuity2017'; // + '/:voterId'
  const routes = {
    root,
    status: root + '/status'
  };

  // TODO: instead of `POST /status` maybe do a `GET /blocks/id/status` ?
  // TODO: `/status` should check for any vote at all for the `voterId`
  //   for the block to determine whether its phase is `gossip` or `election`

  // POST /status
  // Query the status of a block
  app.post(
    routes.status, ensureAuthenticated, brRest.when.prefers.ld,
    (req, res, next) => {
      const ledgerNodeId = req.query.ledgerNodeId || null;
      const options = {
        owner: req.query.owner || req.user.identity.id
      };

      // set config block if ledgerNodeId not given
      if(!ledgerNodeId) {
        options.configEvent = req.body;
      }

      brLedgerAgent.add(
        req.user.identity, ledgerNodeId, options, (err, ledgerAgent) => {
        if(err) {
          return next(err);
        }
        // return the saved config
        res.location(ledgerAgent.service.ledgerAgentStatusService);
        res.status(201).end();
      });
  });
  docs.annotate.post(routes.agents, {
    description: 'Create a new ledger agent',
    schema: 'services.ledger.postConfig',
    securedBy: ['cookie', 'hs1'],
    responses: {
      201: 'Ledger agent creation was successful. HTTP Location header ' +
        'contains URL of newly created ledger agent.',
      400: 'Ledger agent creation failed due to malformed request.',
      403: 'Ledger agent creation failed due to invalid digital signature.',
      409: 'Ledger agent creation failed due to duplicate information.'
    }
  });

  // GET /ledger-agent/{AGENT_ID}/events?id=EVENT_ID
  // Get an existing event
  app.get(routes.events, ensureAuthenticated,
    brRest.when.prefers.ld,
    brRest.linkedDataHandler({
      get: function(req, res, callback) {
        const actor = req.user ? req.user.identity : undefined;
        const agentId = 'urn:uuid:' + req.params.agentId;
        const eventHash = req.query.id;
        const options = {
          owner: req.query.owner || req.user.identity.id
        };

        async.auto({
          getLedgerAgent: callback => brLedgerAgent.get(
            actor, agentId, options, callback),
          getEvent: ['getLedgerAgent', (results, callback) => {
            const ledgerAgent = results.getLedgerAgent;
            ledgerAgent.node.events.get(eventHash, {}, callback);
          }]
        }, (err, results) => {
          if(err) {
            return callback(err);
          }
          const event = results.getEvent;
          callback(null, event);
        });
      }
    })
  );
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
});
