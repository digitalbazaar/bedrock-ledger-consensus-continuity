/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
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

const storage = require('./storage');

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
  // TODO: get ledgerNode API via voter.id

  // POST /status
  // Query the status of a block

  // TODO: need endpoints for clients to:
  //   get block status
  //   get manifests
  //   send events
  //   get events

/* TODO: REMOVE; copied from ledger agent API as boilerplate
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
  */
});

function _createBlockStatus(blockId, ledgerNode, voter, callback) {
  const status = {
    // TODO: add `@context`?
    block: blockId,
    phase: 'gossip'
  };

  async.auto({
    // see if block has achieved consensus
    block: callback => ledgerNode.blocks.get(blockId, (err, block) => {
      if(err && err.name === 'NotFound') {
        // block has not achieved consensus yet
        return callback();
      }
      callback(err, block);
    }),
    // get current votes for the block
    votes: ['block', (results, callback) => {
      if(results.block) {
        const block = results.block;
        if(!block.consensusProof ||
          block.consensusProof.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'No Continuity2017 status for block found.',
            'NotFound', {blockId: blockId}));
        }
        status.phase === 'consensus';
        return callback(null, block.consensusProof.electionResults);
      }
      // obtain current votes from database
      storage.votes.get(ledgerNode.id, blockId, callback);
    }],
    // get event gossip for the current block if this voter hasn't voted yet
    gossip: ['votes', (results, callback) => {
      if(_hasVoted(voter, results.votes)) {
        // TODO: if the voter isn't in the voting population, we could also
        // set the status here to 'election' to allow for propagation of votes
        // via all peers on the network

        // voter has voted, only return votes, not gossip
        if(status.phase === 'gossip') {
          status.phase = 'election';
        }
        return callback();
      }

      // TODO: get all non-deleted event hashes that do not have consensus
      // in an ordered (lexicographically least hash first) array
      //ledgerNode.events.getHashes(..., callback)
      callback(new Error('Not implemeneted'));
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    if(status.phase === 'gossip') {
      status.gossip = results.gossip;
    } else {
      status.electionResults = results.votes;
    }
    callback(null, status);
  });
}

function _hasVoted(voter, votes) {
  return votes.some(v => v.voter === voter.id);
}
