/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brRest = require('bedrock-rest');
const config = require('bedrock').config;
const brLedger = require('bedrock-ledger');
const cors = require('cors');
const docs = require('bedrock-docs');
const url = require('url');

const BedrockError = bedrock.util.BedrockError;

require('bedrock-express');
require('bedrock-permission');

require('./config');

const storage = require('./storage');

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-express.configure.routes', app => {
  const root = '/consensus/continuity2017/voters/:voterId';
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
  //   get manifests
  //   send events
  //   get events

  // get block status
  app.get(
    root + '/blocks/:blockHeight/status', brRest.when.prefers.ld,
    (req, res, next) => {
      let blockHeight;
      try {
        blockHeight = parseInt(req.params.blockHeight);
      } catch(err) {
        return next(new BedrockError(
          'Block height must be an integer.',
          // FIXME: something else
          'NotFound', {
            httpStatusCode: 400,
            public: true,
            blockHeight: req.params.blockHeight
          }, err));
      }
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      async.auto({
        getVoter: callback => storage.voters.get({voterId}, callback),
        getLedgerNode: ['getVoter', (results, callback) => {
          brLedger.get(null, results.getVoter.ledgerNodeId, callback);
        }],
        getBlockStatus: ['getLedgerNode', (results, callback) => {
          const voter = results.getVoter;
          const ledgerNode = results.getLedgerNode;
          _createBlockStatus(blockHeight, ledgerNode, voter, callback);
        }]
      }, (err, results) => {
        if(err) {
          return next(err);
        }
        res.json(results.getBlockStatus);
      });
    });

  // get manifest
  app.get(
    root + '/manifests/:manifestHash', brRest.when.prefers.ld,
    (req, res, next) => {
      storage.manifests.get(req.params.manifestHash, (err, result) => {
        // TODO: implement
        console.log('EEEEE', err, result);
      });
    });

  // Get an event
  app.get(root + '/events', brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: function(req, res, callback) {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const eventHash = req.query.id;

      async.auto({
        getVoter: callback => storage.voters.get({voterId}, callback),
        getLedgerNode: ['getVoter', (results, callback) =>
          brLedger.get(null, results.getVoter.ledgerNodeId, callback)],
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
  docs.annotate.get(root + '/events', {
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
  app.post(root + '/events', brRest.when.prefers.ld, (req, res, next) => {
    const voterId = config.server.baseUri +
      '/consensus/continuity2017/voters/' + req.params.voterId;
    async.auto({
      getVoter: callback => storage.voters.get({voterId}, callback),
      getLedgerNode: ['getVoter', (results, callback) =>
        brLedger.get(null, results.getVoter.ledgerNodeId, callback)],
      addEvent: ['getLedgerNode', (results, callback) =>
        results.getLedgerNode.events.add(req.body, callback)]
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

    // const agentId = 'urn:uuid:' + req.params.agentId;
    // const options = {
    //   owner: req.query.owner || req.user.identity.id
    // };

    // async.auto({
    //   getLedgerAgent: callback => brLedgerAgent.get(
    //     req.user.identity, agentId, options, callback),
    //   addEvent: ['getLedgerAgent', (results, callback) => {
    //     const ledgerAgent = results.getLedgerAgent;
    //
    //     ledgerAgent.node.events.add(req.body, {}, callback);
    //   }]
    // }, (err, results) => {
    //   if(err) {
    //     return next(err);
    //   }
    //   const event = results.addEvent;
    //
    //   const eventUrl = url.format({
    //     protocol: 'https',
    //     host: config.server.host,
    //     pathname: url.parse(req.url).pathname,
    //     query: {
    //       id: event.meta.eventHash
    //     }
    //   });
    //
    //   res.location(eventUrl);
    //   res.status(201).end();
    // });
  });
  docs.annotate.post(root + '/events', {
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

function _createBlockStatus(blockHeight, ledgerNode, voter, callback) {
  const status = {
    // TODO: add `@context`?
    blockHeight: blockHeight,
    phase: 'gossip'
  };

  async.auto({
    blockId: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) =>
        callback(null, result.event.input[0].ledger + '/blocks/' + blockHeight)
      ),
    // see if block has achieved consensus
    block: ['blockId', (results, callback) =>
      ledgerNode.blocks.get(results.blockId, (err, block) => {
        if(err && err.name === 'NotFound') {
          // block has not achieved consensus yet
          return callback();
        }
        callback(err, block);
      })],
    // get current votes for the block
    votes: ['block', (results, callback) => {
      if(results.block) {
        const block = results.block;
        if(!block.consensusProof ||
          block.consensusProof.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'No Continuity2017 status for block found.',
            'NotFound', {blockId: results.blockId}));
        }
        status.phase = 'consensus';
        return callback(null, {
          events: block.consensusProof.electionResults,
          rollCall: []
        });
      }
      // obtain current votes from database
      async.auto({
        eventVotes: callback => storage.votes.get(
          ledgerNode.id, blockHeight, 'Events', callback),
        rollCallVotes: callback => storage.votes.get(
          ledgerNode.id, blockHeight, 'RollCall', callback)
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, {
          events: results.eventVotes,
          rollCall: results.rollCallVotes
        });
      });
    }],
    // get event gossip for the current block if this voter hasn't voted yet
    gossip: ['votes', (results, callback) => {
      if(_hasVoted(voter, results.votes.events) ||
        _hasVoted(voter, results.votes.rollCall)) {
        // TODO: if the voter isn't an elector, we could also set the status
        // here to 'decideEvents' to allow for propagation of votes
        // via all peers on the network

        // voter has voted, only return votes, not gossip
        if(status.phase === 'gossip') {
          if(results.votes.rollCall.length === 0) {
            status.phase = 'decideEvents';
          } else {
            status.phase = 'decideRollCall';
          }
        }
        return callback();
      }
      // get event hashes
      ledgerNode.storage.events.getHashes({
        consensus: false,
        sort: 1
      }, callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    if(status.phase === 'gossip') {
      status.gossip = results.gossip;
    } else {
      status.election = [{
        topic: 'Events',
        electionResults: results.votes.events
      }, {
        topic: 'RollCall',
        electionResults: results.votes.rollCall
      }];
    }
    callback(null, status);
  });
}

function _hasVoted(voter, votes) {
  return votes.some(v => v.voter === voter.id);
}
