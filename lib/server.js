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
const brLedger = require('bedrock-ledger-node');
// const cors = require('cors');
const docs = require('bedrock-docs');
const election = require('./election');
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
          // bedrock-leger-continuity
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

  // get block status
  app.get(routes.blockStatus, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: (req, res, callback) => {
      let blockHeight;
      try {
        blockHeight = parseInt(req.params.blockHeight);
      } catch(err) {
        return callback(new BedrockError(
          'Block height must be an integer.',
          'TypeError', {
            httpStatusCode: 400,
            public: true,
            blockHeight: req.params.blockHeight
          }, err));
      }
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      async.auto({
        getVoter: callback => storage.voters.get({voterId}, callback),
        ledgerNode: ['getVoter', (results, callback) => {
          brLedger.get(null, results.getVoter.ledgerNodeId, callback);
        }],
        latestBlock: ['ledgerNode', (results, callback) =>
          results.ledgerNode.storage.blocks.getLatestSummary((err, result) => {
            if(err) {
              return callback(err);
            }
            // only provide status for block height that is 1 greater than
            // latest block which is the block currently in gossip status
            const maxBlockHeight = result.eventBlock.block.blockHeight + 1;
            if(blockHeight > maxBlockHeight) {
              return callback(new BedrockError(
                'Block status is not available for the specified block ' +
                'height.',
                'NotFoundError', {
                  httpStatusCode: 404,
                  public: true,
                  blockHeight,
                  maxBlockHeight
                }, err));
            }
            callback();
          })],
        blockStatus: ['latestBlock', (results, callback) =>
          _createBlockStatus(
            blockHeight, results.ledgerNode, results.getVoter, callback)]
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, results.blockStatus);
      });
    }
  }));
  docs.annotate.get(routes.blockStatus, {
    description: 'Get information about a specific block height.',
    securedBy: ['null'],
    responses: {
      200: {
        'application/ld+json': {
          example: 'examples/get.ledger.blockstatus.jsonld'
        }
      },
      404: 'Block status was not found.'
    }
  });

  // get manifest
  app.get(routes.manifests, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: (req, res, callback) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const manifestHash = req.query.id;
      async.auto({
        getVoter: callback => storage.voters.get({voterId}, callback),
        getManifest: ['getVoter', (results, callback) =>
          storage.manifests.get(
            results.getVoter.ledgerNodeId, manifestHash, callback)]
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, results.getManifest);
      });
    }
  }));
  docs.annotate.get(routes.manifests, {
    description: 'Get information about a specific manifest.',
    securedBy: ['null'],
    responses: {
      200: {
        'application/ld+json': {
          example: 'examples/get.ledger.manifest.jsonld'
        }
      },
      404: 'Manifest was not found.'
    }
  });

  // get vote
  app.get(routes.votes, brRest.when.prefers.ld, brRest.linkedDataHandler({
    get: (req, res, callback) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const voteHash = req.query.id;
      async.auto({
        voter: callback => storage.voters.get({voterId}, callback),
        getVote: ['voter', (results, callback) =>
          storage.votes.getByHash(
            results.voter.ledgerNodeId, voteHash, callback)]
      }, (err, results) => err ? callback(err) :
        callback(null, results.getVote.vote));
    }
  }));
  docs.annotate.get(routes.votes, {
    description: 'Get information about a specific vote.',
    securedBy: ['null'],
    responses: {
      200: {
        'application/ld+json': {
          example: 'examples/get.ledger.vote.jsonld'
        }
      },
      404: 'Vote was not found.'
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
          brLedger.get(null, results.voter.ledgerNodeId, callback)],
        exists: ['ledgerNode', (results, callback) =>
          results.ledgerNode.storage.events.exists(eventHash, (err, result) => {
            if(err) {
              return callback(err);
            }
            if(result) {
              return callback(new BedrockError(
                'Duplicate event.',
                'DuplicateError', {eventHash, httpStatus: 409, public: true}));
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
});

function _createBlockStatus(blockHeight, ledgerNode, voter, callback) {
  const status = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    ledger: ledgerNode.ledger,
    blockHeight: blockHeight,
    consensusPhase: 'gossip'
  };

  async.auto({
    ledgerId: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) =>
        callback(err, result.event.ledgerConfiguration.ledger)),
    // see if block has achieved consensus
    block: ['ledgerId', (results, callback) => {
      const blockId = results.ledgerId + '/blocks/' + blockHeight;
      ledgerNode.blocks.get(blockId, {expandEvents: false}, (err, record) => {
        if(err && err.name === 'NotFoundError') {
          // block has not achieved consensus yet
          return callback();
        }
        const block = record.block;
        if(block.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'No Continuity2017 status for block found.',
            'NotFoundError', {block, httpStatus: 404, public: true}));
        }
        callback(null, record);
      });
    }],
    // add previous block hash to block status
    previousBlockHash: ['block', (results, callback) => {
      if(blockHeight === 0) {
        // no `previousBlockHash` for first block
        return callback();
      }
      if(results.block) {
        status.previousBlockHash = results.block.block.previousBlockHash;
        return callback();
      }
      // requested block has no consensus, must retrieve previous one to get
      // its hash
      const blockId = results.ledgerId + '/blocks/' + (blockHeight - 1);
      ledgerNode.blocks.get(blockId, {expandEvents: false}, (err, record) => {
        if(err) {
          if(err.name === 'NotFoundError') {
            // previous block not found
            return callback(new BedrockError(
              'Previous block not found.', 'InvalidStateError',
              {blockHeight}));
          }
          return callback(err);
        }
        status.previousBlockHash = record.meta.blockHash;
        callback();
      });
    }],
    // get current votes for the block
    votes: ['previousBlockHash', (results, callback) => {
      if(results.block) {
        status.consensusPhase = 'consensus';
      }
      // obtain current votes from database
      async.auto({
        eventVotes: callback => {
          if(results.block) {
            return callback(null, results.block.block.electionResult);
          }
          storage.votes.get(ledgerNode.id, blockHeight, 'Events', callback);
        },
        rollCallVotes: callback => storage.votes.get(
          ledgerNode.id, blockHeight, 'RollCall', callback)
      }, (err, results) => {
        if(err) {
          return callback(err);
        }
        callback(null, {
          events: results.eventVotes.map(r => r.vote || r),
          rollCall: results.rollCallVotes.map(r => r.vote)
        });
      });
    }],
    hasVoted: ['votes', (results, callback) => {
      callback(null, _hasVoted(voter, results.votes.events) ||
        _hasVoted(voter, results.votes.rollCall));
    }],
    doneGossiping: ['hasVoted', (results, callback) => {
      if(results.hasVoted) {
        // already voted, so not gossiping for this block anymore
        return callback(null, true);
      }
      if(results.votes.events.length === 0) {
        // no votes in yet, not done gossiping
        return callback(null, false);
      }
      // votes are in, so done gossiping if `voter` will never vote
      // because they aren't an elector -- now it's time to mirror votes
      election.getBlockElectors(ledgerNode, blockHeight, (err, result) => {
        if(err) {
          return callback(err);
        }
        callback(null, result.map(v => v.id).includes(voter.id));
      });
    }],
    // get event gossip for the current block if this voter hasn't voted yet
    gossip: ['doneGossiping', (results, callback) => {
      if(results.doneGossiping) {
        // voter has voted or never will, only return votes, not gossip
        if(status.consensusPhase === 'gossip') {
          if(results.votes.rollCall.length === 0) {
            status.consensusPhase = 'decideEvents';
          } else {
            status.consensusPhase = 'decideRollCall';
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
    if(status.consensusPhase === 'gossip') {
      status.eventHash = results.gossip;
    } else {
      status.election = [{
        topic: 'Events',
        electionResult: results.votes.events
      }, {
        topic: 'RollCall',
        electionResult: results.votes.rollCall
      }];
    }
    callback(null, status);
  });
}

function _hasVoted(voter, votes) {
  return votes.some(v => v.voter === voter.id);
}
