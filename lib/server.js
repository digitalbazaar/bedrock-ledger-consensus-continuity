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
const database = require('bedrock-mongodb');
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
          results.ledgerNode.storage.blocks.getLatest((err, result) => {
            if(err) {
              return callback(err);
            }
            // only provide status for block height that is 1 greater than
            // latest block which is the block currently in gossip status
            const maxBlockHeight = result.eventBlock.block.blockHeight + 1;
            if(blockHeight > maxBlockHeight) {
              return callback(new BedrockError(
                'Block status is not available for the specified block height.',
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
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      async.auto({
        getVoter: callback => storage.voters.get({voterId}, callback),
        getLedgerNode: ['getVoter', (results, callback) =>
          brLedger.get(null, results.getVoter.ledgerNodeId, callback)],
        addEvent: ['getLedgerNode', (results, callback) =>
          results.getLedgerNode.events.add(req.body, {
            continuity2017: {peer: true}
          }, callback)]
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

  // Add a new vote
  app.post(
    routes.votes, brRest.when.prefers.ld, validate('continuity.vote'),
    (req, res, next) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const supportedVoteTopics = ['Events', 'RollCall'];
      const voteTopic = req.params.voteTopic;
      if(!supportedVoteTopics.includes(voteTopic)) {
        return next(new BedrockError(
          'Unsupported vote topic.', 'NotSupportedError', {
            httpStatusCode: 400,
            public: true,
            voteTopic,
            supportedVoteTopics,
          }));
      }
      async.auto({
        voteHash: callback =>
          brLedger.consensus._hasher(req.body, callback),
        voter: callback => storage.voters.get({voterId}, callback),
        ledgerNode: ['voter', (results, callback) =>
          brLedger.get(null, results.voter.ledgerNodeId, callback)],
        addVote: ['voteHash', 'ledgerNode', (results, callback) =>
          storage.votes.add(results.ledgerNode.id, voteTopic, req.body, {
            meta: {voteHash: results.voteHash}
          }, callback)]
      }, err => {
        if(err && !(database.isDuplicateError(err) ||
          err.name === 'DuplicateError')) {
          return next(err);
        }
        res.status(200).end();
      });
    });
  docs.annotate.post(routes.votes, {
    description: 'Add a new vote',
    schema: 'services.ledger.postLedgerVote',
    securedBy: ['null'],
    responses: {
      200: 'Vote was accepted for writing. HTTP Location header ' +
        'contains URL of accepted event.',
      400: 'Request failed due to malformed request.',
    }
  });

  // Add a new manifest
  let success = 0;
  let duplicate = 0;
  app.post(
    routes.manifests, brRest.when.prefers.ld, validate('continuity.manifest'),
    (req, res, next) => {
      const voterId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const manifest = req.body;
      // it is safe to assume we have all the events, just store manifest
      // TODO: do a quick existence check on manifest?
      const voterSlug = req.params.voterId.substring(24);
      console.log(`${voterSlug} receiving posted manifest: ${manifest.id}`);
      return async.auto({
        validateHash: callback => _validateManifestHash(manifest, callback),
        voter: ['validateHash', (results, callback) =>
          storage.voters.get({voterId}, callback)],
        ledgerNode: ['voter', (results, callback) =>
          brLedger.get(null, results.voter.ledgerNodeId, callback)],
        checkExists: ['ledgerNode', (results, callback) =>
          storage.manifests.exists(
            results.ledgerNode.id, manifest.id, (err, result) => {
              if(err) {
                return callback(err);
              }
              if(result) {
                return callback(new BedrockError(
                  'The manifest already exists.', 'DuplicateError', {
                    manifest: manifest.id
                  }, err));
              }
              callback();
            })],
        checkMajority: ['checkExists', (results, callback) =>
          election.checkMajority(manifest, results.ledgerNode, callback)],
        checkItems: ['checkMajority', (results, callback) => {
          // FIXME: is it necessary to check items if the type is `Events`?
          if(manifest.type === 'RollCall') {
            return storage.votes.exists(
              results.ledgerNode.id, manifest.item, (err, result) => {
                if(err) {
                  return callback(err);
                }
                if(!result) {
                  return callback(new BedrockError(
                    'Unknown votes enumerated in the manifest.',
                    'DataError', {manifest, httpStatus: 404, public: true}));
                }
                return callback();
              });
          }
          callback();
        }],
        store: ['checkItems', (results, callback) =>
          storage.manifests.add(results.ledgerNode.id, manifest, callback)]
      }, err => {
        if(!err) {
          console.log(`${voterSlug} Manifest store no error: ${success++}`);
        }
        if(err && err.name === 'DuplicateError') {
          console.log(`${voterSlug} Duplicate Manifest ${duplicate++}`);
          // ignore duplicate manifest entries; they only mean that
          // another process has already added the same manifest
          err = null;
        }
        if(err) {
          console.log('ERROR IN POST', err);
          return next(err);
        }
        res.status(200).end();
      });
    });
  docs.annotate.post(routes.votes, {
    description: 'Add a new vote',
    schema: 'services.ledger.postLedgerVote',
    securedBy: ['null'],
    responses: {
      200: 'Vote was accepted for writing. HTTP Location header ' +
        'contains URL of accepted event.',
      400: 'Request failed due to malformed request.',
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
    blockId: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) =>
        callback(
          null, result.event.ledgerConfiguration.ledger +
            '/blocks/' + blockHeight)
      ),
    // see if block has achieved consensus
    block: ['blockId', (results, callback) =>
      ledgerNode.blocks.get(results.blockId, (err, block) => {
        if(err && err.name === 'NotFoundError') {
          // block has not achieved consensus yet
          return callback();
        }
        callback(err, block);
      })],
    // get current votes for the block
    votes: ['block', (results, callback) => {
      if(results.block) {
        const block = results.block.block;
        if(block.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'No Continuity2017 status for block found.',
            'NotFoundError', {block, httpStatus: 404, public: true}));
        }
        status.consensusPhase = 'consensus';
      }
      // obtain current votes from database
      async.auto({
        eventVotes: callback => {
          if(results.block) {
            return callback(null, results.block.block.electionResults);
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

function _validateManifestHash(manifest, callback) {
  const expectedHash = election.createManifestHash(manifest.item);
  if(manifest.id !== expectedHash) {
    return callback(new BedrockError(
      'The hash contained in the manifest is invalid.',
      'ValidationError', {
        httpStatusCode: 400,
        public: true,
        expectedHash,
        manifest,
      }));
  }
  callback();
}
