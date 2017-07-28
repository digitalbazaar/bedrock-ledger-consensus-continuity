/*
 * Web Ledger Continuity2017 consensus worker.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brLedger = require('bedrock-ledger-node');
const logger = require('./logger');
const BedrockError = bedrock.util.BedrockError;

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._election = require('./election');
api._events = require('./events');
api._hasher = brLedger.consensus._hasher;
api._storage = require('./storage');
api._voters = require('./voters');
api._votes = require('./voteStorage');
// exposed for testing
api._gossipWith = _gossipWith;

api.scheduleWork = (session) => {
  // start a consensus session for ledgers
  const maxTime = bedrock.config['ledger-continuity'].worker.session.maxTime;
  session.start(maxTime, _sync, err => {
    if(err) {
      logger.error('Error starting consensus job.', err);
    }
  });
};

// Note: exposed for testing
api._run = (ledgerNode, callback) => {
  _sync({ledgerNode: ledgerNode, isExpired: () => false}, callback);
};

function _sync(session, callback) {
  logger.verbose('consensus job running');
  const ledgerNode = session.ledgerNode;

  async.auto({
    voter: callback => api._voters.get(ledgerNode.id, callback),
    blockHeight: callback => _getBlockHeight(ledgerNode, callback),
    getElectors: ['blockHeight', (results, callback) => {
      api._election.getBlockElectors(ledgerNode, results.blockHeight, callback);
    }],
    getPeers: ['getElectors', (results, callback) => {
      _getPeers(ledgerNode, results.getElectors, callback);
    }],
    election: ['voter', 'getElectors', (results, callback) => {
      _runElection(
        session, ledgerNode, results.voter, results.blockHeight,
        results.getElectors, results.getPeers, callback);
    }],
    writeBlock: ['election', (results, callback) => {
      if(!results.election.done) {
        return callback();
      }
      _writeBlock(
        ledgerNode, results.blockHeight,
        results.election.events, results.election.rollCall,
        callback);
    }]
  }, err => callback(err));
}

/**
 * Gets the latest consensus block and returns the new proposed block height
 * for the ledger (i.e. the current `blockHeight + 1`).
 *
 * @param ledgerNode the ledger node to get the latest block for.
 * @param callback(err, blockHeight) called once the operation completes.
 */
function _getBlockHeight(ledgerNode, callback) {
  // Note: This consensus method assumes that `blockHeight` will always exist
  // on the previous block because it cannot be used on a blockchain that
  // does not have that information. There has presently been no mechanism
  // devised for switching consensus methods between hashgraph-like blocks
  // and typical blockchains with block heights.
  ledgerNode.storage.blocks.getLatest((err, block) => {
    if(err) {
      return callback(err);
    }
    const last = _.get(block, 'eventBlock.block.blockHeight');
    if(last === undefined) {
      return callback(new BedrockError(
        'blockHeight is missing from latest block.', 'NotFound', {
          block
        }));
    }
    callback(null, last + 1);
  });
}

/**
 * Get all peers to gossip with. This population will be the electors plus
 * an additional peers associated with the ledger node.
 *
 * @param ledgerNode the ledger node.
 * @param electors the electors.
 * @param callback(err, peers) called once the operation completes.
 */
function _getPeers(ledgerNode, electors, callback) {
  // TODO: in parallel, contact ledgerNode.peerLedgerAgent (and potentially
  // a cache) to get their continuity voter IDs
  callback(null, electors);
}

/**
 * Runs an election to decide the events and votes to write into the next block.
 *
 * This function will determine both an event manifest and a roll call manifest.
 * An event manifest is an ordered list of event hashes and a roll call manifest
 * is an ordered list of elector hashes. It is possible for a `null` manifest to
 * be returned. This occurs when the work session has expired before the
 * election was over. Another work session will have to run the election later
 * to determine the winning manifest.
 *
 * Running the election involves:
 *
 * 1. Gossiping events with a voting population,
 * 2. Gathering their votes until a 2/3rds majority agrees upon the next
 *    event manifest,
 * 3. Agreeing on who voted to form the elector roll call.
 *
 * Once an event manifest has been elected, the corresponding events will need
 * to be run through any appropriate event validators to produce the final
 * consensus block, which is work that is not performed by this function.
 *
 * @param session the current work session.
 * @param ledgerNode the ledger node being worked on.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the block for the next manifest.
 * @param electors the voting population that will select the next manifest.
 * @param peers all peers to gossip with (all electors plus any additional
 *          peers associated with the ledger node).
 * @param callback(err, electionResult) called once the operation completes.
 */
function _runElection(
  session, ledgerNode, voter, blockHeight, electors, peers, callback) {
  // determine if the current ledger node is voting in this election
  const isElector = api._election.isBlockElector(voter, electors);

  // continue to contact the entire voting population in every round...
  // ...voting at the end of the round (if participating) and
  // ...until a winning manifest is chosen or the work session expires
  const electionResult = {
    events: null,
    rollCall: null,
    done: false
  };
  const condition = () => (electionResult.events && electionResult.rollCall) ||
    session.isExpired();
  const limit = bedrock.config['ledger-continuity'].gossip.concurrentPeers;
  async.until(condition, loop => {
    // 1. Contact all voting peers once (up to limit), at random, in parallel.
    const random = _.shuffle(electors);
    async.eachLimit(random, limit, (elector, nextElector) => {
      // no need to gossip if condition met
      if(condition()) {
        return nextElector();
      }

      async.auto({
        // 2. Gossip with a single elector.
        gossip: callback => _gossipWith(
          ledgerNode, voter, blockHeight, elector, err => {
            // ignore communication error from a single voter
            if(err) {
              logger.verbose('Non-critical error in _gossipWith.', err);
              return nextElector();
            }
            callback();
          }),
        // 3. Tally votes to check for consensus on the events.
        eventsVotes: ['gossip', (results, callback) =>
          api._election.tally(
            ledgerNode.id, blockHeight, electors, 'Events', callback)],
        // 4. Tally votes to check for consensus on roll call.
        rollCallVotes: ['gossip', (results, callback) =>
          api._election.tally(
            ledgerNode.id, blockHeight, electors, 'RollCall', callback)]
      }, (err, results) => {
        if(err) {
          return nextElector(err);
        }
        if(results.eventsVotes) {
          electionResult.events = results.eventsVotes;
        }
        if(results.rollCallVotes) {
          electionResult.rollCall = results.rollCallVotes;
          electionResult.done = true;
        }
        nextElector();
      });
    }, err => {
      // TODO: should wait some period of time before communicating again
      // with the electors to allow gossip/votes to settle?

      // nothing to vote on if error or if not an elector
      if(err || !isElector) {
        return loop(err);
      }

      // if there is no events manifest yet, vote for one
      if(!electionResult.events) {
        // 5. All other electors have been contacted and no events manifest
        //   has been elected, so vote for one.
        return api._election.voteForEvents(
          ledgerNode, voter, blockHeight, electors, loop);
      }

      // if there is no roll call manifest yet, vote for one
      if(!electionResult.rollCall) {
        // TODO: it's possible that the roll-call votes were discarded, so
        // we also need to be keeping track, on our own, separately, whether
        // or not we've received twoThirdsMajority of block status's that
        // are set to `consensus` with the same block hash (or same set of
        // events -- these must be equivalent), if so, we have consensus and
        // don't need the roll call votes ... we can generate the roll-call
        // manifest using the event votes we've collected where
        // `status.consensusPhase === 'consensus'`.

        // 6. No agreed upon roll call selected yet for the events manifest
        //   vote; vote for one.
        return api._election.voteForRollCall(
          ledgerNode, voter, blockHeight, electors, loop);
      }

      loop();
    });
  }, err => err ? callback(err) : callback(err, electionResult));
}

/**
 * Causes the given ledger node, identified by the given `voter` information,
 * to gossip with a `peer` about the next block identified by `blockHeight`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the next block (the one to gossip about).
 * @param peer the peer to gossip with.
 * @param callback(err) called once the operation completes.
 */
function _gossipWith(ledgerNode, voter, blockHeight, peer, callback) {
  if(voter.id === peer.id) {
    // no need to gossip with self
    return callback();
  }
  // Get block status from peer.
  api._client.getBlockStatus(blockHeight, peer.id, (err, status) => {
    if(err) {
      return callback(err);
    }

    if(status.ledger !== ledgerNode.ledger) {
      return callback(new BedrockError(
        'Web Ledger mismatch.', 'InvalidStateError', {
          localLedger: ledgerNode.ledger,
          remoteLedger: status.ledger
        }));
    }

    if(status.consensusPhase === 'gossip') {
      const peerEvents = status.eventHash;
      return async.auto({
        localEvents: callback => ledgerNode.storage.events.getHashes({
          consensus: false,
          sort: 1
        }, callback),
        send: ['localEvents', (results, callback) => {
          const toSend =
            results.localEvents.filter(e => !peerEvents.includes(e));
          async.each(toSend, (e, callback) => async.auto({
            getEvent: callback => ledgerNode.storage.events.get(e, callback),
            sendEvent: ['getEvent', (results, callback) =>
            api._client.sendEvent(results.getEvent.event, peer.id, callback)]
          }, callback), callback);
        }],
        get: ['localEvents', (results, callback) => {
          const toGet =
            peerEvents.filter(e => !results.localEvents.includes(e));
          async.each(toGet, (e, callback) => async.auto({
            getEvent: callback => api._client.getEvent(e, peer.id, callback),
            addEvent: ['getEvent', (results, callback) =>
              api._events.add(results.getEvent, ledgerNode, {
                continuity2017: {peer: true}
              }, callback)]
          }, callback), callback);
        }]
      }, callback);
    }

    // 2.b. Certify votes if in 'decideEvents', 'decideRollCall', or
    //   'consensus' phases.
    if(['decideEvents', 'decideRollCall', 'consensus'].includes(
      status.consensusPhase)) {
      // TODO: send votes we know about to peer to optimize

      // TODO: validate `status.election`
      // TODO: make sure `Events` election topic is certified *BEFORE*
      //   roll-call because we depend on the votes for events to exist
      //   in the database (and they only get there if they are certified)
      return async.eachSeries(status.election, (election, callback) => {
        api._election.certify(
          ledgerNode, election.topic, election.electionResults, callback);
      }, callback);
    }

    return callback(new BedrockError(
      'Unknown consensus phase in block status.', 'NotSupportedError', {
        consensusPhase: status.consensusPhase
      }));
  });
}

// NOTE: THIS FUNCTION IS LEFT FOR COMMENTS ONLY, IT IS NOT USED
function _getBlockStatus(blockHeight, peer, callback) {
  // 1. GET <endpoint>/blocks/<height>/status

  // 2. B responds with its status resource for that block.

  // Example 1 (B has no such block yet):
  // {
  //   '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
  //   type: 'Error',
  //   errorType: 'NotFound'
  // }

  // Example 2 (no consensus or vote yet):
  // {
  //   blockHeight: <blockHeight>,
  //   consenusPhase: 'gossip',
  //   eventHash: [<eventHash1>, <eventHash2>, ...]
  // }

  // Example 3 (no consensus, round 1 through N vote):
  // {
  //   blockHeight: <blockHeight>,
  //   consenusPhase: 'decideEvents',
  //   election: [{
  //     topic: 'Events',
  //     electionResults: [{
  //       manifestHash: <manifestHash1>,
  //       round: 1,
  //       voter: <voter_id1>,
  //       signature: ...
  //     }, {
  //       manifestHash: <manifestHash2>,
  //       round: 1,
  //       voter: <voter_id2>,
  //       signature: ...
  //     }]
  //   }]
  // }

  // Example 3 (no consensus, round 1 through N vote):
  // {
  //   blockHeight: <blockHeight>,
  //   consenusPhase: 'decideRollCall',
  //   election: [{
  //     topic: 'Events',
  //     electionResults: [{
  //       manifestHash: <manifestHash1>,
  //       round: 1,
  //       voter: <voter_id1>,
  //       signature: ...
  //     }, {
  //       manifestHash: <manifestHash2>,
  //       round: 1,
  //       voter: <voter_id2>,
  //       signature: ...
  //     }]
  //   }, {
  //     topic: 'RollCall',
  //     electionResults: [{
  //       manifestHash: <manifestHash1>,
  //       round: 1,
  //       voter: <voter_id1>,
  //       signature: ...
  //     }, {
  //       manifestHash: <manifestHash2>,
  //       round: 1,
  //       voter: <voter_id2>,
  //       signature: ...
  //   }]
  // }

  // TODO: how does returning `consensus` as a phase help? Do we only need
  // the election phase -- after which the node should automatically detect
  // and verify consensus and avoid talking to other nodes?

  // Example 4 (consensus)
  // {
  //   blockHeight: <blockHeight>,
  //   phase: 'consensus',
  //   election: [{
  //     topic: 'Events',
  //     electionResults: [{
  //       manifestHash: <manifestHash1>,
  //       round: 4,
  //       voter: <voter_id1>,
  //       signature: ...
  //     }, {
  //       manifestHash: <manifestHash2>,
  //       round: 4,
  //       voter: <voter_id2>,
  //       signature: ...
  //     }]
  //   }, {
  //     topic: 'RollCall',
  //     electionResults: [{
  //       manifestHash: <manifestHash1>,
  //       round: 1,
  //       voter: <voter_id1>,
  //       signature: ...
  //     }, {
  //       manifestHash: <manifestHash2>,
  //       round: 1,
  //       voter: <voter_id2>,
  //       signature: ...
  //   }]
  // }

  // TODO: Note: If there is more than one vote for a particular voter ID,
  //   then the first vote received will be used. Alternatively, the vote
  //   for the manifest with the lowest hash could be used, but this would
  //   require updating the vote when a lower hash vote arrived -- which is
  //   is likely more problematic than simply accepting the first vote.

  // A new manifest will be created whenever a node is a voter and it has
  // gossiped with 67 peers. Whatever events it has on record at that time
  // will be bundled as a manifest and hashed. This constitutes the first
  // vote for the decider.
  //
  // The REST API can return back the list of hashes for a given manifest.
  // Manifests for blocks that have already achieved consensus may be safely
  // deleted. A manifest that has received a 2/3rds majority vote can be
  // dereferenced by looking at the events in the block it is associated with.
  // The event hashes for these events can be hashed (in lexicographical order)
  // to produce the manifest hash.
  //
  // For collection modeling, see:
  // https://github.com/digitalbazaar/bedrock-ledger-storage-mongodb/blob/master/lib/index.js#L99
}

function _writeBlock(
  ledgerNode, blockHeight, eventManifestHash, rollCallManifestHash, callback) {
  async.auto({
    config: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) => {
        if(err) {
          return callback(err);
        }
        const config = result.event.input[0];
        if(config.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'Consensus method must be "Continuity2017".',
            'InvalidStateError', {
              consensusMethod: config.consensusMethod
            }));
        }
        callback(null, config);
      }),
    eventManifest: callback => api._storage.manifests.get(
      ledgerNode.id, eventManifestHash, callback),
    rollCallManifest: callback => api._storage.manifests.get(
      ledgerNode.id, rollCallManifestHash, callback),
    events: ['eventManifest', (results, callback) =>
      // TODO: optimize getting events
      async.map(results.eventManifest.item, (eventHash, callback) =>
        ledgerNode.events.get(eventHash, callback), callback)],
    updateEvents: ['eventManifest', (results, callback) =>
      async.each(results.eventManifest.item, (eventHash, callback) =>
        ledgerNode.storage.events.update(eventHash, [{
          op: 'unset',
          changes: {
            meta: {
              pending: true
            }
          }
        }, {
          op: 'set',
          changes: {
            meta: {
              consensus: true,
              consensusDate: Date.now(),
              updated: Date.now()
            }
          }
        }], callback), callback)
    ],
    votes: ['rollCallManifest', (results, callback) => {
      // TODO: optimize getting votes
      api._votes.get(ledgerNode.id, blockHeight, 'Events', (err, records) => {
        if(err) {
          return callback(err);
        }
        const votes = records.filter(
          r => results.rollCallManifest.item.includes(r.meta.voteHash))
          .map(r => r.vote);
        callback(null, votes);
      });
    }],
    previousBlock: callback => ledgerNode.storage.blocks.getLatest(callback),
    block: ['config', 'events', 'votes', 'previousBlock', 'updateEvents',
      (results, callback) => {
        const block = {
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          id: results.config.ledger + '/blocks/' + blockHeight,
          blockHeight,
          consensusMethod: 'Continuity2017',
          type: 'WebLedgerEventBlock',
          event: results.events.map(e => e.event),
          electionResults: results.votes,
          previousBlock: results.previousBlock.eventBlock.block.id,
          previousBlockHash: results.previousBlock.eventBlock.meta.blockHash
        };
        api._hasher(block, (err, hash) => {
          if(err) {
            return callback(err);
          }
          callback(null, {
            block: block,
            meta: {
              blockHash: hash,
              consensus: true,
              consensusDate: Date.now()
            }
          });
        });
      }],
    store: ['block', (results, callback) =>
      ledgerNode.storage.blocks.add(
        results.block.block, results.block.meta, callback)]
  }, err => callback(err));
}
