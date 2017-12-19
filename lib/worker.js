/*
 * Web Ledger Continuity2017 consensus worker.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const database = require('bedrock-mongodb');
const brLedgerNode = require('bedrock-ledger-node');
const events = require('./events');
const logger = require('./logger');
const signature = require('./signature');
const util = require('util');
const BedrockError = bedrock.util.BedrockError;

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._election = require('./election');
api._events = require('./events');
api._hasher = brLedgerNode.consensus._hasher;
api._storage = require('./storage');
api._voters = require('./voters');
// exposed for testing
api._gossipWith = _gossipWith;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('./temporaryLedgerNodeMeta');

api.scheduleWork = (session) => {
  // start a consensus session for ledgers
  const maxTime =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  session.start(maxTime, _guardedSync, err => {
    if(err) {
      logger.error('Error starting consensus job.', err);
    }
  });
};

// Note: exposed for testing
let _testMode = false;
api._run = (ledgerNode, callback) => {
  _testMode = true;
  _sync({
    ledgerNode: ledgerNode,
    isExpired: () => false,
    timeRemaining: () => Infinity
  }, callback);
};

function _guardedSync(session, callback) {
  // do not allow sync until `waitUntil` time
  _ledgerNodeMeta.get(session.ledgerNode.id, (err, meta) => {
    if(err) {
      return callback(err);
    }
    const waitUntil = _.get(meta, 'consensus-continuity.waitUntil');
    if(waitUntil && waitUntil > Date.now()) {
      // do not run consensus yet
      logger.verbose('consensus job delaying until ' + new Date(waitUntil),
        {ledgerNodeId: session.ledgerNode.id, waitUntil: waitUntil});
      return callback();
    }
    // ready to run consensus
    _sync(session, callback);
  });
}

function _sync(session, callback) {
  const ledgerNode = session.ledgerNode;
  logger.verbose('consensus job running', {ledgerNodeId: ledgerNode.id});

  async.auto({
    getState: callback => {
      // get state from the last work session
      _ledgerNodeMeta.get(session.ledgerNode.id, (err, meta) => {
        if(err) {
          return callback(err);
        }
        // update state with current session ID
        const state = _.get(meta, 'consensus-continuity.state', {
          sessionId: session.id,
          contacted: {}
        });
        state.sessionId = session.id;
        callback(null, state);
      });
    },
    claimState: ['getState', (results, callback) => {
      _ledgerNodeMeta.claimState(ledgerNode.id, results.getState, callback);
    }],
    voter: callback => api._voters.get(ledgerNode.id, callback),
    nextBlock: callback => _getNextBlockInfo(ledgerNode, callback),
    getElectors: ['nextBlock', (results, callback) => {
      api._election.getBlockElectors(
        ledgerNode, results.nextBlock.blockHeight, callback);
    }],
    getPeers: ['getElectors', (results, callback) => {
      _getPeers(ledgerNode, results.getElectors, callback);
    }],
    election: ['voter', 'getElectors', 'claimState', (results, callback) =>
      _runElectionNEW({
        session,
        ledgerNode,
        state: results.getState,
        voter: results.voter,
        previousBlockHash: results.nextBlock.previousBlockHash,
        blockHeight: results.nextBlock.blockHeight,
        electors: results.getElectors,
        peers: results.getPeers
      }, callback)],
    updateState: ['election', (results, callback) => {
      _ledgerNodeMeta.updateState(ledgerNode.id, results.getState, callback);
    }],
    writeBlock: ['updateState', (results, callback) => {
      if(!results.election.consensus || session.isExpired()) {
        return callback(null, false);
      }
      //console.log('WWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWWW');
      _writeBlock({
        ledgerNode,
        blockHeight: results.nextBlock.blockHeight,
        consensusResult: results.election.consensus
      }, callback);
    }],
    clearState: ['writeBlock', (results, callback) => {
      if(!results.writeBlock) {
        // no block written
        return callback();
      }
      //console.log('XXXXXXXXXXX', ledgerNode.storage.blocks.collection.s.name);
      // block written, clear contacts
      results.getState.contacted = {};
      _ledgerNodeMeta.updateState(
        ledgerNode.id, results.getState, callback);
    }]
  }, err => {
    logger.debug('Work session completed.', {
      session: session.id
    });
    callback(err);
  });
}

/**
 * Gets the latest consensus block and returns the new proposed block height
 * for the ledger (i.e. the current `blockHeight + 1`) and the latest block
 * hash as what would become the next `previousBlockHash`.
 *
 * @param ledgerNode the ledger node to get the latest block for.
 * @param callback(err, {blockHeight, previousBlockHash}) called once the
 *          operation completes.
 */
function _getNextBlockInfo(ledgerNode, callback) {
  // Note: This consensus method assumes that `blockHeight` will always exist
  // on the previous block because it cannot be used on a blockchain that
  // does not have that information. There has presently been no mechanism
  // devised for switching consensus methods between hashgraph-like blocks
  // and typical blockchains with block heights.
  ledgerNode.storage.blocks.getLatestSummary((err, block) => {
    if(err) {
      return callback(err);
    }
    const previousBlockHash =
      _.get(block, 'eventBlock.meta.blockHash');
    const last = _.get(block, 'eventBlock.block.blockHeight');
    if(last === undefined) {
      return callback(new BedrockError(
        'blockHeight is missing from latest block.', 'NotFoundError', {
          block
        }));
    }
    callback(null, {blockHeight: last + 1, previousBlockHash});
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
 * @param state the consensus state that persists across work sessions.
 * @param voter the voter information for the ledger node.
 * @param previousBlockHash the hash for the block to build on.
 * @param blockHeight the height of the block for the next manifest.
 * @param electors the voting population that will select the next manifest.
 * @param peers all peers to gossip with (all electors plus any additional
 *          peers associated with the ledger node).
 * @param callback(err, electionResult) called once the operation completes.
 */
function _runElection({
  session,
  ledgerNode,
  state,
  voter,
  previousBlockHash,
  blockHeight,
  electors,
  peers
}, callback) {
  // determine if the current ledger node is voting in this election
  const isElector = api._election.isBlockElector(voter, electors);

  // continue to contact the entire voting population in every round...
  // ...voting at the end of the round (if participating) and
  // ...until a winning manifest is chosen or the work session expires
  const electionResult = {
    events: {},
    rollCall: {},
    done: false,
    // TODO: add consensusResult, remove `events` and `rollCall`
  };

  const gossipInterval =
    config['ledger-consensus-continuity'].worker.election.gossipInterval;

  // Note: Loop is only used in test mode, in non-test mode, every code
  // path will terminate the work session and then a scheduler will start
  // a new work session later
  let expired = false;
  const condition = () =>
    expired ||
    (electionResult.events.winner && electionResult.rollCall.winner) ||
    // must provide enough time for session state and block to be written
    // afterwards
    session.timeRemaining() < 10000;
  let loopCount = 0;
  async.until(condition, loop => {
    loopCount++;
    logger.debug('LOOP COUNT', {loopCount});
    _contactElectors(
      {ledgerNode, voter, previousBlockHash, blockHeight, electors,
        peers, condition, electionResult, state}, err => {
      if(err) {
        return loop(err);
      }

      // Note: At this point, all electors have been contacted or we have
      // run out of time to contact any more.

      // create callback that will expire current `loop` and postpone further
      // consensus work by the gossip interval
      let newEventsFound = false;
      const expire = () => {
        if(_testMode) {
          // only expire during testing when the gossipInterval exceeds
          // the session time remaining
          if(gossipInterval >= session.timeRemaining()) {
            expired = true;
          }
          return setTimeout(loop, gossipInterval);
        }

        // always expire in non-test mode
        expired = true;

        if(newEventsFound) {
          // new events have been found so reschedule as soon as possible to
          // get them into a block, do not wait for `gossipInterval` to expire
          logger.debug(
            'New events detected; rescheduling as soon as possible.', {
            ledgerNodeId: ledgerNode.id,
            blockHeight
          });
          return loop();
        }
        logger.debug(
          'No new events detected and work session expired; ' +
          'idling until gossip interval expires.', {
          blockHeight,
          gossipInterval,
          timeStamp: Date.now()
        });
        return _ledgerNodeMeta.setWaitUntil(
          ledgerNode.id, Date.now() + gossipInterval, loop);
      };

      // TODO: need a better check than this -- or we need to ensure that
      //   bogus events will get deleted so they won't get returned here
      //   as valid "new" events for a block

      // see if there are any events to put in a block
      ledgerNode.storage.events.getHashes({
        consensus: false,
        limit: 1
      }, (err, hashes) => {
        if(err) {
          return loop(err);
        }
        if(hashes.length === 0) {
          return expire();
        }

        // new events found that could go into a block
        newEventsFound = true;

        // nothing to vote on if not an elector, so simply expire
        if(!isElector) {
          return expire();
        }

        // if not every elector has been contacted yet, do not vote yet
        if(Object.keys(state.contacted).length < electors.length) {
          return expire();
        }

        // if there is no events manifest yet, vote for one
        if(!electionResult.events.winner) {
          // 5. All other electors have been contacted and no events manifest
          //   has been elected, so vote for one.
          return api._election.voteForEvents({ledgerNode, voter, blockHeight,
            electors, round: electionResult.events.currentRound},
            err => err ? loop(err) : expire());
        }

        // if there is no roll call manifest yet, vote for one
        if(!electionResult.rollCall.winner) {
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
          return api._election.voteForRollCall({ledgerNode, voter, blockHeight,
            electors, round: electionResult.rollCall.currentRound},
            err => err ? loop(err) :expire());
        }

        expire();
      });
    });
  }, err => err ? callback(err) : callback(err, electionResult));
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
 * @param state the consensus state that persists across work sessions.
 * @param voter the voter information for the ledger node.
 * @param previousBlockHash the hash for the block to build on.
 * @param blockHeight the height of the block for the next manifest.
 * @param electors the voting population that will select the next manifest.
 * @param peers all peers to gossip with (all electors plus any additional
 *          peers associated with the ledger node).
 * @param callback(err, electionResult) called once the operation completes.
 */
function _runElectionNEW({
  session,
  ledgerNode,
  state,
  voter,
  previousBlockHash,
  blockHeight,
  electors,
  peers
}, callback) {
  // continue gossiping with the current electors until consensus
  // is achieved or the work session expires
  const electionResult = {
    consensus: null
  };

  const gossipInterval =
    config['ledger-consensus-continuity'].worker.election.gossipInterval;

  // Note: Loop is only used in test mode, in non-test mode, every code
  // path will terminate the work session and then a scheduler will start
  // a new work session later
  let expired = false;
  const condition = () =>
    expired || electionResult.consensus ||
    // must provide enough time for session state and block to be written
    // afterwards
    session.timeRemaining() < 10000;

  // create callback that will expire current `loop` and postpone further
  // consensus work by the gossip interval
  let newEventsFound = false;
  const expire = loop => {
    if(_testMode) {
      // only expire during testing when the gossipInterval exceeds
      // the session time remaining
      if(gossipInterval >= session.timeRemaining()) {
        expired = true;
      }
      return setTimeout(loop, gossipInterval);
    }

    // always expire in non-test mode
    expired = true;

    if(newEventsFound) {
      // new events have been found so reschedule as soon as possible to
      // get them into a block, do not wait for `gossipInterval` to expire
      logger.debug(
        'New events detected; rescheduling as soon as possible.', {
        ledgerNodeId: ledgerNode.id,
        blockHeight
      });
      return loop();
    }
    logger.debug(
      'No new events detected and work session expired; ' +
      'idling until gossip interval expires.', {
      blockHeight,
      gossipInterval,
      timeStamp: Date.now()
    });
    return _ledgerNodeMeta.setWaitUntil(
      ledgerNode.id, Date.now() + gossipInterval, loop);
  };

  // FIXME: remove `loopCount`
  let loopCount = 0;
  async.until(condition, loop => {
    loopCount++;
    logger.debug('LOOP COUNT', {loopCount});
    newEventsFound = false;

    async.auto({
      gossip: callback => _contactElectorsNEW(
        {ledgerNode, voter, previousBlockHash, blockHeight, electors,
          peers, condition, electionResult, state}, callback),
      newRegularEvents: ['gossip', (results, callback) => {
        logger.debug('L490 Start getHashes');
        // TODO: need a better check than this -- or we need to ensure that
        //   bogus events will get deleted so they won't get returned here
        //   as valid "new" events for a block

        // see if there are any non-merge events to put in a block
        ledgerNode.storage.events.getHashes({
          // FIXME: critical to also ensure type is not `ContinuityMergeEvent`
          consensus: false,
          limit: 1
        }, (err, hashes) => {
          logger.debug('L500 getHashes Result', {err, hashes});
          if(err) {
            return loop(err);
          }
          if(hashes.length === 0) {
            // nothing to merge or find consensus on
            return expire(loop);
          }
          newEventsFound = true;
          callback();
        });
      }],
      history: ['newRegularEvents', (results, callback) =>
        api._events.getRecentHistory({ledgerNode}, callback)],
      merge: ['history', (results, callback) => {
        logger.debug('L514 START MERGE');
        if(condition()) {
          logger.debug('L516 insufficient time remaining to merge');
          // insufficient time remaining to merge
          return expire(loop);
        }
        logger.debug('L520 running mergeBranches');
        api._events.mergeBranches(
          {history: results.history, ledgerNode}, callback);
      }],
      history2: ['merge', (results, callback) =>
        api._events.getRecentHistory({ledgerNode}, callback)],
      consensus: ['history2', (results, callback) =>
        api._election.findConsensus(
          {electors, history: results.history2, ledgerNode}, (err, result) => {
            if(err) {
              return callback(err);
            }
            electionResult.consensus = result;
            expire(loop);
          })]
    });
  }, err => err ? callback(err) : callback(err, electionResult));
}

function _contactElectorsNEW({
  ledgerNode, voter, previousBlockHash, blockHeight, electors,
  peers, condition, state
}, callback) {
  // maximum number of peers to communicate with at once
  const limit =
    bedrock.config['ledger-consensus-continuity'].gossip.concurrentPeers;

  // filter electors by those already contacted
  const contacted = electors.filter(
    elector => (database.hash(elector.id) in state.contacted));

  // restrict electors to contact to 1/3 of total or limit and exclude self
  const toContact = _.difference(electors, contacted).filter(
    e => e.id !== voter.id);
  const max = Math.min(
    toContact.length - 1, Math.min(limit, Math.ceil(electors.length / 3)));
  toContact.length = max + 1;

  logger.debug('NUMBER OF PREVIOUSLY CONTACTED ELECTORS REMOVED', {
    contacted: contacted.length, remainingToContact: toContact.length
  });

  // 1. Contact N electors at random, in parallel.
  const random = _.shuffle(toContact);
  logger.debug('ELECTORS TO BE CONTACTED', random);
  //console.log('ELECTORS TO BE CONTACTED', random);
  async.eachLimit(random, limit, (elector, nextElector) => {
    // no need to gossip if condition met
    if(condition()) {
      return nextElector();
    }
    // 2. Gossip with a single elector.
    _gossipWith(
      {ledgerNode, peerId: elector.id,
      previousBlockHash, blockHeight, elector}, err => {
      // ignore communication error from a single voter
      if(err) {
        // TODO: revert to verbose
        //logger.verbose('Non-critical error in _gossipWith.', err);
        logger.debug('Non-critical error in _gossipWith.', err);
        state.contacted[database.hash(elector.id)] = {error: err};
      } else {
        state.contacted[database.hash(elector.id)] = true;
      }
      nextElector();
    });
  }, callback);
}

function _contactElectors(
  {ledgerNode, voter, previousBlockHash, blockHeight, electors,
  peers, condition, electionResult, state}, callback) {
  // maximum number of peers to communicate with at once
  const limit =
    bedrock.config['ledger-consensus-continuity'].gossip.concurrentPeers;

  // filter electors by those already contacted
  let toContact = electors.filter(
    elector => !(database.hash(elector.id) in state.contacted));
  logger.debug('NUMBER OF PREVIOUSLY CONTACTED ELECTORS REMOVED', {
    removedElectorCount: (electors.length - toContact.length),
    remainingToContact: toContact.length
  });

  if(toContact.length === 0) {
    // clear contacts from state
    state.contacted = {};
    // all electors have been contacted, reset electors entirely
    toContact = electors.slice();
  }

  // TODO: rewrite gossip, tally, etc. to use gossip, merge, consensus, instead
  // TODO: new code would be
  //   1. gossip with N electors at random
  //   2. merge branches
  //   3. findConsensus
  //   4. If consensus is found, write block
  //   5. repeat until out of time
  // TODO: note that doing merge and then consensus (rather than the other way
  //   around) may require manually adding in the new merge event ... or
  //   retrieving recent history twice, the second time after the merge event
  //   is successfully added)

  // 1. Contact all remaining electors (up to limit), at random, in parallel.
  const random = _.shuffle(toContact);
  logger.debug('ELECTORS TO BE CONTACTED', random);
  async.eachLimit(random, limit, (elector, nextElector) => {
    // no need to gossip if condition met
    if(condition()) {
      return nextElector();
    }

    logger.debug('GOSSIPING WITH ' + elector.id);
    async.auto({
      // 2. Gossip with a single elector.
      gossip: callback => _gossipWith(
        ledgerNode, voter, previousBlockHash, blockHeight, elector,
        (err, consensusPhase) => {
          // ignore communication error from a single voter
          if(err) {
            // TODO: revert to verbose
            //logger.verbose('Non-critical error in _gossipWith.', err);
            logger.debug('Non-critical error in _gossipWith.', err);
            state.contacted[database.hash(elector.id)] = {error: err};
            return nextElector();
          }
          // TODO: what should `consensusPhase` default to for self? right
          //  now it's using `undefined`
          state.contacted[database.hash(elector.id)] = {consensusPhase};
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
      electionResult.events = results.eventsVotes;
      electionResult.rollCall = results.rollCallVotes;
      if(results.eventsVotes.winner && results.rollCallVotes.winner) {
        electionResult.done = true;
      }
      nextElector();
    });
  }, callback);
}

function _gossipMergeEvent({mergeEvent, ledgerNode, peerId}, callback) {
  // this operation must be performed in series
  async.auto({
    signature: callback => signature.verify(
      {doc: mergeEvent, ledgerNodeId: ledgerNode.id}, callback),
    // retrieve all parent events in parallel
    difference: ['signature', (results, callback) =>
      ledgerNode.storage.events.difference(
        mergeEvent.parentHash, callback)],
    parentEvents: ['difference', (results, callback) =>
      async.map(results.difference, (eventHash, callback) =>
        api._client.getEvent({eventHash, peerId}, callback), callback)],
    store: ['parentEvents', (results, callback) => {
      async.eachSeries(results.parentEvents, (event, callback) => {
        if(event.type === 'WebLedgerEvent') {
          // add the event
          return ledgerNode.events.add(
            event, {continuity2017: {peer: true}}, err => {
              if(err && err.name === 'DuplicateError') {
                err = null;
              }
              callback(err);
            });
        }
        if(Array.isArray(event.type) &&
          event.type.includes('ContinuityMergeEvent')) {
          //console.log('-------------', event.signature.creator);
          return _gossipMergeEvent(
            {mergeEvent: event, ledgerNode, peerId}, callback);
        }
        callback(new BedrockError(
          'Unknown event type.', 'DataError', {event}));
      }, callback);
    }],
    // store the initial merge event
    storeMerge: ['store', (results, callback) => ledgerNode.events.add(
      mergeEvent, {continuity2017: {peer: true}}, err => {
        if(err && err.name === 'DuplicateError') {
          err = null;
        }
        callback(err);
      })]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback();
  });
}

// eventHashes contains a list of mergeEvent hashes available on the peer
// the merge events will contain a peerHash array which contains a list
// of other merge event hashes and regular event hashes. Regular events
// must be added to the collection before merge events that reference those
// regular events
function _gossipEvents({eventHashes, ledgerNode, peerId}, callback) {
  // this operation must be performed in series
  async.auto({
    difference: callback => ledgerNode.storage.events.difference(
      eventHashes, callback),
    events: ['difference', (results, callback) => async.eachSeries(
      results.difference, (eventHash, callback) => {
        async.auto({
          mergeEvent: callback => api._client.getEvent(
            {eventHash, peerId}, callback),
          gossipMergeEvent: ['mergeEvent', (results, callback) => {
            _gossipMergeEvent(
              {mergeEvent: results.mergeEvent, ledgerNode, peerId}, callback);
          }]
        }, (err, results) => {
          if(err) {
            return callback(err);
          }
          callback();
        });
      }, callback)]
  }, callback);
}

// FIXME: update documentation
/**
 * Causes the given ledger node, identified by the given `voter` information,
 * to gossip with a `peer` about the next block identified by `blockHeight`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param previousBlockHash the hash of the most current block (the one to
 *          build on top of as the new previous block).
 * @param blockHeight the height of the next block (the one to gossip about).
 * @param peer the peer to gossip with.
 * @param callback(err) called once the operation completes.
 */
function _gossipWith({ledgerNode, peerId}, callback) {
  const eventsCollection = ledgerNode.storage.events.collection;
  async.auto({
    treeHash: callback => api._events._getLocalBranchHead(
      {eventsCollection, creator: peerId}, callback),
    peerHistory: ['treeHash', (results, callback) => api._client.getHistory(
      {peerId, treeHash: results.treeHash}, callback)],
    getEvents: ['peerHistory', (results, callback) => _gossipEvents(
      {eventHashes: results.peerHistory, ledgerNode, peerId}, callback)],
    localHistory: ['peerHistory', (results, callback) => events.getHistory({
      eventHashFilter: results.peerHistory,
      ledgerNode,
      treeHash: results.treeHash
    }, (err, result) => {
      // TODO: first implementation getHistory is designed to produce an
      // ordered list of regular events and merge events. The events in the
      // list should be transmitted to the peer serially.
      callback(err, result);
    })],
    sendEvents: ['localHistory', (results, callback) => async.eachSeries(
      results.localHistory, (eventHash, callback) => async.auto({
        get: callback => ledgerNode.storage.events.get(eventHash, callback),
        send: ['get', (results, callback) => api._client.sendEvent(
          {event: results.get.event, eventHash, peerId}, callback)]
      }, callback), callback)]
  }, err => callback(err));
}

// TODO: document
// consensusResult = {event: [event records], consensusProof: [event records]}
function _writeBlock({ledgerNode, blockHeight, consensusResult}, callback) {
  async.auto({
    config: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) => {
        if(err) {
          return callback(err);
        }
        const config = result.event.ledgerConfiguration;
        if(config.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'Consensus method must be "Continuity2017".',
            'InvalidStateError', {
              consensusMethod: config.consensusMethod
            }));
        }
        callback(null, config);
      }),
    updateEvents: callback =>
      async.each(consensusResult.event, (record, callback) => {
        const now = Date.now();
        ledgerNode.storage.events.update(record.eventHash, [{
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
              consensusDate: now,
              updated: now
            }
          }
        }], callback);
      }, callback),
    keys: callback => {
      const signatureCreators = _.uniq(
        // FIXME: there are regular events included here that do not include
        // signatures, using filter now, better way?
        consensusResult.event.filter(r => r.event.signature)
          .map(r => r.event.signature.creator)
          .concat(consensusResult.consensusProof
            .map(r => r.event.signature.creator)));
      async.map(
        signatureCreators, (keyId, callback) => api._storage.keys.getPublicKey(
          ledgerNode.id, {id: keyId}, callback), (err, result) => {
          if(err) {
            return callback(err);
          }
          const publicKey = result.map(key => key.seeAlso ?
            {id: key.id, seeAlso: key.seeAlso} : {
              id: key.id,
              type: key.type,
              owner: key.owner,
              publicKeyPem: key.publicKeyPem
            });
          callback(null, publicKey);
        });
    },
    previousBlock: callback =>
      ledgerNode.storage.blocks.getLatestSummary(callback),
    createBlock: ['config', 'keys', 'previousBlock', 'updateEvents',
      (results, callback) => {
        const block = {
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          id: results.config.ledger + '/blocks/' + blockHeight,
          blockHeight,
          consensusMethod: 'Continuity2017',
          type: 'WebLedgerEventBlock',
          event: consensusResult.event.map(r => r.event.event),
          consensusProof: consensusResult.consensusProof.map(r => r.event),
          previousBlock: results.previousBlock.eventBlock.block.id,
          previousBlockHash: results.previousBlock.eventBlock.meta.blockHash,
          publicKey: results.keys
        };
        api._hasher(block, (err, blockHash) => {
          if(err) {
            return callback(err);
          }
          // convert events to event hashes
          block.event = consensusResult.eventHash;
          // TODO: ensure storage supports `consensusProof` event hash lookup
          block.consensusProof = consensusResult.consensusProof;
          callback(null, {
            block: block,
            meta: {blockHash}
          });
        });
      }],
    store: ['createBlock', (results, callback) =>
      ledgerNode.storage.blocks.add(
        results.createBlock.block, results.createBlock.meta, callback)],
    updateKey: ['store', (results, callback) => {
      const toUpdate = results.keys
        .filter(key => !key.seeAlso)
        .map(key => {
          key.seeAlso = results.createBlock.block.id;
          return key;
        });
      async.each(toUpdate, (key, callback) =>
        api._storage.keys.updatePublicKey(
          ledgerNode.id, key, callback), callback);
    }],
    updateBlock: ['updateKey', (results, callback) => {
      const patch = [{
        op: 'set',
        changes: {meta: {consensus: true, consensusDate: Date.now()}}
      }];
      const blockHash = results.createBlock.meta.blockHash;
      ledgerNode.storage.blocks.update(blockHash, patch, callback);
    }]
  }, err => callback(err, err ? false : true));
}
