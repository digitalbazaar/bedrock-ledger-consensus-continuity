/*
 * Web Ledger Continuity2017 consensus worker.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const niUri = require('ni-uri');
const BedrockError = bedrock.util.BedrockError;

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._storage = require('./storage');
api._voters = require('./voters');

// TODO: run every X interval via bedrock-jobs
api._run = (job, callback) => {
  // start a consensus session for ledgers
  const maxTime = bedrock.config['ledger-continuity'].worker.session.maxTime;
  const session = brLedger.consensus.createSession('consensus', maxTime);
  brLedger.consensus.execute('Continuity2017', session, _sync, err => {
    console.log('consensus job done', err);
    callback(err);
  });
  // TODO: add `async.auto` and reschedule consensus job or will job scheduler
  // handle this?
};

function _sync(ledgerNode, session, callback) {
  console.log('consensus job running');

  async.auto({
    voter: callback => api._voters.get({ledgerNodeId: ledgerNode.id}, callback),
    blockHeight: callback => _getBlockHeight(ledgerNode, callback),
    getElectors: ['blockHeight', (results, callback) => {
      console.log('LEDGERNODE_ID', ledgerNode.id);
      console.log('BLOCKHEIGHT', results.blockHeight);
      api._voters.getBlockElectors(ledgerNode, results.blockHeight, callback);
    }],
    election: ['getElectors', (results, callback) => {
      console.log('ELECTORS:', results.getElectors);
      _runElection(
        session, ledgerNode, results.voter, results.blockHeight,
        results.getElectors, callback);
    }],
    writeBlock: ['election', (results, callback) => {
      // TODO: ... since consensus decision cannot be changed perhaps fine
      //   to do block writing here even if work session has expired as
      //   attempting to write the same block ID with consensus flag=true
      //   will yield an error that can be safely ignored
      callback(new BedrockError(null, 'NotImplemented'));
    }]
  }, (err, results) => {
    console.log('ERROR', err);
    // TODO: implement
    callback(new BedrockError(null, 'NotImplemented', null, err));
  });
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
    const last = block.eventBlock.block.blockHeight;
    callback(null, last + 1);
  });
}

/**
 * Runs an election to the events and votes to write into the next block. This
 * function will determine both an event manifest and a roll call manifest. An
 * event manifest is an ordered list of event hashes and a roll call manifest is
 * an ordered list of elector hashes. It is possible for a `null` manifest to
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
 * @param callback(err, manifest) called once the operation completes.
 */
function _runElection(
  session, ledgerNode, voter, blockHeight, electors, callback) {
  // determine if the current ledger node is voting in this election
  const isElector = _isBlockElector(voter, electors);

  // continue to contact the entire voting population in every round...
  // ...voting at the end of the round (if participating) and
  // ...until a winning manifest is chosen or the work session expires
  const manifests = {
    events: null,
    rollCall: null
  };
  const condition = () => (manifests.events && manifests.rollCall) ||
    session.isExpired();
  async.until(condition, loop => {
    // TODO: consider using `eachLimit` instead and a configurable value
    // if any issues arise here

    // 1. Contact all voting peers once, at random, and in parallel.
    async.each(electors, (elector, nextElector) => {
      // no need to gossip if condition met
      if(condition()) {
        return nextElector();
      }

      async.auto({
        // 2. Gossip with a single elector.
        gossip: callback => _gossipWith(
          ledgerNode, voter, blockHeight, elector, err => {
            // ignore communicatino error from a single voter
            if(err) {
              return nextElector();
            }
            callback();
          }),
        // 3. Tally votes to check for consensus on the events.
        eventsVotes: ['gossip', (results, callback) =>
          _tally(ledgerNode.id, blockHeight, electors, 'Events', callback)],
        // 4. Tally votes to check for consensus on roll call.
        rollCallVotes: ['gossip', (results, callback) =>
          _tally(ledgerNode.id, blockHeight, electors, 'RollCall', callback)]
      }, (err, results) => {
        if(err) {
          return nextElector(err);
        }
        if(results.eventsVotes) {
          manifests.events = results.eventsVotes;
        }
        if(results.rollCallVotes) {
          manifests.rollCall = results.rollCallVotes;
        }
        nextElector();
      });
    }, err => {
      // nothing to vote on if error or if not an elector
      if(err || !isElector) {
        return loop(err);
      }

      // if there is no events manifest yet, vote for one
      if(!manifests.events) {
        // 5. All other electors have been contacted and no events manifest
        //   has been elected, so vote for one.
        return _voteForEvents(ledgerNode, voter, blockHeight, electors, loop);
      }

      // if there is no roll call manifest yet, vote for one
      if(!manifests.rollCall) {
        // 6. No agreed upon roll call selected yet for the events manifest
        //   vote; vote for one.
        return _voteForRollCall(ledgerNode, voter, blockHeight, electors, loop);
      }

      loop();
    });
  }, err => err ? callback(err) : callback(err, manifests));
}

/**
 * Determines if the given voter is in the passed voting population.
 *
 * @param voter the voter to check for.
 * @param electors the voting population.
 *
 * @return true if the voter is in the voting population, false if not.
 */
function _isBlockElector(voter, electors) {
  return electors.some(v => v.id === voter.id);
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

  // 1. Get block status from peer.
  // TODO: should be able to just call `api._client.getBlockStatus(peer, ...`
  //   and eliminate helper function here
  _getBlockStatus(blockHeight, peer, (err, status) => {
    if(err) {
      return callback(err);
    }

    // 2.a. Gossip with peer if in 'gossip' phase.
    if(status.phase === 'gossip') {
      // TODO: would be nice to reuse ledger-agent/events API for this

      // POST <endpoint>/events

      // {
      //   eventHash: [<all events known that `peer` doesn't know>]
      // }
      // OR ... potentially just one at a time, need to decide for simplicity
      // or bulk to eliminate overhead:
      // { full event }

      // In parallel, get every event not known that B knows:

      // GET <endpoint>/events/<eventHash>
      // { full event }
      return callback();
    }

    // 2.b. Certify votes if in 'decideEvents', 'decideRollCall', or
    //   'consensus' phases.
    if(['decideEvents', 'decideRollCall', 'consensus'].includes(
      status.consensusPhase)) {
      // TODO: validate `status.election`
      return async.each(status.election, (election, callback) => {
        _certifyVotes(election.topic, election.electionResults, callback);
      }, callback);
    }

    return callback(new BedrockError(
      'Unknown consensus phase in block status.', 'NotSupportedError', {
        consensusPhase: status.consensusPhase
      }));
  });
}

/**
 * Verifies and stores each vote.
 *
 * @param electionTopic the election topic ('Events' or 'RollCall').
 * @param votes the votes to certify.
 * @param callback(err) called once the operation completes.
 */
function _certifyVotes(electionTopic, votes, callback) {
  async.each(votes, (vote, callback) => {
    _verifyVote(vote, err => {
      if(err) {
        // ignore failed votes, do not store them
        return callback();
      }
      // TODO: store vote
    });
  }, callback);
}

/**
 * Tallies a vote for the passed block height. If there is a winning manifest
 * for the block, it will be returned, otherwise `null` will be returned.
 *
 * @param ledgerNodeId the ID of the ledger node that is tallying votes.
 * @param blockHeight the height of the block to tally the vote for.
 * @param electors the voting population for the block height.
 * @param electionType the type of votes to tally ('EventOrder' or 'RollCall').
 * @param callback(err, manifest) called once the operation completes.
 */
function _tally(ledgerNodeId, blockHeight, electors, electionType, callback) {
  /* Go through rounds and see if 2/3rds have voted yet in the round. If not,
    then finish the tally because we need to wait for more votes to arrive. If
    so, and a 2/3rds majority has voted for a particular manifest, finish tally
    and return the winner. */
  let done = false;
  let round = 0;
  // special case when electors < 3 -- every elector must agree.
  let twoThirds = (electors.length < 3) ?
    electors.length : Math.floor(electors.length / 3) * 2;
  let winner = null;
  async.until(() => done, callback => {
    api._storage.votes.tally(
      ledgerNodeId, blockHeight, electionType, round++, (err, votes) => {
      if(err) {
        return callback(err);
      }
      if(votes.length < twoThirds) {
        // not enough votes this round, break out to collect more votes
        done = true;
      } else if(votes[0].count > twoThirds) {
        // we have a winner in this round
        done = true;
        winner = votes[0].manifest;
      }
      callback();
    });
  }, err => callback(err, winner));
}

/**
 * Performs a vote for the ledger node identified by the `voter` information.
 * The vote will be for a manifest to use to create the next block which is
 * identified by `blockHeight`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the next block.
 * @param electors all electors for the next block.
 * @param callback(err) called once the operation completes.
 */
function _voteForEvents(ledgerNode, voter, blockHeight, electors, callback) {
  // get last vote cast by voter for the block
  api._storage.votes.getLast(
    ledgerNode.id, blockHeight, 'Events', voter.id, (err, vote) => {
    if(err) {
      return callback(err);
    }
    if(vote) {
      // previous vote has occurred, so do instant run-off and vote again
      return _castInstantRunoffEventVote(ledgerNode, vote, voter, callback);
    }
    _castFirstRoundEventVote(
      ledgerNode, voter, blockHeight, electors, callback);
  });
}

/**
 * Create and store a first round vote based off of all of the current events
 * that have not yet reached consensus.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter that is voting.
 * @param blockHeight the height of the next block.
 * @param electors all electors for the next block.
 * @param callback(err) called once the operation completes.
 */
function _castFirstRoundEventVote(
  ledgerNode, voter, blockHeight, electors, callback) {
  async.auto({
    createManifest: callback => _createEventManifest(
      ledgerNode, blockHeight, callback),
    recommendElectors: ['createManifest', (results, callback) =>
      _recommendElectors(
        ledgerNode, voter, electors, results.createManifest, callback)],
    signVote: ['recommendElectors', (results, callback) => {
      const vote = {
        // TODO: add `@context`
        blockHeight: blockHeight,
        manifest: results.createManifest.id,
        round: 1,
        voter: voter.id,
        recommendElectors: results.recommendElectors
      };
      // TODO: jsigs sign `vote`
      callback(null, vote);
    }],
    storeVote: ['signVote', (results, callback) => {
      api._storage.votes.add(ledgerNode.id, 'Events', results.signVote, err => {
        if(err && err.name === 'DuplicateError') {
          // this vote has already happened, via another process,
          // can safely ignore this error and proceed as if we wrote the vote
          callback();
        }
        callback(err);
      });
    }]
  }, err => callback(err));
}

/**
 * Creates a manifest from all current events that have not yet reached
 * consensus and stores it in the database.
 *
 * @param ledgerNode the ledger node.
 * @param blockHeight the height of the block.
 * @param callback(err, manifest) called once the operation completes.
 */
function _createEventManifest(ledgerNode, blockHeight, callback) {
  // TODO: limit number of events according to continuity config block?
  ledgerNode.storage.events.getHashes({
    consensus: false,
    // limit: x,
    sort: 1
  }, (err, hashes) => {
    if(err) {
      return callback(err);
    }
    if(hashes.length === 0) {
      // no events to vote on, don't create empty manifest
      return callback(null, null);
    }
    // create manifest hash (EOL delimited) and store manifest
    const hash = niUri.digest('sha-256', hashes.join('\n'), true);
    const manifest = {
      id: hash,
      blockHeight: blockHeight,
      events: hashes
    };
    api._storage.manifests.add(
      ledgerNode.id, manifest,
      err => err ? callback(err) : callback(null, manifest));
  });
}

/**
 * Recommends the next set of electors.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter.
 * @param electors all electors for the next block.
 * @param manifest the manifest selected by voter in the first round of voting.
 * @param callback(err, recommendedElectors) called once the operation
 *          completes.
 */
function _recommendElectors(ledgerNode, voter, electors, manifest, callback) {
  // TODO: use given parameters and blockchain to determine a set of
  // electors to recommend for the next block
  callback(new Error('Not implemented'));
}

function _castInstantRunoffEventVote(
  ledgerNode, vote, voter, electors, callback) {
  // TODO: increment round and vote again; run instant run off mechanism to
  //   determine the next manifest to vote for. This instant runoff
  //   vote method's implementation is debatable, but v1 (for now) should
  //   find the events that are in common amongst a 2/3rd's majority of
  //   manifests that were voted for and create a new manifest for that
  //   (if necessary) and vote for it.

  // TODO: store the vote in voteStorage and return nothing.

// TODO: NOTE: Everything below can be safely ignored, just left for
// comments and reasoning. Just the above two lines need to be implemented.

  // 6. If round is 0:
  //   6.1. Create a manifest out of all events that have not yet reached
  //     consensus. (Note: there may be other caveats
  //     here, like events aren't deleted and such)
  //   6.2. Increment round and sign the manifest as a vote for round 1 and
  //     add it to the vote collection for the ledger for the block.
  // 7. If round > 1:
  //   7.1. Execute instant runoff vote algorithm to determine
  //     next manifest to vote for. Increment round, sign vote and store it.

  // TODO: when voting, create a vote and store it to the DB, then return

  // TODO: Note: instant run off voting could involve picking the manifest
  // with the greatest number of events, followed by the least hash. Can this
  // be used as an attack? What about widest diversity of event origins --
  // would that be a better measure?
  // 1. manifest with most events from different parties (attack vector)
  //   1.a. are the parties the deciders or everyone? (attack vector)
  // 2. manifest with most events (attack vector)
  // 3. manifest with least hash (attack vector)
  // 4. DONE; DEFINITIVE.
  // ALTERNATIVE:
  // 1. Create a new manifest with all events seen by a 2/3rds majority of
  //   nodes based on the previous vote.

  // TODO: **PROBLEM** - list of votes for a particular block may not be
  //   the same on every ledger. If this information is included in the block
  //   then it will generate different block hashes. If this information is
  //   not included in the block, then it lacks integrity. Resolutions like
  //   only writing the first 67 votes will not work when there is network
  //   partitioning that has been overcome (or can we make that work?). It
  //   may be that agreement needs to be broadcast on a set of votes as well.
  //
  // TODO: could track which decider nodes know about which events as gossip
  // occurs (without requiring signatures on that knowledge). That could be
  // taken into account when computing the manifest to vote for -- and those
  // parties that you think will vote with you can be listed in your vote. So
  // only once those parties all agree will your vote pass.

// ---------
// Deciders:

// Init: Set phase to 'gossip' for block x.
// Phase 1: Gossip for with every decider at random, ignoring
//   failures. Set phase to 'manifest'.
// Phase 2: Produce a manifest from all known events and sign it.
//   Gossip with every decider at random again, collecting signed
//   manifests and ignoring failures. At least 67 signed manifests
//   must be collected to end the phase. Set phase to 'voting'.
// Phase 3: Determine the most likely manifest to win the vote and
//   sign round 1 of the vote for that manifest. Gossip with every
//   decider at random again, collecting signed votes and ignoring
//   failures. At least 67 signed votes must be collected to end
//   the phase. If any manifest has received 67 votes, end the phase
//   immediately and set the phase to 'consensus'. If, after collecting
//   67 votes there is no winner, determine the most likely manifest to
//   win the vote and proceed to round 2. Rinse and repeat.
// Note: Is there any advantage to having Phase 2 be separate from
//   Phase 3 or is it really the same thing as voting in round 0?
// Note: Once a 2/3rds majority vote is in, it is not so simple to add those
//   votes to a consensus block. Different nodes may receive different
//   parties (e.g. one has votes from 67 members, another has 68 -- but
//   all for the same manifest). If the votes are included in the block
//   hash, then there will actually be disagreement on the nodes. Either
//   the hash algorithm needs to omit votes (which harms the integrity of
//   the block chain), or another solution or round is required to vote
//   on which votes will be counted. That final vote can't be included in
//   the block, but at least a clear 2/3rds majority vote will be included in
//   the blockchain and it would have agreement across nodes.

// OLD deciders algorithm:
// Init: Set stage to 'gossip' for block x.
// Phase 1: Gossip for S milliseconds with every decider at
//   random, ignoring failures. Set stage to 'resolving'.
// Phase 2: Gather 67 signed gossips from deciders. Set
//   stage to 'voting'.
// Phase 3: Based on signed gossip, calculate what everyone
//   is likely to support and which members will support
//   it. Vote for this position in round 1 and sign it.
// Phase 4: Collect votes.
//   Note: If the signed gossip can be "the same" as a
//   vote, we can skip collecting votes, we will have
//   this information already.
//   Problem: We need to agree on whose votes will
//   be counted as a 2/3rds majority. We need to agree on
//   the "roll call".

// Old alternative, incomplete thought
// -----------
// Init: Set stage to 'gossip' for block x.
// Phase 1: Gossip for S milliseconds with every decider at
//   random, ignoring failures. Create manifest and sign
//   it. Set stage to 'resolving'.
// Phase 2: Gather 67 signed manifests from deciders. Set
//   stage to 'voting'. At this point, manifests are all
//   stored locally and it is known what is in them and
//   which other nodes have signed them.
// Phase 3: Based on signed manifests, locally calculate
//   which manifest the other 67 nodes will support.
//   Vote for the manifest most likely to receive
//     support and include the list of 67 most likely
//     supporters from the deciders, using WHAT to
//     break ties? hash(manifest + endpoint) comparison?
//   Gather votes from all members until 67 reply. If
//   there is a manifest with 67 votes and those 67
//   include (NOT GOING TO WORK: FLP)
}

/**
 * Performs a vote for the ledger node identified by the `voter` information.
 * The vote will be for a manifest to use to determine the roll call for
 * the previous election of the events for the next block.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the next block.
 * @param callback(err) called once the operation completes.
 */
function _voteForRollCall(ledgerNode, voter, blockHeight, electors, callback) {
  // get last vote cast by voter for the block
  api._storage.votes.getLast(
    ledgerNode.id, blockHeight, 'RollCall', voter.id, (err, vote) => {
    if(err) {
      return callback(err);
    }
    if(vote) {
      // previous vote has occurred, so do instant run-off and vote again
      return _castInstantRunoffRollCallVote(
        ledgerNode, vote, voter, electors, callback);
    }
    _castFirstRoundRollCallVote(ledgerNode, voter, blockHeight, callback);
  });
}

/**
 * Create and store a first round vote based off of all of the votes that
 * have been collected for the chosen events manifest.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter that is voting.
 * @param blockHeight the height of the block.
 * @param callback(err) called once the operation completes.
 */
function _castFirstRoundRollCallVote(ledgerNode, voter, blockHeight, callback) {
  async.auto({
    createManifest: callback => _createRollCallManifest(
      ledgerNode, blockHeight, callback),
    signVote: ['manifest', (results, callback) => {
      const vote = {
        // TODO: add `@context`
        blockHeight: blockHeight,
        manifest: results.createManifest,
        round: 1,
        voter: voter.id
      };
      // TODO: jsigs sign `vote`
      callback(null, vote);
    }],
    storeVote: ['signVote', (results, callback) => {
      api._storage.votes.add(
        ledgerNode.id, 'RollCall', results.signVote, err => {
        if(err && err.name === 'DuplicateError') {
          // this vote has already happened, via another process,
          // can safely ignore this error and proceed as if we wrote the vote
          callback();
        }
        callback(err);
      });
    }]
  }, err => callback(err));
}

/**
 * Creates a manifest from all current events that have not yet reached
 * consensus and stores it in the database.
 *
 * @param ledgerNode the ledger node.
 * @param blockHeight the height of the block.
 * @param callback(err, manifestHash) called once the operation completes.
 */
function _createRollCallManifest(ledgerNode, blockHeight, callback) {
  api._storage._votes.get(
    ledgerNode.id, blockHeight, 'Events', (err, votes) => {
    if(err) {
      return callback(err);
    }
    if(votes.length === 0) {
      // no votes, don't create empty manifest
      return callback(null, null);
    }
    // create manifest hash (EOL delimited) and store manifest
    const hashes = votes.map(vote => vote.voter);
    const hash = niUri.digest('sha-256', hashes.join('\n'), true);
    api._storage.manifests.add(ledgerNode.id, {
      id: hash,
      blockHeight: blockHeight,
      events: hashes
    }, err => err ? callback(err) : callback(null, hash));
  });
}

function _castInstantRunoffRollCallVote(
  ledgerNode, vote, voter, electors, callback) {
  _selectInstantRunoffRollCallManifest(
    ledgerNode, vote, electors, (err, manifestHash) => {
    // TODO: create and store new vote
    callback(new Error('Not implemented'));
  });
}

function _selectInstantRunoffRollCallManifest(
  ledgerNode, vote, electors, callback) {
  /* Note: If any particular roll call manifest has 51% or more votes, choose
    it. Otherwise, pick the roll call manifest with the greatest number of
    electors (manifest length). Break ties using lexicographically least
    manifest hash. */
  let majority = Math.floor(electors.length / 2) + 1;
  api._storage.votes.tally(
    ledgerNode.id, vote.blockHeight, 'RollCall', vote.round, (err, tallies) => {
    if(tallies[0].count >= majority) {
      // there's a majority of votes for a particular manifestHash; select it
      return callback(null, tallies[0].manifestHash);
    }

    // filter tallies by highest count
    const highCount = tallies[0].count;
    tallies = tallies.filter(tally => tally.count === highCount);

    // no majority, choose roll call manifest with greatest number of electors
    api._storage.manifests.getAllByLength(
      ledgerNode.id, vote.blockHeight, 'RollCall', (err, results) => {
      // add manifest length to tallies
      const lengths = {};
      results.forEach(r => lengths[r.manifestHash] = r.length);
      for(const i = 0; i < tallies.length; ++i) {
        const tally = tallies[i];
        if(!tally.manifestHash in lengths) {
          // should never happen; manifests must be stored before votes are
          // stored according to algorithm
          return callback(new BedrockError(
            'Roll call manifest missing.', 'InvalidStateError', {
              manifestHash: tally.manifestHash
            }));
        }
        tally.manifestLength = lengths[tally.manifestHash];
      }

      // sort tallies by manifest length
      tallies.sort((a, b) => a.manifestLength - b.manifestLength);

      // filter tallies by longest manifest
      const highLength = tallies[0].manifestLength;
      tallies = tallies.filter(tally => tally.manifestLength === highLength);

      if(tallies.length > 0) {
        // still no single tally has won, select by manifest hash
        tallies.sort((a, b) => a.localCompare(b));
      }

      callback(null, tallies[0].manifestHash);
    });
  });
}

function _getBlockStatus(blockHeight, peer, callback) {
  // TODO: implement
  return callback(new BedrockError(null, 'NotImplemented'));

  // 1. GET <endpoint>/blocks/<height>/status

  // 2. B responds with its status resource for that block.

  // Example 1 (B has no such block yet):
  // {
  //   '@context': 'webledger/v1',
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

function _verifyVote(vote, callback) {
  // 1. Verify signature on vote.
  _verifyVoteSignature(vote, err => {
    if(err) {
      return callback(err);
    }
    // 2. Obtain (and verify) manifest.
    _getManifest(vote.id, vote.manifest, callback);
  });
}

function _verifyVoteSignature(vote, callback) {
  // TODO: implement
  callback(new BedrockError(null, 'NotImplemented'));
}

function _getManifest(voterId, manifest, callback) {
  // TODO: get `peer` via voter ID

  // TODO: download manifest from peer, use queue to optimize both the
  // retrieval of the manifest and its events
  // TODO: would be more efficient to get a list of all event hashes across
  // manifests? but that does not check manifest hashes... and may be
  // less efficient during voting

  // GET <endpoint>/manifests/<manifestHash>

  // {
  //   eventHash: [<eventHash1>, <eventHash2>, ...]
  // }

  // For each unknown <eventHash>, fetch full event:

  // GET <endpoint>/events/<eventHash>

  // { full event }

  // TODO: verify each event? (as in, do we need another layer of signatures
  // from nodes on each event, or will the hashes do?... do we only need
  // signatures for the voting part?)

  callback(new BedrockError(null, 'NotImplemented'));
}
