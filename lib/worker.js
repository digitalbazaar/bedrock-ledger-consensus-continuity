/*
 * Web Ledger Continuity2017 consensus worker.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');

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
  // FIXME: remove me
  return callback();

  async.auto({
    voter: callback => api._voters.get(ledgerNode.id, callback),
    nextBlockId: callback => _getNextBlockId(ledgerNode, callback),
    voters: ['nextBlock', (results, callback) =>
      api._voters.get(ledgerNode.id, results.nextBlockId, callback)],
    nextManifest: ['voters', (results, callback) => _electNextManifest(
      session, ledgerNode, results.voter, results.blockId,
      results.voters, callback)],
    writeBlock: ['nextManifest', (results, callback) => {
      // TODO: should this be done here? what about work session timeouts?
      // TODO: maybe a separate worker should be used to write blocks vs.
      // TODO: this gossip one?
      // TODO: ... since consensus decision cannot be changed perhaps fine
      //   to do block writing here even if work session has expired
      callback(new Error('Not implemented'));
    }]
  }, (err, results) => {
    // TODO: implement
    callback(new Error('Not implemented'));
  });
}

/**
 * Gets the latest consensus block and returns the ID for the next block that
 * will follow.
 *
 * @param ledgerNode the ledger node to get the latest block for.
 * @param callback(err, nextBlockId) called once the operation completes.
 */
function _getNextBlockId(ledgerNode, callback) {
  // TODO: get latest consensus block from ledgerNode storage

  // TODO: use block incrementer function (which is probably *this* function)
  // to increment block ID and return it
  callback(new Error('Not implemented'));
}

/**
 * Runs an election to determine the manifest to use for the next block. A
 * "manifest" is a collection of event hashes. It is possible for a `null`
 * manifest to be returned. This occurs when the work session has expired
 * before the election was over. Another work session will have to run the
 * election later to determine the winning manifest.
 *
 * Running the election involves gossiping events with a voting population
 * and gathering their votes until a 2/3rds majority agrees upon the next
 * manifest.
 *
 * Once elected, the events in the manifest will need to be run through
 * any appropriate event guards to produce the final consensus block, which
 * is work that is not performed by this function.
 *
 * @param session the current work session.
 * @param ledgerNode the ledger node being worked on.
 * @param voter the voter information for the ledger node.
 * @param blockId the ID of the block for the next manifest.
 * @param voters the voting population that will select the next manifest.
 * @param callback(err, manifest) called once the operation completes.
 */
function _electNextManifest(
  session, ledgerNode, voter, blockId, voters, callback) {
  // determine if the current ledger node is voting in this election
  const isVoting = _isVoting(voter, voters);

  // continue to contact the entire voting population in every round...
  // ...voting at the end of the round (if participating) and
  // ...until a winning manifest is chosen or the work session expires
  let winner;
  async.until(() => winner || session.isExpired(), loop => {
    // TODO: consider using `eachLimit` instead and a configurable value
    // if any issues arise here

    // 1. Contact all voting peers once, at random, and in parallel.
    async.each(voters, (peer, callback) => {
      // no need to gossip if winner already picked or work session expired
      if(winner || session.isExpired()) {
        return callback();
      }
      // 2. Gossip with a single voting peer.
      _gossipWith(ledgerNode, voter, blockId, peer, err => {
        // TODO: ignore communication error from a single voter...
        // 3. Tally votes to check for consensus on the current block.
        _tally(blockId, voters, (err, result) => {
          if(!err && result) {
            winner = result;
          }
          callback(err);
        });
      });
    }, err => {
      if(err || !isVoting) {
        return loop(err);
      }
      // 4. Now that all other voters have been contacted, if  not done
      //   already, and we are voting, vote.
      _vote(ledgerNode, voter, blockId, loop);
    });
  }, callback);
}

/**
 * Determines if the given voter is in the passed voting population.
 *
 * @param voter the voter to check for.
 * @param voters the voting population.
 *
 * @return true if the voter is in the voting population, false if not.
 */
function _isVoting(voter, voters) {
  return voters.some(v => v.id === voter.id);
}

/**
 * Causes the given ledger node, identified by the given `voter` information,
 * to gossip with a `peer` about the next block identified by `blockId`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockId the ID of the next block (the one to gossip about).
 * @param peer the peer to gossip with.
 * @param callback(err) called once the operation completes.
 */
function _gossipWith(ledgerNode, voter, blockId, peer, callback) {
  if(voter.id === peer.id) {
    // no need to gossip with self
    return callback();
  }

  // 1. Get block status from peer.
  _getBlockStatus(blockId, peer, (err, status) => {
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

    // 2.b. Certify votes if in 'election' phase.
    if(status.phase === 'election') {
      return async.each(status.votes, (vote, callback) => {
        _verifyVote(vote, err => {
          if(err) {
            // ignore failed votes, do not store them
            return callback();
          }
          // TODO: store vote
        });
      }, callback);
    }
  });
}

/**
 * Tallies the vote for the passed block ID. If there is a winning manifest
 * for the block, it will be returned, otherwise `null` will be returned.
 *
 * @param blockId the ID of the block to tally the vote for.
 * @param voters the voting population for the block ID.
 * @param callback(err, manifest) called once the operation completes.
 */
function _tally(blockId, voters, callback) {
  /* Go through rounds and see if 2/3rds have voted yet in the round. If not,
    then finish the tally because we need to wait for more votes to arrive. If
    so, and a 2/3rds majority has voted for a particular manifest, finish tally
    and return the winner. */
  let done = false;
  let round = 0;
  let twoThirds = Math.floor(voters.length / 3) * 2;
  let winner = null;
  async.until(() => done, callback => {
    api._storage.votes.tally(blockId, round++, (err, votes) => {
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
 * identified by `blockId`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockId the ID of the next block.
 * @param callback(err) called once the operation completes.
 */
function _vote(ledgerNode, voter, blockId, callback) {
  // TODO: get last vote from database

  // TODO: increment round and vote again

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

function _getBlockStatus(blockId, peer, callback) {
  // TODO: implement
  return callback(new Error('Not implemented'));

  // POST <endpoint>/status

  // Example 1 (simple query)
  // {
  //   block: 'block ID' // the next block
  // }

  // 2. B responds with its status resource for that block.

  // Example 1 (B has no such block yet):
  // {
  //   '@context': 'webledger/v1',
  //   type: 'Error',
  //   errorType: 'NotFound'
  // }

  // Example 2 (no consensus or vote yet):
  // {
  //   block: 'block ID',
  //   phase: 'gossip',
  //   gossip: [<eventHash1>, <eventHash2>, ...]
  // }

  // Example 3 (no consensus, round 1 through N vote):
  // {
  //   block: 'block ID',
  //   phase: 'election',
  //   electionResults: [{
  //     manifestHash: <manifestHash1>,
  //     round: 1,
  //     voter: <voter_id1>,
  //     signature: ...
  //   }, {
  //     manifestHash: <manifestHash2>,
  //     round: 1,
  //     voter: <voter_id2>,
  //     signature: ...
  //   }]
  // }

  // TODO: how does returning `consensus` as a phase help? Do we only need
  // the election phase -- after which the node should automatically detect
  // and verify consensus and avoid talking to other nodes?

  // Example 4 (consensus)
  // {
  //   block: 'block ID',
  //   phase: 'consensus',
  //   electionResults: [{
  //     manifestHash: <manifestHash1>,
  //     round: 4,
  //     voter: <voter_id1>,
  //     signature: ...
  //   }, {
  //     manifestHash: <manifestHash2>,
  //     round: 4,
  //     voter: <voter_id2>,
  //     signature: ...
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
  callback(new Error('Not implemented'));
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

  callback(new Error('Not implemented'));
}
