/*
 * Web Ledger Continuity2017 Gossip Protocol.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const consensus = brLedger.consensus;

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._server = require('./server');
api._storage = require('./storage');
api._voters = require('./voters');

api._test = (callback) => {
  const session = brLedger.consensus.createSession('gossip', 1000);
  brLedger.consensus.execute('Continuity2017', session, _tmp, err => {
    console.log('consensus job done', err);
    callback(err);
  });
};

function _tmp(ledgerNode, session, callback) {
  console.log('consensus job running');

  async.auto({
    voter: callback => api._voters.get(ledgerNode.id, callback),
    nextBlockId: callback => _getNextBlockId(ledgerNode, callback),
    voters: ['nextBlock', (results, callback) =>
      api._voters.get(ledgerNode.id, results.nextBlockId, callback)],
    gossip: ['voters', (results, callback) => _gatherConsensus(
      session, ledgerNode, results.voter, results.blockId,
      results.voters, callback)],
    writeBlock: ['gossip', (results, callback) => {
      // TODO: should this be done here? what about work session timeouts?
      // TODO: maybe a separate worker should be used to write blocks vs.
      // TODO: this gossip one?
      // TODO: ... since consensus decision cannot be changed perhaps fine
      //   to do block writing here even if work session has expired
    }]
  }, (err, results) => {
    // TODO: implement
  });

  // TODO: get continuity population

  // TODO: create event gossip

  // TODO: begin transmitting to continuity population to get consensus
  // on what to do next

  callback();
}

// gather consensus for a single block
function _gatherConsensus(
  session, ledgerNode, voter, blockId, voters, callback) {
  // TODO: add timeout constraints (through out?) to ensure we still
  // have lock on the ledger node

  // TODO: need to keep track of "phase"/"stage" somewhere... in "voter"
  // database? some info can be built from querying the blockchain itself,
  // but not stuff that is being used to make present decisions
  // TODO: so maybe voter has: nextBlockId and phase in it?

  // determine if ledger node is voting in this election
  const isVoting = _isVoting(voter, voters);

  let done = false;
  async.until(() => done, loop => {
    // 1. Contact all voters once, at random, and in parallel.

    // TODO: consider using `eachLimit` instead and a configurable value
    // if any issues arise here
    async.each(voters, (peer, callback) => {
      if(done) {
        return loop();
      }
      async.auto({
        // 2. Gossip with a single voter.
        // TODO: ignore communication error from a single voter...
        gossip: callback => _gossipWith(
          ledgerNode, voter, blockId, peer, callback),
        // 3. Tally votes to check for consensus on the current block.
        tally: ['gossip', (results, callback) =>
          _tally(blockId, voters, (err, winner) => {
            if(err) {
              return callback(err);
            }
            if(winner) {
              done = true;
              return loop();
            }
            callback();
          })],
        // 4. Quit if work session has ended.
        timeout: ['tally', (results, callback) => {
          if(session.isExpired()) {
            done = true;
            return loop();
          }
          callback();
        }]
      }, err => {
        if(err) {
          return callback(err);
        }
        if(isVoting) {
          // 6. If round is 0:
          //   6.1. Create a manifest out of all events that have not yet reached
          //     consensus. (Note: there may be other caveats
          //     here, like events aren't deleted and such)
          //   6.2. Increment round and sign the manifest as a vote for round 1 and
          //     add it to the vote collection for the ledger for the block.
          // 7. If round > 1:
          //   7.1. Execute instant runoff vote algorithm to determine
          //     next manifest to vote for. Increment round, sign vote and store it.
        }
        callback();
      });
    }, callback);
  });

  // TODO: if not a decider...
  // 1. make a copy of the decider list for this block
  // 2. start a loop that will run until the copy of the list is empty
  // 3. randomly pick and remove a decider from the list
  // 4. contact that decider and run the gossip protocol
  // 5. check for consensus on the current block
  //   Go through rounds and see if a 2/3rds majority won the round for the
  //   block. Must be 2/3rds votes for *something* per round before moving to
  //   the next round.
  //   If a 2/3rds majority is found, quit loop early, we have consensus
  // 6. Loop until work session expires.

  // TODO: if a decider...
  // 0. Set round to 0.
  // 1-5. Same as "not a decider"
  // 6. If list is empty and round is 0:
  //   6.1. Create a manifest out of all events that have not yet reached
  //     consensus. (Note: there may be other caveats
  //     here, like events aren't deleted and such)
  //   6.2. Increment round and sign the manifest as a vote for round 1 and
  //     add it to the vote collection for the ledger for the block.
  // 7. If list is empty and round > 1:
  //   7.1. Execute instant runoff vote algorithm to determine
  //     next manifest to vote for. Increment round, sign vote and store it.
  // 8. Loop until work session expires.

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

function _isVoting(voter, voters) {
  return voters.some(v => v.id === voter.id);
}

function _gossipWith(ledgerNode, voter, blockId, peer, callback) {
  if(voter.id === peer.id) {
    // no need to gossip with self
    return callback();
  }

  // TODO: implement
  officialGossip(peer, callback);
}

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

function officialGossip(peer, callback) {
  // NOTE: 2017-07-04 - This replaces previous gossip protocol work below.

  // GOSSIP protocol between A and B:

  // TODO: This consensus plugin will need its own storage. It will need to
  // store:
  //
  // * manifests (a hash that maps to a list of event hashes)
  // * votes (indexed by manifest, round, and block)
  //
  // A new manifest will be created whenever a node is a decider and it has
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

  // TODO: Implement gossip protocol steps:

  /* Gossip protocol:

  1. A contacts B and requests all of its hashes for the block its on.

  POST <endpoint>/status

  Example 1 (simple query)
  {
    block: 'block ID' // the next block
  }

  2. B responds with its status resource for that block.

  Example 1 (B has no such block yet):
  {
    '@context': 'webledger/v1',
    type: 'Error',
    errorType: 'NotFound'
  }

  Example 2 (no consensus or vote yet):
  {
    block: 'block ID',
    stage: 'gossiping',
    gossip: [<eventHash1>, <eventHash2>, ...]
  }

  Example 3 (no consensus, round 1 through N vote):
  {
    block: 'block ID',
    stage: 'voting',
    votes: [{
      manifestHash: <manifestHash1>,
      round: 1,
      voter: <voter_id1>,
      signature: ...
    }, {
      manifestHash: <manifestHash2>,
      round: 1,
      voter: <voter_id2>,
      signature: ...
    }]
  }

  Example 4 (consensus)
  {
    block: 'block ID',
    stage: 'consensus',
    votes: [{
      manifestHash: <manifestHash1>,
      round: 4,
      voter: <voter_id1>,
      signature: ...
    }, {
      manifestHash: <manifestHash2>,
      round: 4,
      voter: <voter_id2>,
      signature: ...
    }]
  }

  // TODO: Note: If there is more than one vote for a particular voter ID,
  //   then the first vote received will be used. Alternatively, the vote
  //   for the manifest with the lowest hash could be used, but this would
  //   require updating the vote when a lower hash vote arrived -- which is
  //   is likely more problematic than simply accepting the first vote.

  3.a. If response indicates B is 'gossiping', send events:

  // TODO: would be nice to reuse ledger-agent/events API for this

  POST <endpoint>/events

  {
    events: [{... all events from A or other peers A has spoken with
      that B doesn't know ...}]
  }

  In parallel, for every event A does not know that B does:

  GET <endpoint>/events/<eventHash>

  { full event }

  3.b. If response indicates B is 'voting', then "confirm" every vote:

  // 3.b.1. Verify signature on vote.

  // 3.b.2. Fetch manifest to get event hashes for it.

  // TODO: would be more efficient to get a list of all event hashes across
  // manifests, but that does not check manifest hashes

  GET <endpoint>/manifests/<manifestHash>

  {
    gossip: [<eventHash1>, <eventHash2>, ...]
  }

  // 3.b.3. For each unknown <eventHash>, fetch full event:

  GET <endpoint>/events/<eventHash>

  { full event }

  // 3.b.4. Vote confirmed, store it indexed by its round. If we have a
  // 2/3rds majority for the current round, then we have consensus.

  3.c. If response indicates B has reached 'consensus', then "confirm"
    every vote (as per 3.b.).
*/
}

// TODO: register job that will run `runGossipJob` on interval

// TODO: expose gossip job function for testing

// TODO: run every X interval
function runGossipJob(job, callback) {
  // start a consensus session for ledgers
  const maxTime = bedrock.config['ledger-continuity'].sessions.gossip.maxTime;
  const session = consensus.createSession('gossip', Date.now() + maxTime);
  consensus.process('Continuity2017', session, inspectLedger, callback);

  // TODO: add `async.auto` and reschedule gossip job or will job scheduler
  // handle this?
}

function inspectLedger(ledgerId, session, callback) {
  // TODO: ensure certain indexes exist for the ledger?

  // TODO: see if its time to gossip about events for this ledger

  // TODO: get latest time window from storage for where the node is
  //   regarding sync
  // TODO: start gossiping with nodes for that time window, including all
  //   events that are known for that time window (even ones this node
  //   generated)
  // TODO: where/when does this node's address/signature/whatever get into
  //   events that it generated?
  // TODO: when communicating gossip ... is there a signature for every
  //   node on every event? or is there is a signature on a collection of
  //   events (on gossip) from every node? ... when a node receives two
  //   sets of gossip from two different nodes and there is a conflict,
  //   which set does the node report to others? or do they just report
  //   what others said? (if we have a signature on every event, then it's
  //   just a matter of mirroring those events everywhere and you don't have
  //   to trust .... hmm, sin of omission problem where peer X says
  //   here is everything from peer Y, but something may be left out, to
  //   avoid this it would have to be a signed manifest from Y ... signed
  //   manifest of all event hashes that Y knows about for that time window;
  //   so when gossiping we need to collect manifests from every node
  //   which is `[hash1,hash2,hash3] + signature` for the whole time window.
  // Two types of faulty nodes:
  //
  // 1. crashed
  // 2. byzantine
  //
  // Crashed nodes fail to terminate. There is no way for other nodes to
  // determine the difference between a crashed node and a slow one.
  // Need [(n+1)/2] nodes to agree to thwart the crashed node problem.
  //
  // Byzantine nodes can fail for any arbitrary reason including crashing
  // being poorly implemented or because they are malicious and send bad
  // or misleading messages.
  // Need [(2n+1)/3] nodes to agree to thwart the byzantine node problem.
  //
  // Restrictions: Correct nodes must select a valid result. No two correct
  // nodes can decide differently. All correct nodes must terminate.
  //
  //   .... could do a vote from the "continuity" population... so:
  //
  //   1. everyone picks the same continuity population via an algorithm
  //   2. everyone gossips with the continuity population at random
  //   3. gossip continues with each member of the continuity population
  //      until they say they're done by giving you a proposed manifest,
  //      which you record as getting one vote
  //   4. once you've gotten a vote from every member, the top vote
  //      getter wins; if there's a tie, the lowest hash wins.
  //
  //   Note: votes are digitally signed and can be virally spread, so
  //     when you talk to member 37, they may give you the votes from
  //     members 12, 51, and 64, etc.
  //
  // As a member of the continuity pop:
  //
  // 1. When do you decide to lock in your view of the world?
  // 2. When/how do you decide to adopt a different view of the world?
  //
  // As a non-member:
  //
  // 2. When/how do you decide to adopt a different view of the world?
  //
  //   Can assume that the nodes that are communicating with one another will
  //   probably continue to be able to do so -- and probably can approximate
  //   a value to agree upon by using their view hashes as "instant runoff"
  //   votes. So, each member of the continuity population must take the first
  //   67 locked-in view messages in "round 1" (proliferate others but ignore
  //   them for the purpose of the vote). See if 51 voted the same way; if so
  //   adopt their view and finalize it. Otherwise, if 34 members voted the
  //   same way, adopt their view as an estimate view and proceed to another
  //   around. If less than 34 have the same view, use an algorithm to calculate
  //   how every other member would vote if they had the same 67 messages;
  //   assuming a high likelihood of agreement, this should reduce rounds
  //   quickly. Then, on from that basis, select a new view to adopt for the
  //   next round and send that out. Rinse and repeat until there is consensus.
  //   Note: 33 is max faulty nodes
  //   Note: This is based on Ben-Or algorithm
  //
  //  If you are a member of 100:
  //
  //  1. Talk to other members until you've heard from 67 one way or another.
  //  2. Lock in your view. (Note: you must be able to share your view or
  //     others will ignore it and treat you like a faulty node).
  //  3. Start collecting locked in views from other members and provide all
  //     collected locked in views to members who communicate with you as your
  //     round 1 "estimate".
  //  4. Once you've collected 67 locked-in round 1 (n) estimates, see if 51
  //     are for the same view. If so, adopt that view and finalize it,
  //     consensus reached.
  //  5. If instead only 34 or more have the same view, adopt their view
  //     and start round 2 (n+1).
  //  6. If less than 34 have the same view, then use an "instant runoff"
  //     algorithm to determine which view to adopt based on the 67 views.
  //     Everyone with the same 67 messages (or similar) should adopt the
  //     same view. Choose that as your view and start round 2 (n+1).
  //  7. Go to 4 ... forever.
  //

  // OLD BELOW

  // TODO: rules indicate how long time windows are which indicates where the
  //   boundaries are for blocks

  // TODO: gossip should include previous block hash/ID too so it's clear
  //   when events would occur? if previous block hash is required ... and
  //   it takes asking 51% of the network (or verifying their signatures)
  //   to see what the previous block hash was, how will a slow node ever
  //   catch up and be able to spread its events?
  // TODO: is there no real purpose to blocks when using this protocol...
  //   so no separate sync process, just a lot of gossip? So you just ask
  //   for events within a certain time window until you're caught up to
  //   the current time window? (which is really what blocks are, just time
  //   windows?) ... so maybe always gossip events and include timestamps
  //
  // TODO: nodes must specify their address in an event signature ...
  //   perhaps not every event, but at least one that makes it into the ledger
  //   ... and whenever they change addresses -- in order to create "continuity"
  //   for that node and enable it to have voting rights

  // gossip with nodes, starting with oldest first
  let blockIterator;
  let done = false;
  async.until(() => done, loopCallback => {
    async.auto({
      iterator: callback => {
        if(blockIterator) {
          return callback();
        }
        storage.blocks.getIterator({
          direction: 'forward'
        }, callback);
      },
      peers: ['iterator', (results, callback) => {
        // get peers from the next block
        const item = blockIterator.next();
        if(item.done) {
          done = true;
          return loopCallback();
        } else {
          item.value.then(block => {
            const peers = block.event.map(event => event.eventSource);
            callback(null, peers);
          }, callback);
        }
      }],
      gossip: ['getPeers', (results, callback) => {
        // TODO: callback should return whether or not more gossip is necessary
        // or it should be calculated in another item in this async.auto
        // after `gossip` so that the `done` flag can be set....
        inspectPeers(ledgerId, session, results.peers, callback);
      }],
      checkGossipLimit: ['gossip', (results, callback) => {
        // TODO: set `done` if enough gossip has occurred for every passed
        // time window (Note: this needs to be coordinated with `sync` so
        // that gossip doesn't happen when the ledger isn't synced yet...
        // which is a flag that will be set in the database somewhere?)
        // TODO: determine rules for when a sufficient number/kind/percentage
        // of nodes have been contacted
        callback();
      }]
    });
  }, err => callback(err));
}

function inspectPeers(ledgerId, session, peers, callback) {
  // TODO: find events that haven't been fully gossiped yet for the
  // current time window via database query (what's the shape of that query?)

  // TODO: loop through peers...

  // TODO: build list of all events to be gossiped
  //   event gossip meta data indicates who has been informed of the
  //   event and who has been informed of the signatures, as far as this
  //   node knows
  // TODO: the list of events to be gossiped to a particular peer changes
  //   based on that peer -- as we may already have information about what
  //   that peer currently knows from other peers who have gossiped to us;
  //   BUT we need to include entire gossip manifests of what others know
  //   because they are signed... unless just a hash can be sent initially
  // TODO: when a new event enters storage (through a non-gossip mechanism),
  //  it has no gossip meta because it hasn't been shared yet...
  //  if it is entered through this gossip handler, then it will already
  //  include all parties that have signed the event, which includes the
  //  one that just communicated it
  //    NOTE: we must also sign the event if it passes some tests (what tests?)
  //    so our own signature can be included
}

function gossip(peer, callback) {
  // GOSSIP protocol between A and B:

  // TODO: Here are the contact node and request all of its hashes




  // TODO: build a list of all personal events to be gossiped

  // TODO: is it necessary to communicate individual manifests or
  // just events? -- now given the above continuity population-voting-based
  // algorithm/protocol?

  // TODO: create a gossip manifest of all personal events
  // {
  //   gossip: [event_hash1, event_hash2, ...],
  //   gossipWindowStart: "123",
  //   gossipWindowEnd: "456"
  //   signature: ...
  // }
  // hash the manifest
  //
  // send to B all known gossip manifest hashes in sorted order
  // {
  //   manifestHash: [manifest_hash1, manifest_hash2, ...],
  // }
  //
  // B computes and replies with all gossip manifests known to B that A
  //   doesn't know
  //
  // {
  //   manifest: [{
  //     gossip: [event_hash1, event_hash2],
  //     gossipWindowStart: "123",
  //     gossipWindowEnd: "456"
  //   }, {manifest2}, ...]
  // }
  //
  // A computes and replies with all manifests B does not know and all
  //   events that B does not know
  //
  // {
  //   manifest: [{
  //     gossip: [event_hash1, event_hash2],
  //     gossipWindowStart: "123",
  //     gossipWindowEnd: "456"
  //   }, {manifest2}, ...],
  //   event: [{event1}, {event2}, ...]
  // }
  //
  // B computes and replies with all events A does not know
  //
  // {
  //   event: [{event1}, {event2}, ...]
  // }



  // TODO: old gossip data format below ... doesn't cover gossip manifests
  // properly, so don't use it other than for reference purposes

  // TODO: A builds a list of all events to be gossiped
  //   the list needs to have:
  //     event hash + all signature hashes
  //   e.g. {
  //    gossip: [{eventHash: 'abcd', signatureHash: ['123', '1234']}, ...]
  //    gossipWindowStart: "123",
  //    gossipWindowEnd: "456",
  //    signature: /* signature on gossip manifest so this can be shared
  //      with other peers */
  //   }
  // TODO: also communicate everything everyone else knows? perhaps just
  //   a hash of each gossip manifest so we only have to send the manifest if
  //   requested?
  // TODO: so maybe what is sent is just a hash of our own manifest, because
  //   maybe they already have it ... we just send all the gossip manifest
  //   hashes we have -- and then B responds with all the gossip manifest
  //   details it needs and all the additional manifest hashes and details it
  //   has; we send manifests with event hashes, it sends back all the events
  //   it needs... DONE
  //
  //
  //
  // TODO: B replies with an index of all the events and
  //   signature hashes it does not have
  //        (missing event) (has event) (missing hash 0, has 1, missing 2, has rest)
  //   e.g. [0            , 1         , [0, 1, 0]  ]
  //   and its own gossip (already filtered by what it knows A doesn't know),
  //   so in total:
  //   {
  //     delta: [...],
  //     gossip: [...]
  //   }
  //
  // TODO: A responds with full events and signatures per `delta`
  //   {
  //     events: [...]
  //   }
  //
  //  And communication is complete.
}


// TODO: gossip protocol is responsible for filling up the database with
// events and blocks that everyone else is talking about ... and indicating
// in the meta who authored them and so on (where you got them from and any
// other gossippy meta data).... THEN ... a consensus algorithm is responsible
// for analyzing existing events and blocks to determine which ones are
// canonical and which ones aren't
// TODO: TBD - the consensus algorithm may itself add new blocks based on
// events that don't yet have blocks associated with them ... and these then
// need to get gossipped about
// TODO: TBD - the consensus algorithm could also aggressively cull events
// and blocks that are floating around that are known not to belong?
// TODO: also needs to be a separate "validate" process that is built into
// bedrock-ledger that runs over the entire ledger, from time to time, and
// checks everything? how would that work? would need to write some validating
// flag that causes everything else to halt...????
// TODO: ***gossip protocols abide by clear boundaries under which they are
// permitted to run ... i.e. configBlocks that specify/do not specify the
// consensus algorithm to which they are bound; that means that when running
// periodically, if the latest config block for a ledger has a consensus
// algorithm that doesn't match, do not run. In order to enforce this on
// the otherside, where a gossipper may be running and could "bleed into"
// or "past" a configBlock, we either need the design to require gossip
// protocols to run in discrete units where configBlock boundaries always
// coincide with the edges of those units, or we'll have to have extra checks
// to make sure we're not operating where we shouldn't be.
// TODO: that means the gossip story works like this:
// 1. wake up
// 2. see if the latest config block says we should gossip; if no, sleep again
// 3. yes, so gossip about everything during a "window"... where the window
//    ends at (some constraint like a time) OR at a config block, where it
//    must always end
// Therefore, gossipping will always takes place in a discrete unit that will
// be bookended with config blocks or some other constraint (whichever comes
// first)
// TODO: Note: there's no reason you can't just keep gossipping until you
// hit a configBlock, right? And if that configBlock doesn't change the
// gossip algorithm, keep on going? PROBLEM: we need to get consensus on
// the config block and we don't control that in the gossip protocol! how
// is this resolved? how do we really know when to stop? Do we say that
// we can't gossip any more until the configBlock is accepted/rejected?
// can that result in some horrible deadlock? (no, the consensus algorithm must
// prevent that)
// TODO: we can't make it such that if a node proposes a new config block
// that everything halts ... DoS ... but we also can't be gossipping about
// events beyond the config block because those aren't under our aegis!
// TODO: perhaps the gossip messages will inform us of this? if we're really
// behind and need to resync then other modules will reject our request to
// gossip using the gossip protocol we're using (note: we'll need to make sure
// the other side knows that info so they can reject us) ... but what if we're
// caught up and it's happening live? i guess we just keep gossipping and
// end up throwing out some events because they'll be tagged as having been
// received via a gossip protocol that is no longer in control?
// TODO: ***Things we know: gossip messages must say gossip version/protocol
// and events that are written must include the gossip protocol/version used
// to receive them as well


// TODO: scan ledger storage for ungossiped events
// TODO: use a list of peers to gossip with that can change asynchronously
// (i.e. read it from the database?)
// TODO: perhaps what the main loop looks for is events that haven't been
// gossipped to *everyone* yet that's what "ungossipped" means... where
// "everyone" can change over time so old events will continue to get
// gossipped to new nodes as they join... is this necessary? It's not necessary
// if the only way "new nodes" can exist is if they write to the ledger...
// because writing to the ledger requires that the new node already has
// pulled in all other existing events on its own, so there's no need to
// gossip with that node unless it asks for gossip itself; ...can gossip
// responsibility be broken down to that simple concept? you only tell others
// when you hear about new events? When do you need to ask others for
// information? is that a different operation... you ask for blocks as part
// of a "sync" op, rather than a "cloud event" gossip one? The gossip part
// is talking about "what may have happened" and the sync part is talking about
// what the consensus already is on what actually happened?

// TODO: can the "sync" bits go in their own "sync.js" file?

// TODO: connect to peer and send hashes of events and signatures from
// everyone who has signed off on them

// TODO: peer may respond asking for event details (if peer doesn't have
// them for those particular events -- will respond with a set of hashes
// that it needs events for)

// TODO: send peer all event details it asked for

// TODO: keep gossipping forever; marking events with appropriate flags so
// they won't be picked up to be processed again by the gossip engine, but
// so they can be picked up by, for example, a consensus engine that will
// periodically examine what everyone said and decide what really happened

// TODO: setting these flags may involve "closing" the time window for
// gossip by informing other peers (and getting their sign off) that the
// time for gossip on events in list X has ended ... and get everyone's
// agreement -- where "everyone" is based on the "continuity" protocol
// configuration ... if we can break this part out of the gossip protocol
// or abstract it, that would be ideal... as this piece could change based
// on different consensus models, but the rest of the gossip protocol could
// remain

// TODO: tricky dealing with edges around config changes ... gossip/sync
// protocol needs to support pulling in blocks from before the config
// changed ... i suppose the background processes will automatically pick
// up on blocks that are still under the aegis of an older method...
// what to do about changing gossip protocols and are they really part of
// consensus or not?
