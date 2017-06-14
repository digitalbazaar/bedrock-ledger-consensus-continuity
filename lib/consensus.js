/*
 * Web Ledger Continuity Consensus Protocol.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
require('bedrock');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

// TODO: scan ledger storage for fully gossiped events in a time window
// i.e. once X time has passed, "close" the time window for gossip (ideally
// this happens through an abstraction that is set/passed to the gossip
// protocol and implemented here ... or it's done such that the gossip
// protocol bits don't even have to know about it at all -- allows better
// reuse of the gossip protocol and it can be moved to its own module)

// TODO: forever, query the database for the oldest time window that has been
// "closed" by a list of members (signed off by them) that is determined by the
// continuity protocol/configuration

// TODO: get all events in that time window

// TODO: run EventGuards to reject any bad events

// TODO: order all other events by hash

// TODO: wrap the events in a block (to communicate the event order) or
// just emit the event hashes in order? ... or write the order in their
// meta? how is the consensus communicated outside of this module? is this
// module responsible for writing to storage -- if so, how does it work
// in conjunction with a `sync` process? it seems like this module should
// focus on ordering events only ... but keep in mind that other consensus
// mechanisms may work at the block level...

// TODO: should the block be proposed to all peers (or N peers that have
// previously written to the ledger) to collect their votes? Can we know
// what their votes are without having to contact them based on the gossip
// we received from them + the time window or can skew mess that up? Maybe
// gossip happens *after* the time window has closed?

// TODO: so time window closes, then you begin gossiping. after you've
// gossiped with enough nodes that previously wrote to the ledger, you
// should have a set of events and know what those other nodes have too
// based on what you gossiped (is that true?). The only question that
// remains is how do you deal with conflicts? The data we have available to
// resolve conflicts is: lowest hash on block (easiest, just compute all
// possible conflicts and choose the lowest hash -- but are we sure everyone
// else is operating on the same data? (i.e. will a majority pick the same
// thing ... and if we're somehow in the minority can it be guaranteed that
// we're only there because we're a bad actor? if a good actor can wind up
// in the minority, how does it get back to being in the majority?); we
// also have a running list of the nodes that have previously written
// to the ledger and how long ago they wrote to it -- so this information
// can also be used to resolve conflicts. A chart of all possible states
// could be used to prove correctness here.

// TODO: should a separate storage/sync worker come through and deal with
// event consensus?

// TODO: whatever happens, a signal must be written to the database that will
// prevent the window from being brought up again for consideration (infinite
// loop)... whether that be flags on events or a block existing somewhere for
// that particular window or set of events

// TODO: update ... always write blocks when consensus is arrived at... even
// if IDs are duplicates (i.e. IDs are not unique for blocks), we will flip
// flags that indicate whether or not a block has achieved consensus
// TODO: when writing a new block with the same ID ... maybe we need to
// clear the consensus flags for entire chain starting with that block?
// TODO: would be nice for storage to have APIs to do this

// TODO: when working on a ledger node, add self as a worker to its DB
// entry so that garbage collectors know not to touch it until the worker
// expires OR add an API call to the storage API to mark something as in use
// for X period of time

// TODO:
// Could perform all auth on events
// Could model N txns as giant update event w/PoW
// Node doesn't use gossip (or still could), it just
//   takes its one giant event and puts it in a block
//   and proposes that block for entry into the ledger
// If block is accepted, its "work" value is total for
//   the events in it (typically one if mirroring bitcoin).
// When a duplicate block arrives, the consensus algorithm
//   must evaluate its total work value to determine if
//   consensus flags need to be switched
// This approach makes authorization just another event guard
//   that a system may or may not have.

// TODO:
// Note: Need to surface a "could work if you try again" error
//   for authorization event guards (temporary failure vs.
//   hard failure). What happens when using signature-based auth
//   and someone misconfigures their public key and half the
//   nodes retrieve it properly and verify a signature and the
//   other half do not. EventGuard must be executed prior to
//   gossiping so that the disagreement surfaces prior to
//   reconcilation (consensus).

// TODO: Downsides:
//   Increased overhead w/PoW per event for small events
