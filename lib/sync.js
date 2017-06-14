/*
 * Web Ledger Simple Sync Protocol.
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

// TODO: run every X interval
function runSyncJob(job, callback) {
  // start a consensus session for ledgers
  const maxTime = bedrock.config['ledger-continuity'].sessions.gossip.maxTime;
  const session = consensus.createSession('sync', Date.now() + maxTime);
  consensus.process('Continuity2017', session, sync, callback);
}

function sync(ledgerId, session, callback) {
  // TODO: do what needs doing during this sync session

  // TODO: .................difference from gossip.........................
  // TODO: `sync` is different from gossip; during sync, a requesting peer
  // asks a receiving peer for the next block -- this block should contain
  // all of the necessary authorization information that can be checked
  // TODO: as part of this module, an endpoint should be exposed that can
  // provide "the next block" following another one but this isn't part of
  // gossip
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
