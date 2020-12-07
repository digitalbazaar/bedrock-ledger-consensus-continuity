/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _continuityConstants = require('../continuityConstants');
const _events = require('../events');
const _history = require('../history');
const _signature = require('../signature');
const _util = require('../util');
const {config} = require('bedrock');

const api = {};
module.exports = api;

api.merge = async ({
  ledgerNode, creatorId, priorityPeers = [],
  witnesses = [], basisBlockHeight, halt,
  nonEmptyThreshold = 1, emptyThreshold
}) => {
  if(emptyThreshold === undefined) {
    if(witnesses % 3 === 1) {
      // `2f` is the default empty event witness threshold
      const f = (witnesses - 1) / 3;
      emptyThreshold = 2 * f;
    } else {
      // cannot create an empty merge event with a single witness, so
      // set this to 1 which will prevent it from happening
      emptyThreshold = 1;
    }
  }

  const status = await _prepareNextMerge({
    ledgerNode, creatorId, priorityPeers, witnesses, basisBlockHeight, halt,
    nonEmptyThreshold, emptyThreshold
  });
  if(!status.mergeable) {
    // nothing to merge
    return {merged: false, halted: false, record: null, status};
  }

  if(halt && halt()) {
    // ran out of time to merge
    return {merged: false, halted: true, record: null, status};
  }

  /*
  // determine hashes of events to be used as parents for new merge event...
  const {mergeEvents: {maxEvents}} = _continuityConstants;
  const {peerChildlessHashes, localChildlessHashes} = mergeStatus;

  // all `localChildlessHashes` must be included in the merge event;
  // maximum regular event limits are enforced via `merge.js` where
  // `_events.create` is called
  const parentHashes = localChildlessHashes.slice();

  // fill remaining spots with `peerChildlessHashes`, leaving one spot
  // for `treeHash`
  const remaining = maxEvents - parentHashes.length - 1;
  parentHashes.push(...peerChildlessHashes.slice(0, remaining));

  // FIXME: need to ensure that any `parentHashes` weren't already merged...
  // this could potentially be solved by recomputing `childlessHashes` at the
  // start of every work session (this is to avert potential danger that a
  // merge happened but then the cache wasn't updated to remove childless
  // hashes for some reason -- or a regular local event didn't make it into
  // the cache and will not be merged appropriately violating protocol)

  // nothing to merge
  if(parentHashes.length === 0) {
    return null;
  }

  // set `truncated` to true if there are still more hashes to merge
  const total = peerChildlessHashes.length + localChildlessHashes.length;
  const truncated = total > parentHashes.length;

  // get local branch head to merge on top of and compute next generation
  const {eventHash: treeHash, generation} =
    await _history.getHead({creatorId, ledgerNode});
  const nextGeneration = generation + 1;*/

  const {generation, treeHash, parentHash} = status;

  // create, sign, and hash merge event
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash,
    treeHash,
    basisBlockHeight: status.basisBlockHeight
  };
  const ledgerNodeId = ledgerNode.id;
  const signed = await _signature.sign({event, ledgerNodeId});
  const eventHash = await _util.hasher(signed);

  // local merge events must be written directly to storage
  const meta = {
    consensus: false,
    continuity2017: {
      creator: creatorId,
      generation,
      type: 'm'
    },
    eventHash
  };
  const record = await ledgerNode.storage.events.add({event: signed, meta});

  // update cache
  await _cache.events.addLocalMergeEvent({...record, ledgerNodeId});

  return {merged: true, halted: false, record, status};
};

async function _prepareNextMerge({
  ledgerNode, creatorId, priorityPeers, witnesses, basisBlockHeight,
  halt, nonEmptyThreshold, emptyThreshold
}) {
  const ledgerNodeId = ledgerNode.id;
  const status = await _cache.events.getMergeStatus({ledgerNodeId});
  const {peerChildlessHashes, localChildlessHashes} = status;
  status.needsGossip = true;

  // report if any outstanding operations (local or peer);
  // Note: Consider configuration changes "outstanding operations" too.
  // check this in order of least costly function calls:
  // 1. If there are outstanding local regular events (memory)
  // 2. If there are outstanding local operations (cache)
  // 3. If there are outstanding peer regular events (database)
  let hasOutstandingLocalOperations;
  const {hasOutstandingRegularEvents} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  status.hasOutstandingOperations =
    // `localChildlessHashes` represent all local outstanding regular events
    localChildlessHashes.length > 0 ||
    // keep track of whether or not the cache has been hit for later
    (hasOutstandingLocalOperations =
      !await _cache.operations.isEmpty({ledgerNodeId})) ||
    await hasOutstandingRegularEvents();

  // if no outstanding operations of any sort, then there is nothing to merge
  if(!status.hasOutstandingOperations) {
    return {mergeable: false, ...status};
  }

  /* Note: Next the merge strategy rules will be applied to ensure that this
  node does not violate protocol (resulting in it being removed from the
  network; there may be a chance it can be surgically repaired to get back into
  good standing but this should be avoided).

  The rules work like this:

  1. First, no merge event may descend from any parent that another merge
  event in its local tree ancestry has ever descended from, i.e., no parent
  reuse. This is enforced in this implementation by only merging from events
  that are marked as "childless".

  // FIXME: only using "childless" events is going to be problematic; as
  // they get "used up" by other nodes that merge from them -- so a non-witness
  // may merge a witness's childless event in and then gossip with this node
  // and that witness's "childless" event will no longer be childless and
  // cannot be used; we'll need a fix for this implementation approach
  // FIXME: similarly, when witnesses are making empty merge events, a
  // non-priority peer may keep "using up" the witness childless events
  // such that the priority peers can't make progress, stifling consensus

  2. If there are local operations that need to be merged (which will result
  in the creation of a "non-empty" merge event), a merge event may only be
  created if the `nonEmptyThreshold` is met. To meet this threshold, this node
  must have `nonEmptyThreshold` witness events from which to make the new
  merge event directly descend.

  3. If there are no local operations, then only a witness may make a merge
  event (which is called an "empty" merge event). Additionally, this merge
  event must directly descend from at least `emptyThreshold` (which defaults
  to `2f`) other witness merge events that have a `basisBlockHeight` that is
  less than or equal to `basisBlockHeight`.

  // FIXME: this last check for `basisBlockHeight` will require obtaining
  // this information from the database ... or new cache changes; if we're
  // going to hit the database anyway, perhaps we can determine if we have
  // merged with any of these potential merge events already at the same
  // time, resolving some of the above FIXME issues

  // If operation ready:
  // ...If "this" node is a witness:
  // ...  require at least 1 witness parent (basisBlockHeight can be anything)
  // ...  fill remaining spots with own regular events
  // ...  add in a non-witness parent
  // ...  add in witness parents until full (basisBlockHeight can be anything)
  // ...  if there are remaining spots, add in non-witness parents
  // ...Else (not a witness):
  // ...  fill remaining spots with own regular events
  // ...  merge in a non-witness parent
  // ...  merge in a witness parent
  // Else if 5:
  // ...  create empty merge event from the selected `2f` parent merge events
  // Else: can't merge.
  */

  // determine if we have anything local to merge
  const localOutstanding =
    localChildlessHashes.length > 0 ||
    // `hasOutstandingLocalOperations` may be undefined (cache not hit yet
    // due to short-circuiting), true, or false
    hasOutstandingLocalOperations === true ||
    (hasOutstandingLocalOperations === undefined &&
      !await _cache.operations.isEmpty({ledgerNodeId}));

  // determine if node is a witness and a priority peer; priority peers
  // must be witnesses, and if `witnesses` are not given, assume all nodes
  // are witnesses and if `priorityPeers` are not given, assume all witnesses
  // are priority peers
  const isWitness = witnesses.length === 0 || witnesses.includes(creatorId);
  const isPriorityPeer = isWitness;/* && (priorityPeers.length === 0 ||
    priorityPeers.includes(creatorId));*/

  // if there's something local to merge or the node is a priority peer and
  // there are peer events to merge, then we can continue, if not, return
  // early
  // FIXME: ensure using priority peer here instead of `isWitness` cannot cause
  // consensus to fail
  if(!(localOutstanding ||
    (isPriorityPeer && peerChildlessHashes.length > 0))) {
    return {mergeable: false, ...status};
  }

  // filter witness heads into those that are childless to determine the
  // ones that can be safely merged (without risking merging the same event
  // as a parent multiple times, which is a protocol violation)
  const {heads: witnessHeads} = await _history.getHeads(
    {ledgerNode, creatorIds: witnesses});
  const peerChildlessHashesSet = new Set(peerChildlessHashes);
  const witnessHashes = [];
  for(const [creator, head] of witnessHeads.entries()) {
    // do not include self
    if(peerChildlessHashesSet.has(head.eventHash) && creator !== creatorId) {
      witnessHashes.push(head.eventHash);
    }
  }

  // FIXME: enable threshold code
  // determine if there are sufficient witness hashes to use as parents
  let thresholdMet = true;//false;
  let parentHashes = [];
  /*if(localOutstanding) {
    // peer has local outstanding operations, so use non-empty event threshold
    if(witnessHashes.length >= nonEmptyThreshold) {
      // FIXME: we could use more than this... perhaps if the witness is
      // a priority peer we should merge all that we can
      parentHashes.push(
        ...witnessHashes.slice(0, nonEmptyThreshold));
      thresholdMet = true;
    }
  } else {
    // Note: Due to above conditionals, at this point we know that the node
    // is a witness and a priority peer.
    if(witnessHashes.length >= emptyThreshold) {
      parentHashes.push(
        ...witnessHashes.slice(0, emptyThreshold));
      thresholdMet = true;
    }
  }*/

  if(!thresholdMet) {
    // witness event threshold not met, cannot merge
    return {mergeable: false, ...status};
  }

  // Note: At this point, we have determined that we will be merging. We do
  // not need more gossip to be able to merge.
  status.needsGossip = false;

  // FIXME: remove this once the above is implemented to include the
  // appropriate peer childless hashes, but for now add some to keep
  // the implementation testable
  parentHashes.push(...peerChildlessHashes);
  // FIXME: we need to ensure we don't duplicate parent hashes when adding
  // from `peerChildlessHashes`
  parentHashes = [...new Set(parentHashes)];

  // get head to determine `treeHash` and merge generation; we may have
  // already retrieved our own head if we're a witness, otherwise call
  // `getHead` to fetch it
  const head = witnessHeads.get(creatorId) ||
    await _history.getHead({creatorId, ledgerNode});

  // set merge generation
  status.generation = head.generation + 1;

  // add head as tree hash and the first parent hash
  status.treeHash = head.eventHash;
  parentHashes.unshift(status.treeHash);

  // add any already created local regular events
  parentHashes.push(...localChildlessHashes);

  /* Note: At this point, we have determined that we can merge and have
  computed the required parent hashes. So, compute how many additional
  operation events can be created and merged to flush out the operation
  queue. Create up to that many events and add them to the list of
  `parentHashes` so that they will be merged when passing the status to
  `merge`. If these new events are not merged due to an error or work session
  timeout, they will be included in `parentHashes` next time so there should
  be no corruption resulting in lost operation events or malformed merge
  events. */
  const {mergeEvents: {maxEvents}} = _continuityConstants;
  let operationEventSlots = maxEvents - parentHashes.length;
  while(operationEventSlots > 0 && (!halt || !halt())) {
    const {hasMore, eventHash} = await _events.create({ledgerNode});
    // update merge status if an event was created
    if(eventHash) {
      localChildlessHashes.push(eventHash);
      parentHashes.push(eventHash);
    }
    if(!hasMore) {
      break;
    }
    operationEventSlots--;
  }

  // FIXME: old below
/*
  // if there are no peer events to merge and there are `priorityPeers`
  // but `creatorId` is not one of them...
  if(peerChildlessHashes.length === 0 &&
    (priorityPeers.length > 0 && !priorityPeers.includes(creatorId))) {
    // if we don't have anything local that needs merging, do not merge
    const localOutstanding =
      localChildlessHashes.length > 0 ||
      // `hasOutstandingLocalOperations` may be undefined (cache not hit yet
      // due to short-circuiting), true, or false
      hasOutstandingLocalOperations === true ||
      (hasOutstandingLocalOperations === undefined &&
        !await _cache.operations.isEmpty({ledgerNodeId}));
    if(!localOutstanding) {
      return {mergeable: false, ...status};
    }

    // we have local events/ops that need merging, but we should only merge
    // if our previous merge event contained only other merge events (none
    // of our local regular (operation) events); otherwise, we must wait for
    // other priority peers to send us their merge events before merging
    // get the head merge event from the database to ensure that the consensus
    // status is up-to-date
    const [headRecord] = await getHead({creatorId});
    if(!headRecord || headRecord.meta.continuity2017.generation === 0) {
      // head is the genesis head which has no operation events, safe to merge
      return {mergeable: true, ...status};
    }

    // if the head has consensus then merge the outstanding local events
    if(headRecord.meta.consensus) {
      return {mergeable: true, ...status};
    }

    // get parents of head merge event
    // if already merged some regular events in, do not merge any more local
    // regular events in until we receive more priority peer merge events
    const [{event: headMergeEvent}] = await _events.getEvents({
      eventHash: headRecord.meta.eventHash,
      ledgerNode
    });
    const eventHash = headMergeEvent.parentHash.filter(
      h => h !== headMergeEvent.treeHash);
    const events = await _events.getEvents({eventHash, ledgerNode});
    if(events.some(({event: {type}}) => type === 'WebLedgerOperationEvent')) {
      return {mergeable: false, ...status};
    }
  }*/

  status.parentHash = [...parentHashes];
  status.basisBlockHeight = basisBlockHeight;
  return {mergeable: true, ...status};
}
