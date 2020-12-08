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
    if(witnesses.length % 3 === 1) {
      // `2f` is the default empty event witness threshold
      const f = (witnesses.length - 1) / 3;
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
    basisBlockHeight: status.basisBlockHeight,
    mergeHeight: status.mergeHeight,
    parentHash,
    treeHash
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
      // this will always be the same as `generation` for local merge events
      localAncestorGeneration: generation,
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
  status.treeHash = null;
  status.parentHash = null;
  status.basisBlockHeight = basisBlockHeight;
  status.mergeHeight = 0;
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
  reuse. Note that this can be partially implemented here by making use of
  "childless" events.

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

  Note: Some witness merge events that could be used to meet thresholds may
  not be childless. This requires us to do an additional check to see if
  we have previously merged them directly as parents to avoid eliminating
  valid options for parents. */

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

  // FIXME: determine if threshold should only be applied to non-witnesses

  // determine which threshold to use based on whether or not there's
  // anything local to merge
  const threshold = localOutstanding ? nonEmptyThreshold : emptyThreshold;

  /* Note: We could find a peer head with a `mergeHeight` that is after our
  head's but yet does not descend from it. This would be possible if enough
  merges happend in another part of the DAG that we have yet to merge. We need
  to be able to merge that peer head or consensus could fail. But this means
  we have to check its ancestry to ensure it doesn't descend from our head. We
  could walk back from each of the peer head's parents until we find our head
  or until hit our `mergeHeight` (at which point we know the peer head does
  not descend from our head), but this would be very slow if the peer had a
  much higher `mergeHeight` than our head.

  Instead, we can leverage the fact that we know that *we* are not going to
  fork, i.e., we assume we are not a byzantine node and have measures in place
  to prevent forks, aka if we fork we're byzantine anyway, all is lost. So,
  given that assumption, that means that for any (local) "generation" number,
  there will only ever be one merge event (whereas forking peers may have
  more than one). We can store this local generation of the last ancestor
  that was merged into a peer event as meta data as `localAncestorGeneration`.
  This would only add a single number to the meta data for every peer event and
  we can use that information to do a simple check to determine if the peer
  event descends from our head or not (peerHead.localAncestorGeneration <
  ourHead.generation). */

  // first, filter witness heads into those that could count toward the
  // threshold; merge events count if:
  // 1. The merge event is not our own merge head.
  // 2. The witness head's `localAncestorGeneration < head.generation`.
  // 3. There's something local to merge or the head's `basisBlockHeight` is
  //   not beyond our current `basisBlockHeight`.
  const {heads: witnessHeads} = await _history.getHeads(
    {ledgerNode, creatorIds: witnesses});
  // get head to determine `treeHash`, merge generation, and merge height;
  // we may have already retrieved our own head if we're a witness, otherwise
  // call `getHead` to fetch it
  const head = witnessHeads.get(creatorId) ||
    await _history.getHead({creatorId, ledgerNode});
  let {mergeHeight} = head;
  let witnessHashes = [];
  for(const peerHead of witnessHeads.values()) {
    if(peerHead !== head &&
      peerHead.localAncestorGeneration < head.generation &&
      (localOutstanding || peerHead.basisBlockHeight <= basisBlockHeight)) {
      witnessHashes.push(peerHead.eventHash);
      // FIXME: this needs to be refactored so the `mergeHeight` only
      // increases if this witnessHash ends up getting used
      mergeHeight = Math.max(mergeHeight, peerHead.mergeHeight);
    }
  }
  // get unique witness hashes to avoid duplicate genesis heads
  witnessHashes = [...new Set(witnessHashes)];

  // next, determine if any of the witness hashes are "peerChildlessHashes",
  // which means we know we do not descend from them; see if we meet the
  // threshold by using only these
  const peerChildlessHashesSet = new Set(peerChildlessHashes);
  const childlessWitnessHashes = [];
  const nonChildlessWitnessHashes = [];
  for(const hash of witnessHashes) {
    if(peerChildlessHashesSet.has(hash)) {
      childlessWitnessHashes.push(hash);
    } else {
      nonChildlessWitnessHashes.push(hash);
    }
  }

  // determine if there are sufficient witness hashes to use as parents
  let parentHashes = [];
  let thresholdMet = false;
  if(childlessWitnessHashes.length >= threshold) {
    parentHashes.push(...childlessWitnessHashes.slice(0, threshold));
    thresholdMet = true;
  } else {
    // next, check if any of the viable witness hashes with children can be
    // merged without violating protocol by ensuring none of them have been
    // previously merged as direct parents by another merge event created by
    // `creatorId`
    // FIXME: we may want to indicate whether or not a head has been previously
    // merged in the cache so we don't have to hit the database to check here;
    // we may always be hitting the database for at least one head (unclear how
    // much benefit cache would be here yet)
    ({diff: witnessHashes} = await _history.diffNonParentHeads(
      {ledgerNode, creatorId, eventHashes: nonChildlessWitnessHashes}));
    witnessHashes = childlessWitnessHashes.concat(witnessHashes);
    if(witnessHashes.length >= threshold) {
      parentHashes.push(...witnessHashes.slice(0, threshold));
      thresholdMet = true;
    }
  }
  // FIXME: uncomment once bugs w/merging non-childless parents have been
  // fixed
  // FIXME: need to implement `mergeHeight` to prevent creating cycles
  parentHashes = [];

  if(!thresholdMet) {
    // witness event threshold not met, cannot merge
    // FIXME: uncomment to enforce threshold
    //return {mergeable: false, ...status};
  }

  // Note: At this point, we have determined that we will be merging. We do
  // not need more gossip to be able to merge.
  status.needsGossip = false;

  // FIXME: note: even if an "empty merge event" is created by a witness,
  // it must be able to add non-witness parents in addition to the `2f`
  // witness parents to ensure that a network where all of the operations
  // are flowing through non-witnesses that consensus will be reached

  // FIXME: remove this once the above is implemented to include the
  // appropriate peer childless hashes, but for now add some to keep
  // the implementation testable
  parentHashes.push(...peerChildlessHashes);
  // FIXME: we need to ensure we don't duplicate parent hashes when adding
  // from `peerChildlessHashes` (could be overlap from `witnessesHashes`)
  // ...this should be more carefully done so this FIXME can be removed
  parentHashes = [...new Set(parentHashes)];

  // add head as tree hash and the first parent hash and set merge generation
  status.treeHash = head.eventHash;
  status.generation = head.generation + 1;
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

  // FIXME: fill any remaining slots in `parentHashes`:
  // 1. add in a non-witness childless parent
  // 2. if `priorityPeer`:
  //   add in witness childless parents (`parent.basisBlockHeight` can be
  //   anything if localOutstanding, otherwise must be <= `basisBlockHeight`)
  //   add in non-witness childless parents
  // 3. else if `non-witness`:
  //   add in a witness parent (and change above threshold to not apply
  //   to non-witnesses?)
  // FIXME: we also need to update `mergeHeight` based on any additional
  // merge event parents, should always be max `mergeHeight` from all
  // merge event parents + 1

  status.parentHash = parentHashes;
  status.basisBlockHeight = basisBlockHeight;
  status.mergeHeight = mergeHeight + 1;
  return {mergeable: true, ...status};
}
