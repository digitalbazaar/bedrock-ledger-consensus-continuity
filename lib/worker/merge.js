/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _continuityConstants = require('../continuityConstants');
const _events = require('../events');
const _signature = require('../signature');
const _util = require('../util');
const {config} = require('bedrock');

const api = {};
module.exports = api;

api.merge = async ({
  // FIXME: change threshold to a single property and make it `witness`
  // threshold
  worker, priorityPeers = [], witnesses = [], basisBlockHeight,
  nonEmptyThreshold, emptyThreshold
}) => {
  if(emptyThreshold === undefined) {
    // `2f` is the default empty event witness threshold
    const f = Math.max(1, (witnesses.length - 1) / 3);
    emptyThreshold = 2 * f;
  }
  if(nonEmptyThreshold === undefined) {
    // default to the same threshold as empty event
    nonEmptyThreshold = emptyThreshold;
  }

  const status = await _prepareNextMerge({
    worker, priorityPeers, witnesses,
    basisBlockHeight, nonEmptyThreshold, emptyThreshold
  });
  if(!status.mergeable) {
    // nothing to merge
    return {merged: false, halted: false, record: null, status};
  }

  if(worker.halt()) {
    // ran out of time to merge
    return {merged: false, halted: true, record: null, status};
  }

  const {generation, treeHash, parentHash, isLocalContributor} = status;

  // create, sign, and hash merge event
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    basisBlockHeight: status.basisBlockHeight,
    mergeHeight: status.mergeHeight,
    parentHash,
    treeHash
  };
  const {head, localPeerId, ledgerNode} = worker;
  const ledgerNodeId = ledgerNode.id;
  const signed = await _signature.sign({event, ledgerNodeId});
  const eventHash = await _util.hasher(signed);

  /* Note: The ledger work session must ensure that a partial merge is
  completed before any other events are added. This is because some regular
  events may be pending and have local event numbers assigned to them that MUST
  be followed by the merge event that merges them, otherwise the ordering used
  for gossiping non-consensus events can become corrupt. */

  // FIXME: if the merge event has a non-witness merge event parent that
  // does not yet have a `localEventNumber` assigned to it, create an option
  // with the parent eventHash and localEventNumber (incrementing
  // `worker.nextLocalEventNumber`) to be passed into
  // `worker._addLocalMergeEvent` so that the parent event can be updated
  // in an ACID transaction... alternatively -- it could be done here without a
  // a transaction since the gossip server will truncate properly ... provided
  // that if this process fails before the merge event is added that the next
  // time the pipeline runs, the exact same non-consensus witness will be
  // selected

  // local merge events must be written directly to storage
  const meta = {
    blockHeight: -1,
    consensus: false,
    continuity2017: {
      creator: localPeerId,
      generation,
      isLocalContributor,
      lastLocalContributor: head.isLocalContributor ?
        head.eventHash : head.lastLocalContributor,
      // this will always be the same as `generation` for local merge events
      localAncestorGeneration: generation,
      localEventNumber: worker.nextLocalEventNumber++,
      // always `0` and `-1`, respectively, for local merge events
      localReplayNumber: 0,
      replayDetectedBlockHeight: -1,
      type: 'm'
    },
    eventHash
  };

  const record = await worker._addLocalMergeEvent({event: signed, meta});

  return {merged: true, halted: false, record, status};
};

// FIXME: use `priorityPeers` if appropriate or remove
async function _prepareNextMerge({
  worker, /*priorityPeers,*/ witnesses,
  basisBlockHeight, nonEmptyThreshold, emptyThreshold
}) {
  const {mergeEvents: {maxEvents}} = _continuityConstants;
  if(emptyThreshold >= maxEvents || nonEmptyThreshold >= maxEvents) {
    throw new Error(
      `Merge event thresholds (${emptyThreshold}, ${nonEmptyThreshold}) ` +
      `must be less than maximum permitted events (${maxEvents}).`);
  }

  const {localPeerId, ledgerNode, lastLocalContributorConsensus} = worker;
  const ledgerNodeId = ledgerNode.id;
  const {peerChildlessMap, pendingLocalRegularEventHashes} = worker;

  const status = {
    basisBlockHeight,
    generation: 0,
    isLocalContributor: false,
    hasOutstandingOperations: false,
    mergeHeight: 0,
    // this flag is only set to true if we've determined we need to merge but
    // we need more gossip to be able to do so
    needsGossip: false,
    parentHash: null,
    treeHash: null
  };

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
    // `pendingLocalRegularEventHashes` represent all local outstanding
    // regular events
    pendingLocalRegularEventHashes.size > 0 ||
    // keep track of whether or not the cache has been hit for later
    (hasOutstandingLocalOperations =
      !await _cache.operations.isEmpty({ledgerNodeId})) ||
    await hasOutstandingRegularEvents();

  // determine if peer is a witness
  const isWitness = witnesses.length === 0 || witnesses.includes(localPeerId);

  // if no outstanding operations of any sort, then there is nothing to merge;
  // also, if the peer is not a witness, then it cannot merge if the last local
  // contribution has not yet reached consensus
  if(!status.hasOutstandingOperations ||
    (!isWitness && !lastLocalContributorConsensus)) {
    return {mergeable: false, ...status};
  }

  /* Note: Next the merge strategy rules will be applied to ensure that this
  peer does not violate protocol (resulting in it being removed from the
  network; there may be a chance it can be surgically repaired to get back into
  good standing but this should be avoided).

  The rules work like this:

  1. No merge event may descent from any parent with a `basisBlockHeight` that
  is greater than its `basisBlockHeight`. Note that we assume we get this check
  for free here, because we do not accept any peer events that have a
  `basisBlockHeight` that is less than the `basisBlockHeight` we will set on
  the next merge event.

  2. If there are local operations that need to be merged (which will result
  in the creation of a "non-empty" merge event), a merge event may only be
  created if the `nonEmptyThreshold` is met. To meet this threshold, this node
  must have `nonEmptyThreshold` witness events from which to make the new
  merge event directly descend. Witness events used to meet this threshold
  MUST have a `mergeHeight` >= `head.mergeHeight` (`head` refers to the tree
  parent of the merge event to be created). Additionally, local operations
  may only be merged if the last merge event with local operations has reached
  consensus.

  3. If there are no local operations, then only a witness may make a merge
  event (which is called an "empty" merge event). Additionally, this merge
  event must directly descend from at least `emptyThreshold` (which defaults
  to `2f`) other merge events from unique witnesses. Witness events used to
  meet this threshold MUST have a `mergeHeight` >= `head.mergeHeight` (`head`
  refers to the tree parent of the merge event to be created). Note that the
  witnesses used when creating the next block are those for
  `basisBlockHeight+1` (the next block). So a merge event with a
  `basisBlockHeight` of `N` used witnesses for `N+1`.

  4. Finally, every merge event must have a `mergeHeight` that is 1 greater
  than the highest `mergeHeight` of its merge parents. The only exception is
  the genesis event, which always has a `mergeHeight` of zero. */

  // determine if we have anything local to merge
  const localOutstanding =
    pendingLocalRegularEventHashes.size > 0 ||
    // `hasOutstandingLocalOperations` may be undefined (cache not hit yet
    // due to short-circuiting), true, or false
    hasOutstandingLocalOperations === true ||
    (hasOutstandingLocalOperations === undefined &&
      !await _cache.operations.isEmpty({ledgerNodeId}));

  // determine if peer is a priority peer; priority peers must be witnesses,
  // and if `witnesses` are not given, assume all nodes are witnesses and if
  // `priorityPeers` are not given, assume all witnesses are priority peers
  // FIXME: ensure using priority peer here instead of `isWitness` cannot cause
  // consensus to fail
  const isPriorityPeer = isWitness;/* && (priorityPeers.length === 0 ||
    priorityPeers.includes(localPeerId));*/

  // if we have nothing local we can merge and we're not a priority peer with
  // peer events to merge, then there's nothing for us to merge
  if(!(localOutstanding && lastLocalContributorConsensus) &&
    !(isPriorityPeer && peerChildlessMap.size > 0)) {
    return {mergeable: false, ...status};
  }

  // get witness heads to target as potential parents
  const {heads: witnessHeads} = await worker._getHeads({peerIds: witnesses});
  // get local head to determine `treeHash`, merge generation, and merge height
  const {head} = worker;

  // determine which threshold to use based on whether or not there's
  // anything local to merge
  const threshold = localOutstanding ? nonEmptyThreshold : emptyThreshold;

  /* Note: Here we must find witness peer (merge) events that we can merge
  to meet the threshold. According to the protocol, we can only use a witness
  peer event as a parent if:

  1. The witness peer event's `basisBlockHeight` <= our `basisBlockHeight` (to
  be attached to the merge event we create). We don't have to check this here
  because we *assume* we have no peer events that have a `basisBlockHeight`
  that comes before our current `basisBlockHeight` by design.

  2. The witness peer event's `mergeHeight` >= `head.mergeHeight`. If it is
  not, it may still be mergeable, but will not count toward the threshold. */

  // first, filter witness heads into those that count toward the threshold;
  // use a `Set` because the genesis merge event is shared amongst all peers
  const thresholdWitnessHeads = new Set();
  for(const peerHead of witnessHeads.values()) {
    // to count toward the threshold, the peer head must not be our own head
    // and must have a `mergeHeight >= head.mergeHeight`.
    if(peerHead !== head && peerHead.mergeHeight >= head.mergeHeight) {
      thresholdWitnessHeads.add(peerHead);
    }
  }

  // only enforce thresholds if there is more than one witness or if the
  // local peer is not that one witness
  const isSoloWitness = witnesses.length === 1 && isWitness;
  if(!isSoloWitness && thresholdWitnessHeads.size < threshold) {
    // witness event threshold not met, cannot merge without more gossip
    status.needsGossip = true;
    return {mergeable: false, ...status};
  }

  // Note: At this point, we have determined that we will be merging.

  // add head as tree hash and the first parent hash and set merge generation
  status.treeHash = head.eventHash;
  const parentHashes = new Set();
  parentHashes.add(head.eventHash);
  status.generation = head.generation + 1;

  // add threshold witness heads to parent hashes and update `mergeHeight`;
  // the target number of parents is the tree parent + the threshold, with
  // a preference for a minimum of 2 parents (tree parent + a witness parent)
  // to help spread witness events
  const parentTarget = Math.max(2, 1 + threshold);
  let {mergeHeight} = head;
  for(const peerHead of thresholdWitnessHeads) {
    parentHashes.add(peerHead.eventHash);
    mergeHeight = Math.max(mergeHeight, peerHead.mergeHeight);
    if(parentHashes.size >= parentTarget) {
      break;
    }
  }

  // always add any already created local regular events, which could have only
  // been created if `lastLocalContributorConsensus` is true
  if(pendingLocalRegularEventHashes.size > 0) {
    status.isLocalContributor = true;
    for(const eventHash of pendingLocalRegularEventHashes) {
      parentHashes.add(eventHash);
    }
  }

  /* Note: At this point, we have determined that we can merge and have
  computed the required parent hashes. So, compute how many additional
  remaining parent slots there are so we can add a potential non-witness
  parent and operation events can be created and merged to flush out the
  operation queue (provided that `lastLocalContributorConsensus` is `true`). */
  let remainingSlots = maxEvents - parentHashes.size;

  // determine if a childless non-witness peer event is available to be added;
  // get remaining peer childless events
  const remainingChildlessPeerHeads = [];
  for(const [eventHash, peerHead] of peerChildlessMap) {
    if(!parentHashes.has(eventHash)) {
      remainingChildlessPeerHeads.push(peerHead);
    }
  }

  // FIXME: track whether the added non-witness event (if any) has had its
  // `localEventNumber` set yet; if not, it will need to be set and
  // the event meta updated in the database; if this happens we need
  // to ensure either via an ACID transaction or via some other means
  // (e.g., deterministic algorithm for choosing the next non-witness merge
  // event to use) that the same non-witness parent will always be chosen
  // in case the merge event we're about to create doesn't get written to
  // the database but the `localEventNumber` does get set for the non-witness
  // merge event; this is a requirement because we need to serve both
  // of these merge events *consecutively* during gossip or else we will
  // violate protocol

  // add a non-witness childless event as a parent first; this is important
  // to do even if an "empty merge event" is created by a witness, as it must
  // be able to add non-witness parents in addition to the `emptyThreshold`
  // witness parents to ensure that a network where all of the operations
  // are flowing through non-witnesses can reach consensus
  for(const peerHead of remainingChildlessPeerHeads) {
    // do not use `_isWitness` helper function which maps all peers to
    // witnesses if `witnesses` array is empty for unit tests
    if(!witnesses.includes(peerHead.creator)) {
      parentHashes.add(peerHead.eventHash);
      mergeHeight = Math.max(mergeHeight, peerHead.mergeHeight);
      remainingSlots--;
      break;
    }
  }

  // if the last local contribution has achieved consensus, then we can add
  // more local operations
  if(lastLocalContributorConsensus) {
    /* Create up to `remainingSlots` operation events and add them to the list
    of `parentHashes` so that they will be merged when passing the status to
    `merge`. If these new events are not merged due to an error or work session
    timeout, they will be included in `parentHashes` next time so there should
    be no corruption resulting in lost operation events or malformed merge
    events. */
    while(remainingSlots > 0 && !worker.halt()) {
      const {hasMore, eventHash} = await _events.create({ledgerNode, worker});
      // update merge status if an event was created
      if(eventHash) {
        status.isLocalContributor = true;
        pendingLocalRegularEventHashes.add(eventHash);
        parentHashes.add(eventHash);
        remainingSlots--;
      }
      if(!hasMore) {
        break;
      }
    }
  }

  // if peer is a witness, add in childless witnesses to fill slots to
  // speed along consensus
  if(isWitness && remainingSlots > 0) {
    for(const peerHead of remainingChildlessPeerHeads) {
      // use `_isWitness` helper function that maps all peers to witnesses
      // if `witnesses` array is empty for unit tests
      if(_isWitness({witnesses, creator: peerHead.creator}) &&
        !parentHashes.has(peerHead.eventHash)) {
        parentHashes.add(peerHead.eventHash);
        mergeHeight = Math.max(mergeHeight, peerHead.mergeHeight);
        remainingSlots--;
        if(remainingSlots === 0) {
          break;
        }
      }
    }
  }

  status.parentHash = [...parentHashes];
  status.basisBlockHeight = basisBlockHeight;
  status.mergeHeight = mergeHeight + 1;
  return {mergeable: true, ...status};
}

function _isWitness({witnesses, creator}) {
  // if witnesses not specified, test code is in use, presume all are witnesses
  return witnesses.length === 0 || witnesses.includes(creator);
}
