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
  const {mergeEvents: {maxEvents}} = _continuityConstants;
  if(emptyThreshold >= maxEvents || nonEmptyThreshold >= maxEvents) {
    throw new Error('Merge event thresholds must be less than maximum events.');
  }

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
  reuse. There's more exposition later on how we accomplish ensuring this
  in this implementation.

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

  4. Finally, every merge event must have a `mergeHeight` that is 1 greater
  than the maximum `mergeHeight` of its merge parents. The only exception is
  the genesis event, which always has a `mergeHeight` of zero. */

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
  // FIXME: ensure using priority peer here instead of `isWitness` cannot cause
  // consensus to fail
  const isPriorityPeer = isWitness;/* && (priorityPeers.length === 0 ||
    priorityPeers.includes(creatorId));*/

  // if there's something local to merge or the node is a priority peer and
  // there are peer events to merge, then we can continue, if not, return
  // early
  if(!(localOutstanding ||
    (isPriorityPeer && peerChildlessHashes.length > 0))) {
    return {mergeable: false, ...status};
  }

  // get witness heads to target as potential parents
  const {heads: witnessHeads} = await _history.getHeads(
    {ledgerNode, creatorIds: witnesses});
  // get head to determine `treeHash`, merge generation, and merge height;
  // we may have already retrieved our own head if we're a witness, otherwise
  // call `getHead` to fetch it
  const head = witnessHeads.get(creatorId) ||
    await _history.getHead({creatorId, ledgerNode});

  // determine which threshold to use based on whether or not there's
  // anything local to merge
  const threshold = localOutstanding ? nonEmptyThreshold : emptyThreshold;

  /* Note: Here we must find peer (merge) events that we can merge. According
  to the protocol, we can only use a peer event as a parent if:

  1. The peer event's `basisBlockHeight` <= our `basisBlockHeight` (to be
    attached to the merge event we create), OR, our merge event will include
    a regular event (at which point our peer parents can use any
    `basisBlockHeight`).
  2. We have not used the peer event as a parent before.

  The first check is a simple check. Secondly, we can determine if we have
  used a peer event as a parent before by querying the database directly, but
  this is slow. It turns out that we can leverage a protocol rule to help
  avoiding hitting the database in many cases. This rule requires every merge
  merge event to have a `mergeHeight` that is 1 greater than the maximum
  `mergeHeight` of its parents. Therefore, we can know we haven't used a peer
  event as a parent if:

  1. The peer event's `mergeHeight` >= our head's `mergeHeight`. This is
    because all of the events being considered have been validated to follow
    the above rule.

  So, taken together, a peer event may be merged if:

  1. `localOutstanding || peerHead.basisBlockHeight <= basisBlockHeight`
  AND
  2. `peerEvent.mergeHeight >= head.mergeHeight`

  However, if `1` is true and `2` is not, there is still a chance that the peer
  event may be mergeable. Any peer event we have previously merged will have a
  `mergeHeight` that is less than our head's `mergeHeight`. However, there may
  also be peer events that we have not used as parents that similarly have
  a lesser `mergeHeight`. We need to be able to merge such peer events or
  consensus could fail. For this case, we can first check to see if any of
  the peer events are "childless", meaning they haven't been used as parents
  at all yet -- as far as we know. That means we haven't merged them. Short
  of that, we will have to ask the database to tell us if we have used any of
  these peer events previously as parents. */

  // first, filter witness heads into those that definitely count toward the
  // threshold and those that MAY (but we aren't sure yet):
  let {mergeHeight} = head;
  const mergeableWitnessHeads = new Set();
  const maybeMergeableWitnessHeads = new Set();
  for(const peerHead of witnessHeads.values()) {
    // our own head does not count toward the threshold and we can only use
    // a peer event with a `basisBlockHeight` beyond our own if we have a
    // local event to merge
    if(peerHead === head ||
      (peerHead.basisBlockHeight > basisBlockHeight && !localOutstanding)) {
      // event is not mergeable
      continue;
    }
    // if true, we have definitely not merged this peer event before
    if(peerHead.mergeHeight >= head.mergeHeight) {
      mergeableWitnessHeads.add(peerHead);
      continue;
    }
    // we need more information to determine if we have merged this before
    maybeMergeableWitnessHeads.add(peerHead);
  }

  // next, determine if any of the `maybemergableWitnessHeads` are
  // "peerChildlessHashes", which means we know we do not descend from them
  // and thus they can be merged
  const peerChildlessHashesSet = new Set(peerChildlessHashes);
  for(const peerHead of maybeMergeableWitnessHeads) {
    if(peerChildlessHashesSet.has(peerHead.eventHash)) {
      maybeMergeableWitnessHeads.delete(peerHead);
      mergeableWitnessHeads.add(peerHead);
    }
  }

  // only if there are still insufficent witness merge events to use as parents
  // to meet the threshold do we hit the database to try and add more
  const maxMergeableWitnessHeads = mergeableWitnessHeads.size +
    maybeMergeableWitnessHeads.size;
  if(mergeableWitnessHeads.size < threshold &&
    maxMergeableWitnessHeads >= threshold) {
    // see if the `maybeMergeableWitnessHeads` have never been used as
    // parents by `creatorId`
    // FIXME: we may want to indicate whether or not a head has been previously
    // merged in the cache so we don't have to hit the database to check here
    const eventHashes = [];
    const hashToHead = new Map();
    for(const peerHead of maybeMergeableWitnessHeads) {
      eventHashes.push(peerHead.eventHash);
      hashToHead.set(peerHead.eventHash, peerHead);
    }
    // add peer heads that have not been used as parents to mergeable set
    const {diff} = await _history.diffNonParentHeads(
      {ledgerNode, creatorId, eventHashes});
    for(const eventHash of diff) {
      mergeableWitnessHeads.add(hashToHead.get(eventHash));
    }
  }

  if(mergeableWitnessHeads.size < threshold) {
    // witness event threshold not met, cannot merge
    return {mergeable: false, ...status};
  }

  // get parent hashes and update `mergeHeight`; use at least `threshold`
  // parent hashes from mergeable witness heads and at least 1, if available
  const parentHashes = [];
  const parentTarget = Math.max(1, threshold);
  for(const peerHead of mergeableWitnessHeads) {
    parentHashes.push(peerHead.eventHash);
    mergeHeight = Math.max(mergeHeight, peerHead.mergeHeight);
    if(parentHashes.length === parentTarget) {
      break;
    }
  }

  // Note: At this point, we have determined that we will be merging. We do
  // not need more gossip to be able to merge.
  status.needsGossip = false;

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
  let remainingSlots = maxEvents - parentHashes.length;
  while(remainingSlots > 0 && (!halt || !halt())) {
    const {hasMore, eventHash} = await _events.create({ledgerNode});
    // update merge status if an event was created
    if(eventHash) {
      localChildlessHashes.push(eventHash);
      parentHashes.push(eventHash);
    }
    if(!hasMore) {
      break;
    }
    remainingSlots--;
  }

  // fill any remaining slots in `parentHashes`
  if(remainingSlots > 0) {
    // get remaining peer childless events
    const parentHashSet = new Set(parentHashes);
    const remainingPeerChildlessHashes = [];
    for(const eventHash of peerChildlessHashes) {
      if(!parentHashSet.has(eventHash)) {
        remainingPeerChildlessHashes.push(eventHash);
      }
    }
    // FIXME: include `mergeHeight`, `basisBlockHeight`, and `creator` along
    // with peer childless hashes in the cache so we don't have to hit the
    // database for them
    const records = await _events.getEvents(
      {eventHash: remainingPeerChildlessHashes, ledgerNode});

    // add a non-witness childless event as a parent first; this is important
    // to do even if an "empty merge event" is created by a witness, as it must
    // be able to add non-witness parents in addition to the `emptyThreshold`
    // witness parents to ensure that a network where all of the operations
    // are flowing through non-witnesses can reach consensus
    for(const record of records) {
      const {
        meta: {continuity2017: {creator}, eventHash}, event
      } = record;
      if((localOutstanding || event.basisBlockHeight <= basisBlockHeight) &&
        !witnesses.includes(creator)) {
        parentHashSet.add(eventHash);
        parentHashes.push(eventHash);
        mergeHeight = Math.max(mergeHeight, event.mergeHeight);
        remainingSlots--;
        break;
      }
    }

    // if node is a witness, add in childless witnesses to fill slots to
    // speed along consensus
    if(isWitness && remainingSlots > 0) {
      for(const record of records) {
        const {
          meta: {continuity2017: {creator}, eventHash}, event
        } = record;
        if((localOutstanding || event.basisBlockHeight <= basisBlockHeight) &&
          _isWitness({witnesses, creator}) && !parentHashSet.has(eventHash)) {
          parentHashSet.add(eventHash);
          parentHashes.push(eventHash);
          mergeHeight = Math.max(mergeHeight, event.mergeHeight);
          remainingSlots--;
          if(remainingSlots === 0) {
            break;
          }
        }
      }
    }
  }

  status.parentHash = parentHashes;
  status.basisBlockHeight = basisBlockHeight;
  status.mergeHeight = mergeHeight + 1;
  return {mergeable: true, ...status};
}

function _isWitness({witnesses, creator}) {
  // if witnesses not specified, test code is in use, presume all are witnesses
  return witnesses.length === 0 || witnesses.includes(creator);
}
