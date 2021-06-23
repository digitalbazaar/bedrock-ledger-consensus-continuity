/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _continuityConstants = require('../continuityConstants');
const _events = require('../events');
const _peers = require('../peers');
const _signature = require('../signature');
const _util = require('../util');
const {config} = require('bedrock');

const api = {};
module.exports = api;

api.merge = async ({
  // FIXME: change threshold to a single property and make it `witness`
  // threshold
  worker, priorityPeers = [], witnesses = worker.consensusState.witnesses,
  basisBlockHeight, peerWitnessParentThreshold
}) => {
  if(witnesses.size === 0) {
    throw new TypeError('"witnesses" must be a non-empty set.');
  }

  if(peerWitnessParentThreshold === undefined) {
    // both the solo witness case and the first generation case have no
    // peer witness parent threshold
    if(witnesses.size === 1 || worker.head.generation === 0) {
      peerWitnessParentThreshold = 0;
    } else {
      // `2f` is the default peer witness parent threshold
      const f = Math.max(1, (witnesses.size - 1) / 3);
      peerWitnessParentThreshold = 2 * f;
    }
  }

  const status = await _prepareNextMerge({
    worker, priorityPeers, witnesses,
    basisBlockHeight, peerWitnessParentThreshold
  });
  if(!status.mergeable) {
    // nothing to merge
    return {merged: false, halted: false, record: null, status};
  }

  if(worker.halt()) {
    // ran out of time to merge
    return {merged: false, halted: true, record: null, status};
  }

  const {
    isLocalContributor,
    generation,
    newPeerForCommitment,
    parentHash,
    peerHeadCommitment,
    requiredBlockHeight,
    treeHash
  } = status;

  // create, sign, and hash merge event
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    basisBlockHeight: status.basisBlockHeight,
    mergeHeight: status.mergeHeight,
    parentHash,
    treeHash
  };
  if(peerHeadCommitment) {
    event.parentHashCommitment = [peerHeadCommitment.eventHash];
  }
  if(newPeerForCommitment) {
    event.peerCommitment = [newPeerForCommitment.id];
  }
  const {head, localPeerId, ledgerNode} = worker;
  const ledgerNodeId = ledgerNode.id;
  const {signed} = await _signature.sign({event, ledgerNodeId});
  const eventHash = await _util.hasher(signed);

  /* Note: The ledger work session must ensure that a partial merge is
  completed before any other events are added. This is because some regular
  events may be pending and have local event numbers assigned to them that MUST
  be followed by the merge event that merges them, otherwise the ordering used
  for gossiping non-consensus events can become corrupt. */

  // local merge events must be written directly to storage
  const meta = {
    blockHeight: -1,
    consensus: false,
    continuity2017: {
      creator: localPeerId,
      generation,
      hasParentHashCommitment: !!event.parentHashCommitment,
      isLocalContributor,
      lastLocalContributor: head.isLocalContributor ?
        head.eventHash : head.lastLocalContributor,
      // this will always be the same as `generation` for local merge events
      localAncestorGeneration: generation,
      localEventNumber: worker.nextLocalEventNumber++,
      // always `0` and `-1`, respectively, for local merge events
      localReplayNumber: 0,
      replayDetectedBlockHeight: -1,
      requiredBlockHeight,
      type: 'm'
    },
    eventHash
  };

  const record = await worker._addLocalMergeEvent(
    {event: signed, meta, peerHeadCommitment});

  return {merged: true, halted: false, record, status};
};

async function _prepareNextMerge({
  worker, priorityPeers, witnesses,
  basisBlockHeight, peerWitnessParentThreshold
}) {
  const {mergeEvents: {maxParents, maxNonMergeParents}} = _continuityConstants;
  if(peerWitnessParentThreshold >= maxParents) {
    throw new Error(
      `Peer witness parent threshold (${peerWitnessParentThreshold}) ` +
      `must be less than maximum permitted parent events (${maxParents}).`);
  }

  const {localPeerId, ledgerNode, lastLocalContributorConsensus} = worker;
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
    newPeerForCommitment: null,
    parentHash: null,
    peerHeadCommitment: null,
    requiredBlockHeight: -1,
    treeHash: null
  };

  // report if any outstanding operations (local or peer);
  // Note: Consider configuration changes "outstanding operations" too.
  // check this in order of least costly function calls:
  // 1. If there are outstanding local regular events (memory)
  // 2. If there are outstanding local operations (cache)
  // 3. If there are outstanding peer regular events (database)
  let hasOutstandingLocalOperations;
  const {
    hasOutstandingRegularEvents,
    hasOutstandingParentHashCommitments
  } = ledgerNode.storage.events.plugins['continuity-storage'];
  status.hasOutstandingOperations =
    // `pendingLocalRegularEventHashes` represent all local outstanding
    // regular events
    pendingLocalRegularEventHashes.size > 0 ||
    // any withheld events include outstanding operations
    worker.hasWithheldEvents() ||
    // keep track of whether or not the cache has been hit for later
    (hasOutstandingLocalOperations =
      await worker.operationQueue.hasNextChunk()) ||
    await hasOutstandingRegularEvents({basisBlockHeight});

  // determine if peer is a witness
  const isWitness = witnesses.has(localPeerId);
  if(isWitness) {
    // a merge event created by a witness requires only `basisBlockHeight`
    // block height for acceptance
    status.requiredBlockHeight = basisBlockHeight;
  }

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

  2. Local operations may only be merged (resulting in the creation of a
  "non-empty" or "contributing" merge event) if the last merge event with local
  operations has reached consensus (or the merge event is the first one that
  merges local operations).

  3. If there are no local operations, then only a witness may make a merge
  event (which is called an "empty" merge event). Additionally, this merge
  event must directly descend from at least `peerWitnessParentThreshold` (which
  defaults to `2f`) other merge events from unique peer witnesses. Witness
  events used to meet this threshold MUST have a
  `mergeHeight` >= `head.mergeHeight` (`head` refers to the tree parent of the
  merge event to be created). Note that the witnesses used when creating the
  next block are those for `basisBlockHeight+1` (the next block). So a merge
  event with a `basisBlockHeight` of `N` used witnesses for `N+1`.

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
      await worker.operationQueue.hasNextChunk());

  // determine if peer is a priority peer; priority peers must be witnesses,
  // and if `witnesses` are not given, assume all nodes are witnesses and if
  // `priorityPeers` are not given, assume all witnesses are priority peers
  // FIXME: ensure using priority peer here instead of `isWitness` cannot cause
  // consensus to fail
  const isPriorityPeer = isWitness && (priorityPeers.length === 0 ||
    priorityPeers.includes(localPeerId));

  // determine if a contributing aka "non-empty" merge event can be created
  const canLocallyContribute = localOutstanding &&
    lastLocalContributorConsensus;

  // if we have nothing local we can merge AND we're not a priority peer with
  // peer events to merge AND we're not a witness with either withheld events
  // to commit to/merge or outstanding parent hash commitments, then there's
  // nothing for us to merge
  if(!canLocallyContribute &&
    !(isPriorityPeer && peerChildlessMap.size > 0) &&
    !(isWitness && (worker.hasWithheldEvents() ||
      hasOutstandingParentHashCommitments({basisBlockHeight})))) {
    return {mergeable: false, ...status};
  }

  // get local head to determine `treeHash`, merge generation, and merge
  // height; get `mergeCommitment` to ensure previously committed event can
  // still be legally merged
  const {head, mergeCommitment} = worker;

  // get witness heads to target as potential parents
  const {heads: witnessHeads} = await worker._getHeads({peerIds: witnesses});

  // if we can't locally contribute, then ensure that the "empty" merge event
  // threshold can be met; if can locally contribute, ensure a peer commitment
  // parent exists if required
  const thresholdWitnessHeads = new Map();
  let peerCommitmentParent;
  if(!canLocallyContribute) {
    /* Note: Here we must find witness peer (merge) events that we can merge
    to meet the threshold. According to the protocol, we can only use a witness
    peer event as a parent if:

    1. The witness peer event's `basisBlockHeight` <= our `basisBlockHeight` (to
    be attached to the merge event we create). We don't have to check this here
    because we *assume* we have no peer events that have a `basisBlockHeight`
    that comes before our current `basisBlockHeight` by design.

    2. The witness peer event's `mergeHeight` >= `head.mergeHeight`. If it is
    not, it may still be mergeable, but will not count toward the threshold. */

    // first, filter witness heads into those that count toward the threshold
    for(const peerHead of witnessHeads.values()) {
      // to count toward the threshold, the peer head must not be our own head
      // must have a `mergeHeight >= head.mergeHeight`, and must have a unique
      // event hash (e.g., do not count genesis head more than once)
      if(peerHead.eventHash !== head.eventHash &&
        peerHead.mergeHeight >= head.mergeHeight &&
        !thresholdWitnessHeads.has(peerHead.eventHash)) {
        thresholdWitnessHeads.set(peerHead.eventHash, peerHead);
        if(thresholdWitnessHeads.size === peerWitnessParentThreshold) {
          break;
        }
      }
    }

    // if threshold not met, cannot merge without more gossip
    if(thresholdWitnessHeads.size < peerWitnessParentThreshold) {
      status.needsGossip = true;
      return {mergeable: false, ...status};
    }
  } else if(head.generation === 0 && !isWitness &&
    !await _peers.hasBeenWitness({ledgerNode, peerId: localPeerId})) {
    // this is the first generation event for the local peer and the local peer
    // has never been a witness, so fetch a peer commitment to use as a parent
    const [commitment] = await _peers.getCommitmentToPeer(
      {ledgerNode, peerId: localPeerId});
    if(!commitment) {
      // no peer commitment yet, cannot merge
      return {mergeable: false, ...status};
    }
    peerCommitmentParent = {
      creator: commitment.meta.continuity2017.creator,
      mergeHeight: commitment.event.mergeHeight,
      eventHash: commitment.meta.eventHash
    };
  }

  // Note: At this point, we have determined that we will be merging.

  // start obtaining a new peer to commit to concurrently with other merge
  // work below
  const peerForCommitmentPromise = _peers.getPeerForCommitment(
    {ledgerNode, localPeerId, witnesses});

  // add head as tree hash and the first parent hash and set merge generation
  status.treeHash = head.eventHash;
  const parentHashes = new Set();
  parentHashes.add(head.eventHash);
  status.generation = head.generation + 1;

  // add either threshold witness parents (for an empty merge event) or add
  // peer commitment parent for a non-empty merge event that requires it
  let {mergeHeight} = head;
  const peerCreatorSet = new Set();
  if(!canLocallyContribute) {
    // add threshold witness heads to parent hashes and update `mergeHeight`;
    // the target number of parents is the tree parent + the threshold, with
    // a preference for a minimum of 2 parents (tree parent + a witness parent)
    // to help spread witness events
    const parentTarget = Math.max(2, 1 + thresholdWitnessHeads.size);
    for(const peerHead of thresholdWitnessHeads.values()) {
      parentHashes.add(peerHead.eventHash);
      peerCreatorSet.add(peerHead.creator);
      mergeHeight = Math.max(mergeHeight, peerHead.mergeHeight);
      if(parentHashes.size >= parentTarget) {
        break;
      }
    }
  } else if(peerCommitmentParent) {
    // add peer commitment parent
    parentHashes.add(peerCommitmentParent.eventHash);
    peerCreatorSet.add(peerCommitmentParent.creator);
    mergeHeight = Math.max(mergeHeight, peerCommitmentParent.mergeHeight);
  }

  // always add any already created local regular events, which could have only
  // been created if `lastLocalContributorConsensus` is true
  let nonMergeParents = 0;
  if(pendingLocalRegularEventHashes.size > 0) {
    for(const eventHash of pendingLocalRegularEventHashes) {
      parentHashes.add(eventHash);
      nonMergeParents++;
    }
  }

  /* Note: At this point, we have determined that we can merge and have
  computed the required parent hashes. Compute how many additional remaining
  parent slots there are so we can add a potential non-witness parent and
  operation events can be created and merged to flush out the operation queue
  (provided that `lastLocalContributorConsensus` is `true`). */
  let remainingSlots = maxParents - parentHashes.size;

  /* Determine if there is a slot available to merge a non-witness event:
  1. There are at least two slots left, OR
  2. Local peer is a witness and there's at least one slot left, OR
  3. There's only one slot left but regular events have already been added. */
  const hasNonWitnessSlot =
    remainingSlots >= 2 || (isWitness && remainingSlots > 0) ||
    (remainingSlots > 0 && pendingLocalRegularEventHashes.size > 0);

  /* If there is room for a non-witness parent and there is a pending
  commitment that has reached consensus, merge the event committed to. For
  witnesses, any commitment from *any* peer will suffice to allow merging a
  non-witness event, however, the implementation is simpler when relying upon
  its own commitment. Non-witnesses can only use their own committments and
  the commitment must have been made in its current head. Note that
  `mergeCommitment` is cleared elsewhere before this point if the non-witness
  parent should not be merged because it was detected as a replayer. */
  if(hasNonWitnessSlot && mergeCommitment && mergeCommitment.consensus &&
    !peerCreatorSet.has(mergeCommitment.committedTo.creator)) {
    // if local peer is a witness, any commitment will do, but a non-witness
    // requires its last merge event to be the one that committed
    const {committedBy, committedTo} = mergeCommitment;
    if(isWitness || head.eventHash === committedBy.eventHash) {
      parentHashes.add(committedTo.eventHash);
      peerCreatorSet.add(committedTo.creator);
      mergeHeight = Math.max(mergeHeight, committedTo.mergeHeight);
      remainingSlots--;
    }
  }

  // compute remaining childless peer heads to fill out remaining slots; they
  // cannot come from creators that have already contributed a parent
  const remainingChildlessPeerHeads = [];
  for(const peerHead of peerChildlessMap.values()) {
    if(!peerCreatorSet.has(peerHead.creator)) {
      remainingChildlessPeerHeads.push(peerHead);
    }
  }

  /* If there is no pending merge commitment or the current one has reached
  consensus, then we can make a new commitment.

  For non-witnesses, the current merge commitment will have reached consensus
  every time we merge because the tree parent must reach consensus before we
  can merge again according to the protocol rules.

  For witnesses, that same consensus rule only applies to merge events that
  include local contributions (e.g., operations). It does not apply to "empty"
  merge events. For "empty" merge events, we end up making no commitment until
  our previous one (whether it was via an empty or non-empty merge event) has
  reached consensus to enable us to easily keep track of whether we are
  adhereing to protocol. According to protocol, witnesses can merge a
  non-witness event as long as *any* peer has produced an event that committed
  to it and that event has reached consensus at any point at or before the
  `basisBlockHeight` to be used for the next merge event. However, we rely on
  checking that our *own* event committed to the non-witness event and that it
  has reached consensus to avoid having to scan the database for such events
  from other peers.

  It is conceivable that we may have a `mergeCommitment` that has reached
  consensus and was not merged above because there was not an available
  non-witness slot (Note: "merge above" covers merging the event because its
  creator has either become a witness now or due to the commitment). Ledgers
  should generally be configured to ensure that there is always a non-witness
  parent slot to fill. But in the case of a ledger that allows there to
  sometimes be insufficient slots, we will drop our commitment here and rely
  on another peer to commit and merge -- or we will have to eventually gossip
  again with the non-witness peer. */
  if(!mergeCommitment || mergeCommitment.consensus) {
    /* Find a non-witness merge event to commit to. Look first in the withheld
    events from the worker, and second using the remaining childless peer
    heads. */

    // first try to get a withheld event to commit to
    const withheld = worker._selectWithheld({witnesses});
    if(withheld) {
      status.peerHeadCommitment = {
        basisBlockHeight: withheld.mergeEvent.event.basisBlockHeight,
        creator: withheld.mergeEvent.meta.continuity2017.creator,
        eventHash: withheld.mergeEvent.meta.eventHash,
        mergeHeight: withheld.mergeEvent.event.mergeHeight
      };
    }

    /* If no peer head to commit to could be found in the withheld cache, then
    try to select one from the remaining childless peer heads. Note that the
    remaining childless peer heads have been filtered such that any creator
    of a parent we've already added will not have a peer head in the list. It
    is conceivable that the same creator we committed to last time and that we
    already have a parent for could technically have a childless peer that we
    filtered out that we could have committed to here without violating
    protocol. This could occur if another witness had already committed to the
    same parent event we just committed to *and* it had reached consensus and
    its creator has already gossiped the next event).

    This seems unlikely and we would rather not have to change the code to
    allow for that corner case when there are no other non-witnesses to choose
    from. Instead we use a simpler implementation that will result in our
    commitment slot not being used for the same creator twice in a row. */
    if(!status.peerHeadCommitment) {
      for(const peerHead of remainingChildlessPeerHeads) {
        if(!witnesses.has(peerHead.creator)) {
          status.peerHeadCommitment = peerHead;
          break;
        }
      }
    }
  }

  // if the peer can locally contribute, then the event will be a local
  // contributor
  if(canLocallyContribute) {
    status.isLocalContributor = true;
  }

  // if contributing, add more local operations to fill out remaining slots
  if(status.isLocalContributor) {
    let remainingNonMergeSlots = Math.min(
      maxNonMergeParents - nonMergeParents, remainingSlots);

    /* Create up to `remainingNonMergeSlots` operation events and add them to
    the list of `parentHashes` so that they will be merged when passing the
    status to `merge`. If these new events are not merged due to an error or
    work session timeout, they will be included in `parentHashes` next time so
    there should be no corruption resulting in lost operation events or
    malformed merge events. */
    while(remainingNonMergeSlots > 0 && !worker.halt()) {
      const {hasMore, eventHash} = await _events.create({ledgerNode, worker});
      // update merge status if an event was created
      if(eventHash) {
        status.isLocalContributor = true;
        pendingLocalRegularEventHashes.add(eventHash);
        parentHashes.add(eventHash);
        remainingNonMergeSlots--;
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
      if(witnesses.has(peerHead.creator) &&
        !peerCreatorSet.has(peerHead.creator)) {
        parentHashes.add(peerHead.eventHash);
        peerCreatorSet.add(peerHead.creator);
        mergeHeight = Math.max(mergeHeight, peerHead.mergeHeight);
        remainingSlots--;
        if(remainingSlots === 0) {
          break;
        }
      }
    }
  }

  // get any new peer to commit to
  status.newPeerForCommitment = await peerForCommitmentPromise;
  status.parentHash = [...parentHashes];
  status.basisBlockHeight = basisBlockHeight;
  status.mergeHeight = mergeHeight + 1;
  return {mergeable: true, ...status};
}
