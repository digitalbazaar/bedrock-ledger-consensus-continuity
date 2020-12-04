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
const logger = require('../logger');

const api = {};
module.exports = api;

// FIXME: remove me
api._merge = async ({ledgerNode, creatorId, priorityPeers, halt}) => {
  try {
    // prepare next merge
    const mergeStatus = await _prepareNextMerge(
      {ledgerNode, creatorId, priorityPeers, halt});
    const {mergeable, hasOutstandingOperations} = mergeStatus;

    // can't merge yet; we'll run again when later to try again
    if(!mergeable) {
      return {merged: false, hasOutstandingOperations};
    }

    // FIXME: we should be able to just call api.merge() from the worker
    // and get the result which will say whether or not a merge occurred...
    // and delete this file entirely -- will need to update `api.merge()`
    // to return non-null when it doesn't merge and it will need some
    // additional threshold parameters that can be set differently in some
    // "history" tests to ensure those continue to run

    // do the merge
    if(!halt()) {
      const record = await api.merge({creatorId, ledgerNode, mergeStatus});
      return {merged: !!record, record, hasOutstandingOperations};
    }
  } catch(e) {
    logger.error(`Error during merge: ${ledgerNode.id}`, {error: e});
  }

  return {merged: false};
};

api.merge = async ({
  creatorId, ledgerNode, priorityPeers, mergeStatus, halt
}) => {
  // FIXME: move `prepareNextMerge` into this function (combine them)
  // and move them into their own merge.js file
  if(!mergeStatus) {
    // merge status was not provided, prepare next merge on demand
    mergeStatus = await _prepareNextMerge(
      {ledgerNode, creatorId, priorityPeers, halt});
  }

  if(!mergeStatus.mergeable) {
    // nothing to merge
    return null;
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

  const {generation, treeHash, parentHash} = mergeStatus;

  // create, sign, and hash merge event
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
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
      type: 'm'
    },
    eventHash
  };
  const record = await ledgerNode.storage.events.add({event: signed, meta});

  // update cache
  await _cache.events.addLocalMergeEvent({...record, ledgerNodeId});

  // FIXME: return `{merged: true/false, record, status}`
  return record;
};

// FIXME: consider moving merge functions to a new `merge.js`
// FIXME: remove "priorityPeers"
async function _prepareNextMerge({
  ledgerNode, creatorId, priorityPeers = [], halt
}) {
  const ledgerNodeId = ledgerNode.id;
  const status = await _cache.events.getMergeStatus({ledgerNodeId});
  const {peerChildlessHashes, localChildlessHashes} = status;

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

  // FIXME: at this point, there's something to merge, but we need to decide:
  // 1. is our operation queue full
  // 2. have we timed out on a pending operation (should send it through)
  // 3. we have nothing local to send, but we're a witness and if we have
  //    `2f` peerChildlessHashes from witnesses to merge, we should make
  //    and empty merge event ... this should be included in the status
  //    information so we don't have to recompute it when it is time to
  //    merge; if it isn't present, we simply don't merge

  /*
  // FIXME:
  // 1. Concurrently:
  // 1.1. Get the `basisBlockHeight` we want to use from the latest
  //   block we've created and the witnesses for that `basisBlockHeight`.
  // 1.2. Check if local operation queue is full.
  // 1.3. Check if a local operation timeout has occurred.
  // 2. Determine if "this" node is a witness or not.
  // 3. If an operation is ready (1.2 or 1.3), create a merge event using
  //   the retrieved `basisBlockHeight`. Use filling rules for parents below.
  // 4. If an operation is not ready, get a list of the latest merge events
  //   from other witnesses that have not been merged yet by "this" node.
  // 5. If there are merge events with a `basisBlockHeight` that is less than
  //   or equal to our determined `basisBlockHeight` (from 1.1), then create
  //   an "empty merge event".
  //
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

  // determine if node is a priority peer
  const isPriorityPeer = priorityPeers.length === 0 ||
    priorityPeers.includes(creatorId);

  // if the node has nothing local to merge and there is nothing to merge from
  // peers or the node is not a priority peer, return immediately w/o merging
  if(!localOutstanding &&
    (peerChildlessHashes.length === 0 || !isPriorityPeer)) {
    return {mergeable: false, ...status};
  }

  // FIXME: apply rules to create parent hashes, add threshold
  // parameters as function parameters to allow tests to use different
  // values from built-in constants
  const witness = true;
  const parentHashes = status.parentHash = [];
  if(witness) {
    if(!localOutstanding) {
      // FIXME: apply "empty merge event threshold"
    } else {
      // FIXME: apply "witness merge event threshold"
    }

    // FIXME: if threshold cannot be met, return that cannot merge
  } else {
    // FIXME: apply non-witness threshold, if threshold cannot be met
    // return that cannot merge
  }

  // FIXME: remove this once the above is implemented to include the
  // appropriate peer childless hashes, but for now add some to keep
  // the implementation testable
  parentHashes.push(...peerChildlessHashes);

  // Note: At this point, we have determined that we will be merging.

  // we have local events/ops that need merging, but we should only merge
  // if our previous merge event contained only other merge events (none
  // of our local regular (operation) events); otherwise, we must wait for
  // other priority peers to send us their merge events before merging
  // get the head merge event from the database to ensure that the consensus
  // status is up-to-date
  // const [headRecord] = await getHead({creatorId});
  // const generation = headRecord ?
  //   headRecord.meta.continuity2017.generation : 0;
  const head = await _history.getHead({creatorId, ledgerNode});
  if(head.generation === 0) {
    // head is the genesis head which has no operation events, safe to merge
    // FIXME: note, when the head is the genesis head, it is safe to create
    // an empty merge event regardless of witness threshold -- this check
    // needs to be lifted higher
    //return {mergeable: true, ...status};
  }

  // FIXME: why is it unsafe to merge if our head does not have consensus?
  // FIXME: is the concern that consensus may have been reached by other
  // peers and we don't know it yet ... and this may result in extra
  // empty blocks? If so, requiring `2f` other new witness merge events
  // should help remedy/prevent that such that we don't have to be
  // concerned with it here.
  // FIXME: therefore, remove this line -- which also means we should be
  // able to use _history.getHead() ... potentially even using the cache
  // instead of the database
  // if the head does not have consensus then do not merge
  /*if(generation > 0 && !headRecord.meta.consensus) {
    return {mergeable: false, ...status};
  }*/

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

  return {mergeable: true, ...status};
}
