/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _blocks = require('./blocks');
const _cache = require('./cache');
const _continuityConstants = require('./continuityConstants');
const _witnesses = require('./witnesses');
const bedrock = require('bedrock');

const api = {};
module.exports = api;

bedrock.events.on('bedrock.init', () => {
  const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
  const {mergeEvents: {maxParents}} = _continuityConstants;
  // this limit must be large enough to ensure at least one entire merge event
  if(gossip.maxEvents <= 0 || gossip.maxEvents < maxParents) {
    throw new Error(
      'Gossip configuration is invalid; "gossip.maxEvents" ' +
      `(${gossip.maxEvents}) must be a positive number that is greater than ` +
      'or equal to continuity constants "mergeEvents.maxParents" ' +
      `(${maxParents}).`);
  }
});

// returns a partition of the DAG for a (remote) peer with the given
// remoteInfo: {basisBlockHeight, localEventNumber, peerId, peerHeads}
api.partition = async ({ledgerNode, remoteInfo} = {}) => {
  const result = {
    blockHeight: 0,
    blockHash: null,
    batch: {
      eventHash: []
    },
    cursor: null
  };

  const timer = new _cache.Timer();
  timer.start({name: 'partition', ledgerNodeId: ledgerNode.id});

  // the remote peer's declared `basisBlockHeight` is the block height it
  // knows about, so its next block height is one greater
  const remoteNextBlockHeight = remoteInfo.basisBlockHeight + 1;

  // get local peer's current block info and event history that occurs after
  // the remote peer's known block height
  const nextBlockInfo = await _blocks.getNextBlockInfo({ledgerNode});
  const localBlockHeight = nextBlockInfo.blockHeight - 1;
  const eventHistory = await api._getEventHistory({
    ledgerNode, remoteInfo,
    localBlockHeight,
    minBlockHeight: remoteNextBlockHeight
  });

  // include local peer's latest block height and block hash in the result
  result.blockHeight = localBlockHeight;
  result.blockHash = nextBlockInfo.previousBlockHash;
  // include events that occur in the current batch and the cursor for the
  // next batch
  result.batch.eventHash = eventHistory.eventHash;
  result.cursor = eventHistory.cursor;
  timer.stop();
  return result;
};

// TODO: documentation; adds the hashes for non-consensus regular events
// ancestors to the merge event hashes and that must be included in the block
api._addNonConsensusAncestorHashes = async function({
  blockHeight, hashes, ledgerNode
} = {}) {
  /* Note: `hashes.parentHashes` includes hashes for parents that were not
  included in the DAG set used to compute consensus. These hashes either refer
  to merge events that have already achieved consensus or they refer to regular
  events that are achieving consensus now. We must ensure we include these
  regular events in the next block, so we filter out those external parents
  that haven't acheived consensus yet here. */
  const {parentHashes} = hashes;

  // by now, all referenced events MUST exist because all events that were
  // fed into the consensus algorithm were validated, so go ahead and find
  // those that have not yet acheived consensus
  const nonConsensusHashes = await _filterHashes(
    {ledgerNode, consensus: false, eventHashes: parentHashes});

  // events may have been assigned to the current block during a prior
  // failed operation. All events must be included so that `blockOrder`
  // can be properly computed
  const nonConsensusSet = new Set(nonConsensusHashes);
  const notFound = parentHashes.filter(h => !nonConsensusSet.has(h));
  // FIXME: the test suite does not pass `blockHeight` into this API so we
  // have `blockHeight === undefined` here -- the test suite should be updated
  // so we can remove it
  if(notFound.length !== 0 && blockHeight !== undefined) {
    const hashes = await _filterHashes(
      {ledgerNode, blockHeight, eventHashes: notFound});
    nonConsensusHashes.push(...hashes);
  }
  // return all merge event hashes and the hashes for non-consensus regular
  // events that must be included in the block
  return hashes.mergeEventHashes.concat(nonConsensusHashes);
};

api._getEventHistory = async function({
  ledgerNode, remoteInfo, localBlockHeight, minBlockHeight
} = {}) {
  const result = {
    eventHash: [],
    cursor: null
  };

  // get the head information for the peer heads referenced by the remote
  // peer in `peerHeads`; if the peer heads aren't known to the local peer,
  // they cannot be used to filter event history and they are ignored
  const {getKnownPeerHeads} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  const {knownHeads} = await getKnownPeerHeads(
    {peerHeads: remoteInfo.peerHeads});

  // fetch event summary info up to `maxEvents` where
  // `localEventNumber >= ` remote peer provided number, defaulting to 0
  const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
  let limit = gossip.maxEvents;
  const {getSortedEventSummaries} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  const records = await getSortedEventSummaries({
    minBlockHeight,
    minLocalEventNumber: remoteInfo.localEventNumber,
    // add one to `limit` to enable detection of more available events
    limit: limit + 1
  });
  if(records.length === 0) {
    // no records are available, so keep `cursor` the same based on what was
    // requested by the peer and indicate no other events are available
    result.cursor = {
      requiredBlockHeight: remoteInfo.basisBlockHeight,
      localEventNumber: remoteInfo.localEventNumber || 0,
      hasMore: false
    };
    return result;
  }

  // store whether or not there is more event history based on whether the
  // limit was breached (the query can return at most `1` more record than
  // `limit`); pop-off `overflowRecord` as it should not be included in the
  // filtered results and it may be needed for possible reference in the cursor
  let overflowRecord;
  if(records.length > limit) {
    overflowRecord = records.pop();
  }

  // filter by remote peer ID and any known peer heads from the remote peer
  const {peerId: remotePeerId} = remoteInfo;
  const filtered = _filterByPeerHeads(
    {records, remotePeerId, peerHeads: knownHeads});

  // if there is an `overflowRecord`, update the limit to match the current
  // `filtered.length` and then append the `overflowRecord` so that when
  // we check for acceptable history, the `overflowRecord` will get truncated
  // (and not sent) and marked as the `cursorRecord` if no earlier truncation
  // occurs
  if(overflowRecord) {
    limit = filtered.length;
    filtered.push(overflowRecord);
  }

  if(filtered.length === 0) {
    /* Note: All records have been filtered out and no more are available in
    the database. Since we don't know what the next `basisBlockHeight` will be,
    set `cursor` to use the same `basisBlockHeight` that the remote peer
    provided. Use the last record from the database to get the highest
    `localEventNumber` to which we increment by 1 to get what we know will be
    the next `localEventNumber`. */
    // it is known that `records.length > 0` here
    const {
      meta: {continuity2017: {localEventNumber}}
    } = records[records.length - 1];
    result.cursor = {
      requiredBlockHeight: remoteInfo.basisBlockHeight,
      // refer to future next `localEventNumber`
      localEventNumber: localEventNumber + 1,
      hasMore: false
    };
    return result;
  }

  // get the number of records that the remote peer can accept (we must ensure
  // that that the last record being sent is a merge event and that all of the
  // records to be returned meet the remote peer's `basisBlockHeight`
  // requirement); sending trailing non-merge events or an event that the
  // remote peer can't yet process is a violation of protocol
  const {
    acceptableCount, truncated, cursorRecord
  } = await _getAcceptableHistory({
    ledgerNode, records: filtered, limit,
    localBlockHeight, basisBlockHeight: remoteInfo.basisBlockHeight
  });

  // set `cursor` based on `cursorRecord`
  const {
    event: {basisBlockHeight},
    meta: {continuity2017: {localEventNumber, requiredBlockHeight}}
  } = cursorRecord;
  if(truncated) {
    // since we have the information available, use the next record to
    // determine what to send in the cursor
    result.cursor = {
      // if the next record has a `requiredBlockHeight` set, the peer needs
      // to have at least that `basisBlockHeight` to receive it
      requiredBlockHeight: Math.max(basisBlockHeight, requiredBlockHeight),
      localEventNumber,
      hasMore: true
    };
  } else {
    /* Note: Since we did not truncate, we cannot know what the
    `basisBlockHeight` will be required for the next event. If the next event
    actually requires a higher `basisBlockHeight` than the remote peer has
    supplied, the local peer will correct for this by returning a new cursor
    value with the required `basisBlockHeight`. We do know the next event's
    `localEventNumber` which is one more than the one from `nextRecord`. */
    result.cursor = {
      requiredBlockHeight: remoteInfo.basisBlockHeight,
      localEventNumber: localEventNumber + 1,
      hasMore: false
    };
  }

  // return event hashes
  if(acceptableCount > 0) {
    filtered.length = acceptableCount;
    const {eventHash} = result;
    for(const record of filtered) {
      eventHash.push(record.meta.eventHash);
    }
  }

  return result;
};

// returns history that the remote peer can accept; note that this API assumes
// that `records.length > 0`
async function _getAcceptableHistory({
  ledgerNode, records, limit, localBlockHeight, basisBlockHeight
}) {
  // `limit` is necessarily larger than 1, meaning that if the limit is hit,
  // we will always properly check for a trailing merge event in the loop below
  // if any events have an acceptable `basisBlockHeight`
  const maxRecords = Math.min(limit, records.length);

  /* Note: `records` may contain some records that have a greater
  `basisBlockHeight` than the remote peer will accept and we MUST NOT send
  these to the remote peer. We can't limit the query to find these records to
  what the remote peer accepts because then we wouldn't know if there is more
  history to return after it. Also, events may have been created by
  non-witnesses and require commitments or knowledge that a non-witness
  creator became witness in the subsequent block. Therefore, we must walk
  forward to limit the number of records by `basisBlockHeight` and non-witness
  requirements before walking backwards.

  Non-witness events MUST NOT be sent to the remote peer unless they meet
  certain conditions. Non-witness events are those that were created by a peer
  that was a non-witness at the block height at which the event was created
  (its `basisBlockHeight + 1`) and at every subsequent block. We can only send
  these to the remote peer if:

  1. We know the remote peer must have a commitment for the non-witness event
    that has reached consensus, OR
  2. We know the remote peer knows that the event's creator became a witness
    at some block height beyond `basisBlockHeight + 1`, OR
  3. The non-witness event is from this local peer and, in this case, we cannot
    serve the merge event *unless* it is the last one in the batch, so we must
    truncate after it. */
  let acceptableCount;
  let witnesses;
  for(acceptableCount = 0; acceptableCount < maxRecords; ++acceptableCount) {
    const {
      event, meta: {continuity2017: {creator, type, requiredBlockHeight}}
    } = records[acceptableCount];
    /* Do not serve an event with a `basisBlockHeight` greater than the remote
    peer's nor can we safely serve an event that has a `requiredBlockHeight`
    greater than the remote peer's `basisBlockHeight`. Note: The meta field
    `requiredBlockHeight` stores the block height at which we detected the
    merge event can be safely stored (it is from a detectable witness or it has
    a commitment that reached consensus) OR it has a value of `-1` to indicate
    that the event was locally created by a peer that was a non-witness at
    the block height during which the event was created, i.e.,
    `event.basisBlockHeight + 1`. */
    if(event.basisBlockHeight > basisBlockHeight ||
      requiredBlockHeight > basisBlockHeight) {
      break;
    }

    // if `requiredBlockHeight` is not `-1` then it is not local and, at this
    // point it is known to the remote peer, so it is safe to send
    if(requiredBlockHeight !== -1) {
      continue;
    }

    // skip past all non-merge events, we will rewind to a merge event
    // below if we do not terminate in one here
    if(type !== 'm') {
      continue;
    }

    // at this point we know that the event was created by the local peer,
    // that it was a non-witness at its creation, and we haven't seen it
    // become a witness in previous blocks...

    // if the event is the last one in the batch, go ahead and accept it
    // because it is legal to send it as the last one regardless of the
    // witness status of the local peer
    if((acceptableCount + 1) === maxRecords) {
      acceptableCount++;
      break;
    }

    // the local peer might have become a witness for the next block; if the
    // remote peer can detected this and the local peer is in fact a witness
    // for the next block, return it and continue
    if(basisBlockHeight >= localBlockHeight) {
      if(!witnesses) {
        ({witnesses} = await _witnesses.getBlockWitnesses(
          {ledgerNode, blockHeight: localBlockHeight + 1}));
      }
      if(witnesses.has(creator)) {
        acceptableCount++;
        continue;
      }
    }

    // merge event was created by a local non-witness, must truncate after it
    acceptableCount++;
    break;
  }

  // walk backwards to find acceptable trailing merge event
  while(acceptableCount > 0) {
    /* Note: This works because the ordering algorithm used on `records` causes
    non-merge events (e.g., operation or config events) to be inserted just
    prior to their child merge event instead of spreading them out
    breadth-first like merge events are. Therefore we should never have ordered
    events like this: [r1,r2,m1,m2] as that would create a problem if we
    truncated after `m1` since `r2` would have no merge event transferred along
    with it. But the ordering algorithm used results in this ordering instead:
    `[r1,m1,r2,m2]`, so when we truncate after `m1` there's nothing that comes
    before it that is incomplete. */
    if(records[acceptableCount - 1].meta.continuity2017.type === 'm') {
      // acceptable trailing merge event found, break
      break;
    }
    acceptableCount--;
  }

  /* Note: Get the record to use to populate `cursor` so the remote peer can
  continue from where it left off with the next request. If the acceptable
  count is less than `records.length`, then use the record right after the last
  acceptable one. Otherwise, use the very last acceptable record (but the
  cursor information will need to be appropriately altered to account for this
  record being one that was returned vs. one that was not). */
  const truncated = acceptableCount < records.length;
  const cursorRecord = truncated ?
    records[acceptableCount] : records[acceptableCount - 1];
  return {acceptableCount, truncated, cursorRecord};
}

async function _filterHashes(
  {ledgerNode, blockHeight, consensus, eventHashes}) {
  // retrieve up to 1000 at a time to prevent hitting limits or starving
  // resources
  const batchSize = 1000;
  const filteredHashes = [];
  const chunks = _.chunk(eventHashes, batchSize);
  for(const chunk of chunks) {
    const filteredChunk = await ledgerNode.storage.events.filterHashes(
      {blockHeight, consensus, eventHash: chunk});
    filteredHashes.push(...filteredChunk);
  }
  return filteredHashes;
}

function _filterByPeerHeads({records, remotePeerId, peerHeads} = {}) {
  // return early if there are no records to filter
  if(records.length === 0) {
    return [];
  }

  /* 1. Build a DAG from the event summaries. The given event summaries are
  guaranteed to be in topological order, with parent events coming before
  children. */
  const graph = new Map();
  // track which events are childless in the partition to use them as
  // starting points (i.e., `startHeads`) for the filtering process
  const startHeads = new Set();
  let nonMergeEvents = [];
  for(const record of records) {
    const {meta: {eventHash, continuity2017: {type}}} = record;
    // non-merge events will either be followed by their child merge event or
    // they will be "dangling" because their child merge event was not fetched
    // in this partition of history (in which case they will just be appended)
    // at the end of the filtered history; we do not need to process their
    // relationships, we just tack them onto their child merge event
    if(type !== 'm') {
      nonMergeEvents.push(record);
      continue;
    }

    // an empty `nonMergeEvents` array will be shared amongst all merge events
    // without any non-merge parents; this is considered a safe optimization
    const parents = [];
    const vertex = {record, parents, children: [], nonMergeEvents};
    if(nonMergeEvents.length > 0) {
      // if there were non-merge events, clear the array so non-merge events
      // are not erroneously assigned to the wrong merge child; note that if
      // a replayer has two or more merge events that share the same non-merge
      // parents, the shared non-merge events will only be included with the
      // first merge event the local peer received them with (this is fine)
      nonMergeEvents = [];
    }
    graph.set(eventHash, vertex);
    startHeads.add(vertex);

    // add parents to `vertex` and `children` to all parent vertices
    for(const parentHash of record.event.parentHash) {
      const parentVertex = graph.get(parentHash);
      if(!parentVertex) {
        // `parentVertex` is not in `graph` (it comes before this partition of
        // history or refers to a non-merge event); skip it
        continue;
      }
      parents.push(parentVertex);
      parentVertex.children.push(vertex);
      // if a parent has children, remove it as a starting point for the
      // filtering process
      startHeads.delete(parentVertex);
    }
  }

  /* 2. Filter out events using the given `endHeads`. Compare both `generation`
  and `localReplayNumber` information to determine if we can short-circuit or
  not. For heads of replayers, this may be inefficient, resulting in hashes
  being sent to the remote peer that it actually has, which is not a protocol
  violation. It's up to the remote peer to compare on its end and decide what
  to request or if it should ask for more hashes using the `cursor` information
  provided by the local peer to continue (the local peer will send its
  `localEventNumber` to enable continuation even when no heads or heads of
  replayers are given). */
  // create index for finding end heads by creator
  const endHeadsMap = new Map();
  for(const head of peerHeads) {
    const entry = endHeadsMap.get(head.creator);
    if(entry) {
      entry.push(head);
    } else {
      endHeadsMap.set(head.creator, [head]);
    }
  }
  // process `startHeads` to walk backwards through DAG ancestry, adding any
  // hashes for events that are not ancestors of `endHeads`
  const eventHashes = new Set();
  let next = [...startHeads];
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const vertex of current) {
      const {meta: {
        eventHash,
        continuity2017: {creator, generation, localReplayNumber}
      }} = vertex.record;
      // if the creator of the event is the remote peer, we can always skip
      // as the event came from them originally
      if(creator === remotePeerId) {
        continue;
      }
      // see if we can skip based on the given peer head
      const heads = endHeadsMap.get(creator);
      let skip = false;
      if(heads) {
        for(const head of heads) {
          // if replay number matches and generation is <= than an end head,
          // then the event should not be returned in this partition of history
          if(localReplayNumber === head.localReplayNumber &&
            generation <= head.generation) {
            skip = true;
            break;
          }
        }
      }
      if(skip) {
        continue;
      }

      // include event in the partition of history, including any non-merge
      // events it descends from
      eventHashes.add(eventHash);
      for(const record of vertex.nonMergeEvents) {
        eventHashes.add(record.meta.eventHash);
      }

      // add parents to `next` if they aren't already in `eventHashes`
      for(const parentVertex of vertex.parents) {
        if(!eventHashes.has(parentVertex.record.meta.eventHash)) {
          next.push(parentVertex);
        }
      }
    }
  }

  // filter records based on chosen hashes, preserving order
  const filtered = [];
  for(const record of records) {
    if(eventHashes.has(record.meta.eventHash)) {
      filtered.push(record);
    }
  }
  // if any "dangling" non-merge events were found, add them as well; they
  // can be simply appended because their order was not disrupted
  if(nonMergeEvents) {
    filtered.push(...nonMergeEvents);
  }
  return filtered;
}
