/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _blocks = require('./blocks');
const _cache = require('./cache');
const _continuityConstants = require('./continuityConstants');
const bedrock = require('bedrock');
const {util: {BedrockError}} = bedrock;
const logger = require('./logger');

const api = {};
module.exports = api;

bedrock.events.on('bedrock.init', () => {
  const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
  const {mergeEvents: {maxEvents}} = _continuityConstants;
  // this limit must be large enough to ensure at least one entire merge event
  // can be retrieved when starting after a valid `blockEventCount`
  if(gossip.maxEvents <= 0 || gossip.maxEvents < maxEvents) {
    throw new Error(
      'Gossip configuration is invalid; "gossip.maxEvents" ' +
      `(${gossip.maxEvents}) must be a positive number that is greater than ` +
      'or equal to continuity constants "mergeEvents.maxEvents" ' +
      `(${maxEvents}).`);
  }
});

// returns a partition of the DAG for a (remote) peer with the given
// remoteInfo: {peerId, peerHeads, blockHeight, blockEventCount}
api.partition = async ({ledgerNode, remoteInfo} = {}) => {
  const result = {
    blockHeight: 0,
    blockHash: null,
    consensusEvents: {
      blocks: []
    },
    nonConsensusEvents: {
      eventHash: []
    },
    totalEvents: 0,
    cursor: null
  };

  logger.verbose('Start history.partition');
  const timer = new _cache.Timer();
  timer.start({name: 'partition', ledgerNodeId: ledgerNode.id});

  // the remote peer's declared `blockHeight` is the `blockHeight` it knows
  // about; but it may be a partial block if `blockEventCount` is also given;
  // determine the remote peer's next `blockHeight` based on this information
  let nextRemoteBlockHeight = remoteInfo.blockHeight;
  if(remoteInfo.blockEventCount === undefined) {
    nextRemoteBlockHeight++;
  }

  // if the calculated `nextRemoteBlockHeight` is before the local peer's
  // next `blockHeight`, then first do a fast lookup against consensus history
  // to get ordered events for catch-up
  const nextBlockInfo = await _blocks.getNextBlockInfo({ledgerNode});
  result.blockHeight = nextBlockInfo.blockHeight - 1;
  result.blockHash = nextBlockInfo.previousBlockHash;
  if(nextRemoteBlockHeight < nextBlockInfo.blockHeight) {
    const consensusHistory = await api._getConsensusHistory({
      ledgerNode, remoteInfo,
      minBlockHeight: nextRemoteBlockHeight,
      maxBlockHeight: result.blockHeight
    });
    result.totalEvents = consensusHistory.eventCount;
    result.consensusEvents.blocks = consensusHistory.blocks;
    result.cursor = consensusHistory.cursor;
    // if there is more consensus history, return early
    if(result.cursor) {
      timer.stop();
      return result;
    }
  }

  // no more consensus history to return, so get non-consensus history
  const nonConsensusHistory = await api._getNonConsensusHistory({
    ledgerNode, remoteInfo, minBlockHeight: nextBlockInfo.blockHeight
  });
  result.nonConsensusEvents.eventHash = nonConsensusHistory.eventHash;
  result.cursor = {
    // include any specified `blockHeight` and `blockEventCount` from the
    // remote peer to ensure consensus events continue to be skipped until the
    // remote peer can make the next block
    blockHeight: remoteInfo.blockHeight,
    blockEventCount: remoteInfo.blockEventCount,
    ...nonConsensusHistory.cursor
  };
  result.totalEvents = nonConsensusHistory.eventHash.length;
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

api._getNonConsensusHistory = async function({
  ledgerNode, remoteInfo, minBlockHeight
} = {}) {
  const result = {
    eventHash: [],
    cursor: null
  };

  // FIXME: remember to move `meta.gossipOrder` into `meta.continuity2017`

  // get the head information for the peer heads referenced by the remote
  // peer in `peerHeads`; if the peer heads aren't known to the local peer,
  // they cannot be used to filter non-consensus history and they are ignored
  const {knownHeads} = await api._getKnownPeerHeads(
    {ledgerNode, peerHeads: remoteInfo.peerHeads});

  // fetch non-consensus event summary info up to `maxEvents` where
  // `localEventNumber >= ` remote peer provided number, defaulting to 0
  const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
  let limit = gossip.maxEvents;
  const records = await api._getSortedNonConsensusEventSummaries({
    ledgerNode,
    // pass `minBlockHeight` to ensure we include any events that are
    // concurrently reaching consensus and being written non-atomically as
    // these events would otherwise be erroneously skipped
    minBlockHeight,
    minLocalEventNumber: remoteInfo.localEventNumber,
    // add one to `limit` to enable detection of more non-consensus events
    limit: limit + 1
  });
  if(records.length === 0) {
    // no records are available, so keep `cursor` the same based on what was
    // requested by the peer and indicate no other events are available
    result.cursor = {
      basisBlockHeight: remoteInfo.basisBlockHeight,
      localEventNumber: remoteInfo.localEventNumber || 0,
      hasMore: false
    };
    return result;
  }

  // store whether or not there is more non-consensus history based on whether
  // the limit was breached (the query can return at most `1` more record than
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
      basisBlockHeight: remoteInfo.basisBlockHeight,
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
  const {acceptableCount, truncated, cursorRecord} = _getAcceptableHistory({
    records: filtered, limit, basisBlockHeight: remoteInfo.basisBlockHeight
  });
  // set `cursor` based on `cursorRecord`
  const {
    event: {basisBlockHeight},
    meta: {continuity2017: {localEventNumber}}
  } = cursorRecord;
  if(truncated) {
    // since we have the information available, use the next record to
    // determine what to send in the cursor
    result.cursor = {
      basisBlockHeight,
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
      basisBlockHeight: remoteInfo.basisBlockHeight,
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

// FIXME: move to bedrock-ledger-consensus-continuity-storage?
api._findNewForkers = async function({
  ledgerNode, forkNumberMap, explain = false
} = {}) {
  // for all forker numbers that are not `null`, we must check to see if a
  // forker has been detected via the block to be created by comparing against
  // events that have previously reached consensus with different fork numbers
  const $or = [];
  for(const [creator, forkNumber] of forkNumberMap) {
    if(forkNumber === null) {
      // not a fork
      continue;
    }
    // query for a creator that has any event that has reached consensus with
    // a different fork number than the one given; if found, it is a new forker
    $or.push({
      'meta.continuity2017.creator': creator,
      'meta.continuity2017.type': 'm',
      'meta.continuity2017.forkNumber': {$ne: forkNumber}
    });
  }

  if($or.length === 0) {
    // no new forkers
    return [];
  }

  // FIXME: make this a covered query
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    {
      $match: {$or}
    },
    // group by creator to return just `creator`
    {
      $group: {_id: '$meta.continuity2017.creator'}
    },
    // limit number of results to number of peers
    {
      $limit: $or.length
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  const records = await cursor.toArray();
  return records.map(({_id}) => _id);
};

api._getConsensusHistory = async function({
  ledgerNode, remoteInfo, minBlockHeight, maxBlockHeight
} = {}) {
  const result = {
    blocks: [],
    cursor: null,
    eventCount: 0
  };

  // get maximum number of sorted consensus event summaries
  const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
  const limit = gossip.maxEvents;
  const records = await api._getSortedConsensusEventSummaries({
    ledgerNode,
    minBlockHeight,
    minGossipOrder: remoteInfo.blockEventCount,
    // pass a `maxBlockHeight` to avoid finding blocks that are concurrently
    // reaching consensus and being written non-atomically
    maxBlockHeight,
    // add one to `limit` to enable detection of more consensus events
    limit: limit + 1
  });
  if(records.length === 0) {
    // no results
    return result;
  }

  /* Note: The last event we return to the remote peer must be a merge event
  and it must have a `basisBlockHeight` that is <= the one the remote peer
  declared. If we found fewer records than the gossip max event limit, the
  query is guaranteed to end in a merge event unless the remote peer made an
  improper request, or there is database corruption. Here we always roll back
  until we find a terminating merge event with an appropriate
  `basisBlockHeight`. If we started with some records and roll back all the way
  to zero, that means either the very next event requires a greater
  `basisBlockHeight` or the remote peer made an improper request. Either way,
  we return what must be sent in `cursor` to continue receiving events. If we
  did find a trailing merge event with a valid `basisBlockHeight`, then we save
  the information for the record that comes after it so the remote peer can
  continue in another request. */
  const {acceptableCount, truncated, cursorRecord} = _getAcceptableHistory({
    records, limit, basisBlockHeight: remoteInfo.basisBlockHeight
  });
  // initialize `cursor` based on `cursorRecord`
  result.cursor = {
    basisBlockHeight: cursorRecord.event.basisBlockHeight,
    blockHeight: cursorRecord.meta.blockHeight,
    blockEventCount: cursorRecord.meta.gossipOrder,
    // the local peer always has more events here because it necessarily has
    // non-consensus events that were used to determine the consensus events
    hasMore: true
  };
  if(!truncated) {
    /* Note: Since the results were not truncated, we've run out of consensus
    events to send and don't know what the next (non-consensus) event will be.
    We're at the end of a block so we can safely keep the `blockHeight`
    parameter and increment the seen `blockEventCount`, but must use the
    remote peer's declared `basisBlockHeight` since we do not know what the
    next `basisBlockHeight` will be. When the remote peer makes their next
    request, the entire block will be skipped leading to either: the next block
    to be sent (if was created in the meantime) or no consensus records found
    resulting in a non-consensus query being performed. If the remote peer's
    `basisBlockHeight` not high enough in either case for the next event, then
    the local peer will respond with a new cursor that indicates the required
    `basisBlockHeight`.
    */
    result.cursor.basisBlockHeight = remoteInfo.basisBlockHeight;
    result.cursor.blockEventCount++;
  }
  if(acceptableCount === 0) {
    // truncated entirely, return early
    return result;
  }
  result.eventCount = acceptableCount;

  // combine results by block height
  const resultMap = new Map();
  let lastResult;
  const blockHeights = [];
  for(let i = 0; i < acceptableCount; ++i) {
    const {blockHeight, eventHash} = records[i].meta;
    if(lastResult && lastResult.blockHeight === blockHeight) {
      // add event hash to the previous result
      lastResult.eventHash.push(eventHash);
    } else {
      // create a new result for this block height
      blockHeights.push(blockHeight);
      resultMap.set(blockHeight, lastResult = {
        blockHeight,
        blockHash: null,
        eventHash: [eventHash]
      });
    }
  }
  // return block hashes for each block height
  if(acceptableCount > 0) {
    const blockRecords = await api._getBlockHashes({
      ledgerNode, blockHeights: [...resultMap.keys()]
    });
    for(const record of blockRecords) {
      resultMap.get(record.block.blockHeight).blockHash =
        record.meta.blockHash;
    }
  }
  result.blocks = [...resultMap.values()];
  return result;
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
api._getSortedConsensusEventSummaries = async function({
  ledgerNode, minBlockHeight, minGossipOrder = 0, maxBlockHeight,
  limit = 100, explain = false
} = {}) {
  // FIXME: make this a covered query
  const query = {
    'meta.blockHeight': {
      // by specifying a range, we ensure that no events that are actively
      // reaching consensus during a non-atomic block write operation get
      // included in the results
      $gte: minBlockHeight, $lte: maxBlockHeight
    }
  };
  const projection = {
    _id: 0,
    'meta.blockHeight': 1,
    'meta.gossipOrder': 1,
    'event.basisBlockHeight': 1,
    'meta.continuity2017.type': 1,
    'meta.eventHash': 1
  };
  const {collection} = ledgerNode.storage.events;
  const cursor = await collection.find(query, {projection})
    .sort({
      'meta.blockHeight': 1,
      'meta.gossipOrder': 1
    })
    // effectively skips any declared `blockEventCount`
    .skip(minGossipOrder)
    .limit(limit);
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.toArray();
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
// gets block hashes for the given block heights
api._getBlockHashes = async function({
  ledgerNode, blockHeights, explain = false
} = {}) {
  const query = {
    'block.blockHeight': {$in: blockHeights}
  };
  const projection = {
    _id: 0,
    'block.blockHeight': 1,
    'meta.blockHash': 1
  };
  const {collection} = ledgerNode.storage.blocks;
  const cursor = await collection.find(query, {projection});
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.toArray();
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
api._getSortedNonConsensusEventSummaries = async function({
  ledgerNode, minBlockHeight, minLocalEventNumber = 0,
  limit = 100, explain = false
} = {}) {
  const cursor = ledgerNode.storage.events.collection.find({
    // ensure we search both for events that have not reached consensus
    // and any that may be concurrently reaching consensus via a non-atomic
    // database update; we use `meta.blockHeight: null` to avoid having to
    // include `consensus` in the index used for this query
    'meta.continuity2017.localEventNumber': {$gte: minLocalEventNumber},
    $or: [
      {'meta.blockHeight': null},
      {'meta.blockHeight': {$gte: minBlockHeight}}
    ]
  }, {
    projection: {
      _id: 0,
      'meta.continuity2017.localEventNumber': 1,
      'meta.continuity2017.type': 1,
      'meta.continuity2017.creator': 1,
      'meta.continuity2017.forkNumber': 1,
      'meta.continuity2017.generation': 1,
      'event.basisBlockHeight': 1,
      'event.parentHash': 1,
      'meta.eventHash': 1
    }
  }).sort({'meta.continuity2017.localEventNumber': 1}).limit(limit)
    // ensure the index that is designed for this query is used; without
    // providing this hint it seemed that mongo insisted on doing something
    // more complicated, involving generating its own sort keys, etc.
    .hint('event.continuity2017.localEventNumber');
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
};

// given a set of heads from an untrusted peer, returning which ones are
// known to be valid, the rest having an unknown status and will be ignored;
// `peerHeads` is an array with event hashes identifying the heads
api._getKnownPeerHeads = async ({ledgerNode, peerHeads} = {}) => {
  // no heads given, return immediately
  if(peerHeads.size === 0) {
    return {knownHeads: []};
  }

  // FIXME: simplify protocol to simply accept event hashes
  const headMap = new Map();
  const eventHashes = [];
  for(const {creator, heads} of peerHeads) {
    for(const head of heads) {
      // if any peer heads are duplicates, throw
      const {eventHash} = head;
      if(headMap.has(eventHash)) {
        throw new BedrockError(
          'Multiple peer heads with the same event hash were given.',
          'DuplicateError', {
            httpStatusCode: 400,
            public: true,
            eventHash
          });
      }
      eventHashes.push(eventHash);
      headMap.set(eventHash, {creator, ...head});
    }
  }

  // FIXME: consider moving to `bedrock-ledger-consensus-continuity-storage`
  // FIXME: make sure this is a covered query
  // FIXME: we could also limit the search by blockHeight and consensus=false
  // ... determine if that would help/hurt performance
  const records = await ledgerNode.storage.events.collection.find({
    'meta.eventHash': {$in: eventHashes},
    'meta.continuity2017.type': 'm'
  }).project({
    _id: 0,
    'meta.eventHash': 1,
    'meta.continuity2017.creator': 1,
    'meta.continuity2017.generation': 1,
    'meta.continuity2017.forkNumber': 1
  }).toArray();
  const knownHeads = [];
  for(const {meta} of records) {
    const {
      eventHash,
      continuity2017: {creator, generation, forkNumber = null}
    } = meta;
    const head = headMap.get(eventHash);
    if(head.generation === generation && head.creator === creator) {
      knownHeads.push({...head, forkNumber});
    }
  }
  return {knownHeads};
};

// returns history that the remote peer can accept; note that this API assumes
// that `records.length > 0`
function _getAcceptableHistory({records, limit, basisBlockHeight}) {
  // `limit` is necessarily larger than 1, meaning that if the limit is hit,
  // we will always properly check for a trailing merge event in the loop below
  // if any events have an acceptable `basisBlockHeight`
  const maxRecords = Math.min(limit, records.length);

  /* Note: `records` may contain some records that have a greater
  `basisBlockHeight` than the remote peer will accept. We can't limit the query
  to find these records to what the remote peer accepts because then we
  wouldn't know if there is more history to return after it. Also, events that
  have reached consensus may be spread across multiple blocks and a block may
  start with an event with a lower `basisBlockHeight` than an event that
  occurred in a prior block. Therefore, we must walk forward to limit the
  number of records by `basisBlockHeight` before walking backwards. */
  let acceptableCount;
  for(acceptableCount = 0; acceptableCount < maxRecords; ++acceptableCount) {
    if(records[acceptableCount].event.basisBlockHeight > basisBlockHeight) {
      break;
    }
  }

  // walk backwards to find acceptable trailing merge event
  while(acceptableCount > 0) {
    /* Note: This works because the ordering algorithm used on `records` causes
    non-merge events (e.g., operation or config events) to be inserted just
    prior to their child merge event instead of spreading them out
    breadth-first like merge events are. Therefore we should never have ordered
    non-consensus events or a consensus block with an order like this:
    [r1,r2,m1,m2] as that would create a problem if we truncated after `m1`
    since `r2` would have no merge event transferred along with it. But the
    ordering algorithm used results in this ordering instead: `[r1,m1,r2,m2]`,
    so when we truncate after `m1` there's nothing that comes before it that is
    incomplete. */
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
      // a forker has two or more merge events that share the same non-merge
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
  and `forkNumber` information to determine if we can short-circuit or not. For
  heads of forkers, this may be inefficient, resulting in hashes being sent to
  the remote peer that it actually has, which is not a protocol violation. It's
  up to the remote peer to compare on its end and decide what to request or if
  it should ask for more hashes using the `cursor` information provided by the
  local peer to continue (the local peer will send its `localEventNumber` to
  enable continuation even when no heads or heads of forkers are given). */
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
        continuity2017: {creator, generation, forkNumber = null}
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
          // if fork number matches and generation is <= than an end head, then
          // the event should not be returned in this partition of history
          if(forkNumber === head.forkNumber && generation <= head.generation) {
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
