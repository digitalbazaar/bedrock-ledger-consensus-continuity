/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

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

api.getGenesisHead = async ({ledgerNode}) => {
  const {getHead} = ledgerNode.storage.events.plugins['continuity-storage'];
  const [{meta}] = await getHead({generation: 0});
  if(!meta) {
    throw new BedrockError(
      'The genesis merge event was not found.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }
  // generation, basisBlockHeight, mergeHeight, localAncestorGeneration for
  // genesis are always zero
  return {
    eventHash: meta.eventHash,
    generation: 0,
    basisBlockHeight: 0,
    mergeHeight: 0,
    localAncestorGeneration: 0
  };
};

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
    next: null
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
    const consensusEvents = await api._getConsensusEvents({
      ledgerNode,
      basisBlockHeight: remoteInfo.basisBlockHeight,
      blockHeight: nextRemoteBlockHeight,
      blockEventCount: remoteInfo.blockEventCount,
      maxBlockHeight: result.blockHeight
    });
    result.totalEvents = consensusEvents.eventCount;
    result.consensusEvents.blocks = consensusEvents.blocks;
    result.next = consensusEvents.next;
    // if there is more consensus history, return early
    if(result.next) {
      timer.stop();
      return result;
    }
  }

  // no more consensus history to return, so get non-consensus history...

  // prepare to do a breadth-first graph search that will start at the
  // local peer's non-consensus heads and end at the remote peer's
  // non-consensus heads; we must inspect the `peerHeads` from the remote
  // peer to determine which are valid; we cannot assume they are valid
  // valid as they could come from an untrusted peer
  const [startHeads, {validHeads: endHeads}] = await Promise.all([
    api._getNonConsensusHeads({ledgerNode}),
    api._inspectPeerHeads({ledgerNode, peerHeads: remoteInfo.peerHeads})
  ]);

  /* Note: For the end head for the remote peer, we use the latest
  non-consensus head that the local peer knows about -- we remove it from the
  `startHeads` and add it to the `endHeads`. This is for two reasons: there's
  no reason that a well-behaved remote peer needs to send its own head to the
  remote peer because the local peer can safely assume the remote peer has at
  least the latest head the local peer has. Secondly, if the remote peer is a
  forker, then by using only the highest generation head known to the local
  peer here as its end head, the forker will receive gaps in the DAG if the
  lesser generation fork has been merged by other peers, thus disrupting the
  forker's ability to gossip. If the lesser generation is not merged by other
  peers, then it isn't actually a meaningful fork and the forker is
  indistinguishable from a non-forker. */
  for(let i = 0; i < startHeads.length; ++i) {
    const startHead = startHeads[i];
    if(startHead.creator === remoteInfo.peerId) {
      endHeads.push(startHead);
      startHeads.splice(i, 1);
      // FIXME: once `forkId` is implemented, clear `forkId` for `startHead` to
      // ensure that it stops all forks at the given generation
      break;
    }
  }

  // FIXME: need to address the issue of potential forks; we need to ensure
  // that all non-forked peers can incrementally download events without
  // getting stuck (ideally forked peers actually DO get stuck, thus
  // discouraging them from forking) receiving the same history and without
  // returning holes in the DAG
  // ... we must ensure that if an attacker creates a fork with a line
  // of merges that is longer than the truncation limit that a valid
  // peer will still be able to make progress downloading events, i.e.,
  // the same set of truncated events won't always be returned to them
  // FIXME: note that if a long fork line is not ever merged by a valid
  // witness, it cannot be important for consensus and if it is merged by
  // a valid witness, then eventually we will have a head for that witness
  // that could be used to prevent having to send the hashes for a long fork
  // line again; however, to get that witness head, we have to download the
  // long fork line to begin with -- all which implies having to send multiple
  // heads, one per fork, so far this seems unavoidable; also, the server may
  // receive unseen peer heads and must therefore drop them as we can't
  // validate them or they could be forks that we know about that the client
  // does not; this means forking creates inefficiencies in the gossip
  // protocol, which is undesirable. We need to include fork information in
  // our stopping conditions ... so any event that has {$lte: generation}, AND
  // that has a fork ID that is in the given fork ID list ... that would
  // guarantee that it was an ancestor. That should work once we have fork ID
  // implemented.

  /* Note: Although `peerHeads` have been validated as internally consistent,
  they may still refer to events that occurred before the remote peer's
  declared known `blockHeight` if the remote peer is not following protocol. At
  this point we've determined that the declared `blockHeight` is the same as
  the local peer's so we're only dealing with events that have not reached
  consensus on the local peer. Therefore, the query here ensures we always stop
  searching at any event that has not achieved consensus so we do not rely
  on the `peerHeads` to prevent the search from going back too far. The
  `peerHeads` can only make search even shorter than the maximum permitted
  depth. This prevents remote peers that aren't following protocol from
  causing us to search too far back into the graph. */

  /* Note: This code will limit the maximum number of events examined to
  those that had not reached consensus at the time this function was called,
  i.e., using the `blockHeight` for the next block retrieved above. Note that
  after this block height was retrieved, N blocks could have concurrently been
  made by another process, so in order to ensure we do not return a partition
  of the DAG that has gaps in it, we limit our search not just to non-consensus
  events but to any consensus events we find at or after that block height.
  On a fast moving network, this could be several blocks, but it is unlikely
  that if it several blocks that they would be very large. However, without
  protocol rules in place to ensure that the set of non-consensus events cannot
  grow too large, which also maps to block size, this implementation could walk
  more events than would be desirable in a single call. If significantly large
  blocks become a problem, a future change will be required to reduce
  processing time to something more manageable. */
  const history = await api._aggregateHistory({
    ledgerNode, minBlockHeight: nextBlockInfo.blockHeight, startHeads, endHeads
  });

  // if there is no history, return early
  if(history.length === 0) {
    timer.stop();
    return result;
  }

  // sort history
  const sortedMap = _sortHistory(history);
  const records = [...sortedMap.values()];

  // truncate history to permissible `basisBlockHeight` and number of event
  // hashes to be returned
  const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
  // FIXME: it's not clear what the smallest this limit could be is; since
  // a remote peer with a peer head that is *ahead* of the local peer causes
  // the local peer to act as if the head wasn't given at all... since it
  // can't know if it's a fork or not; this needs to examined for improvement
  const limit = gossip.maxEvents + 1;
  const recordCount = _truncateHistory({
    records, limit, basisBlockHeight: remoteInfo.basisBlockHeight,
    consensus: false
  });
  // if the record count is less than what was returned, then return
  // `basisBlockHeight` for the following record so that the client can
  // continue when it is ready to make another request
  if(recordCount < records.length) {
    const {event: {basisBlockHeight}} = records[recordCount];
    result.next = {basisBlockHeight};
    records.length = recordCount;
  }
  if(recordCount > 0) {
    const eventHashes = [];
    for(const record of records) {
      eventHashes.push(record.meta.eventHash);
    }
    result.nonConsensusEvents.eventHash = eventHashes;
    result.totalEvents = eventHashes.length;
  }

  timer.stop();
  // FIXME: should we return `startHeads` ... is that valuable?
  return result;
};

// given a set of heads from an untrusted peer, returning which ones are
// known to be valid, the rest having an unknown status
// here `peerHeads` is an array with {creator, heads} entries
api._inspectPeerHeads = async ({ledgerNode, peerHeads} = {}) => {
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
  // FIXME: we could also limit the search by blockHeight OR consensus=false
  // ... determine if that would help/hurt performance
  // FIXME: whenever we can't find the given heads, we currently act as if the
  // client knows *nothing* about that creator -- we could probably do better,
  // for example, if the client sent `treeHash` or more than one head per
  // creator (like last 2-3 generations) then we could go one or two
  // generations back which is probably the common case ... and avoid sending
  // a lot of hashes -- but what is better ... the client sending a little
  // more always or the server sending a lot more sometimes?
  const records = await ledgerNode.storage.events.collection.find({
    'meta.eventHash': {$in: eventHashes},
    'meta.continuity2017.type': 'm'
  }).project({
    _id: 0,
    'meta.eventHash': 1,
    'meta.continuity2017.creator': 1,
    'meta.continuity2017.generation': 1
    // FIXME: in order to handle forks, we need to return the fork ID here
    // as well -- and append that to the head information so we can do
    // a proper graphLookup with it
  }).toArray();
  const validHeads = [];
  for(const {meta} of records) {
    const {eventHash, continuity2017: {creator, generation}} = meta;
    const head = headMap.get(eventHash);
    if(head.generation === generation && head.creator === creator) {
      validHeads.push(head);
    }
  }
  return {validHeads};
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
api._aggregateHistory = async function({
  ledgerNode, minBlockHeight, startHeads, endHeads = [], explain = false
} = {}) {
  // use `restrictSearchWithMatch` to apply stopping points for the graph
  // lookup query; these are always on "merge event boundaries" and they are
  // based on:
  // 1. `minBlockHeight` - Stop at any merge event that reached consensus
  //   before the given block height.
  // 2. `endHeads` - Stop at any merge event with an event hash that matches
  //   the given head information.
  const restrictSearchWithMatch = {
    $nor: [{
      'meta.blockHeight': {$exists: true, $lt: minBlockHeight}
    }]
  };
  for(const endHead of endHeads) {
    // all head fields used here must have been previously validated for
    // internal consistency (a head's `creator` and `generation` correctly
    // match up with its `eventHash`)
    restrictSearchWithMatch.$nor.push({
      'meta.continuity2017.type': 'm',
      'meta.continuity2017.creator': endHead.creator,
      'meta.continuity2017.generation': {$lte: endHead.generation}
      // FIXME: in order to properly handle forks, we need to include
      // `{$in: endHead.forkId}` as well
    });
  }

  // start heads are presumed valid and in the database
  const startEventHashes = [];
  for(const head of startHeads) {
    startEventHashes.push(head.eventHash);
  }
  const {collection} = ledgerNode.storage.events;
  const pipeline = [
    {$match: {
      'meta.eventHash': {$in: startEventHashes},
      'meta.continuity2017.type': 'm'
    }},
    {$group: {
      _id: null,
      startWith: {$addToSet: '$meta.eventHash'}
    }},
    {$graphLookup: {
      from: collection.collectionName,
      startWith: '$startWith',
      connectFromField: 'event.parentHash',
      connectToField: 'meta.eventHash',
      as: '_parents',
      restrictSearchWithMatch
    }},
    {$project: {
      _id: 0,
      '_parents.meta.eventHash': 1,
      '_parents.meta.continuity2017.type': 1,
      '_parents.event.parentHash': 1,
      '_parents.event.basisBlockHeight': 1
    }},
    {$unwind: '$_parents'},
    {$replaceRoot: {newRoot: '$_parents'}},
    // FIXME: can we easily sort by `basisBlockHeight` here? and remove any
    // records >= a basisBlockHeight + 1 (need `+1` to be able to notify the
    // client of what is next)
  ];
  const cursor = await collection.aggregate(
    pipeline, {allowDiskUse: true});
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.toArray();
};

api._getConsensusEvents = async function({
  ledgerNode, basisBlockHeight, blockHeight, blockEventCount, maxBlockHeight
} = {}) {
  const result = {
    blocks: [],
    next: null,
    eventCount: 0
  };

  // ensure there's always an additional record that can be used to determine
  // if we truncated the results
  const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
  // add one to `maxEvents` to enable us to detect if there is another full
  // merge event available for download after we've hit the limit; we also
  // pass a `maxBlockHeight` to avoid finding blocks that are concurrently
  // reaching consensus and being written non-atomically
  const limit = gossip.maxEvents + 1;
  const records = await api._getConsensusHistory({
    ledgerNode,
    minBlockHeight: blockHeight,
    minGossipOrder: blockEventCount,
    maxBlockHeight,
    limit
  });
  if(records.length === 0) {
    // no results
    return result;
  }

  /* Note: The last event we return to the client must be a merge event and
  it must have a `basisBlockHeight` that is <= the one the client declared. If
  we are under the gossip max event limit (one less than `limit`), the query
  is guaranteed to end in a merge event unless the client made an improper
  request, or there is database corruption. Here we always roll back until we
  find a terminating merge event with an appropriate `basisBlockHeight`. If we
  started with some records and roll back all the way to zero, that means
  either the very next event requires a greater `basisBlockHeight` or the
  client made an improper request. Either way, we return what must be sent
  in `next` to continue receiving events. If we did find a trailing merge
  event with a valid `basisBlockHeight`, then we save the information for the
  record that comes after it so the client can continue in another request. */
  const recordCount = _truncateHistory({
    records, limit, basisBlockHeight, consensus: true
  });
  // get the record to use to populate `next` so the client can continue from
  // where it left off with the next request; if the record count is less than
  // what was returned, then use the record right after truncation otherwise
  // use the very last record to be sent (and see subsequent note about
  // incrementing `blockEventCount`)
  const truncated = recordCount < records.length;
  const nextRecord = truncated ?
    records[recordCount] : records[recordCount - 1];
  result.next = {
    basisBlockHeight: nextRecord.event.basisBlockHeight,
    blockHeight: nextRecord.meta.blockHeight,
    blockEventCount: nextRecord.meta.gossipOrder
  };
  if(recordCount === 0) {
    // truncated entirely, return early
    return result;
  }
  // if not truncated, then we're at the end of a block and can safely
  // increment the seen `blockEventCount` whilst maintaining the other `next`
  // parameters such that when the client makes their next request, the
  // entire block will be skipped leading to either: the next block to be sent
  // (if was created in the meantime) or no consensus records found resulting
  // in a non-consensus query being performed
  if(!truncated) {
    result.next.blockEventCount++;
  }
  result.eventCount = recordCount;

  // combine results by block height
  const resultMap = new Map();
  let lastResult;
  const blockHeights = [];
  for(let i = 0; i < recordCount; ++i) {
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
  if(recordCount > 0) {
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
api._getConsensusHistory = async function({
  ledgerNode, minBlockHeight, minGossipOrder = 0, maxBlockHeight,
  limit = 1000, explain = false
} = {}) {
  // FIXME: make this a covered query
  const query = {
    'meta.blockHeight': {
      // by specifying a range, we ensure that no events that are actively
      // reaching consensus during a non-atomic block write operation get
      // included in the results
      $exists: true, $gte: minBlockHeight, $lte: maxBlockHeight
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

/**
 * Gets all non-consensus heads.
 *
 * @param {integer} [limit=1000] the maximum number of heads to return.
 * @param {boolean} [explain=false] return statistics for query profiling.
 *
 * @returns {Promise} the head information.
 */
// FIXME: move to bedrock-ledger-consensus-continuity-storage
api._getNonConsensusHeads = async ({
  ledgerNode, limit = 1000, explain = false
} = {}) => {
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    // find non-consensus merge events
    {
      $match: {
        'meta.consensus': false,
        'meta.continuity2017.type': 'm'
      }
    },
    // sort by generation
    {
      $sort: {'meta.continuity2017.generation': -1}
    },
    // group by creator ID to make them distinct and return
    // needed head information; only the highest generation for a creator
    // that has forked will be returned; as long as a shorter generation fork
    // gets merged, it will be found in the subsequent graph lookup
    {
      $group: {
        _id: '$meta.continuity2017.creator',
        // can safely use `$first` here because we sorted by generation already
        eventHash: {$first: '$meta.eventHash'},
        generation: {$first: '$meta.continuity2017.generation'}
      }
    },
    // limit number of distinct heads to
    {
      $limit: limit
    },
    // map `_id` to `creator`
    {
      $project: {_id: 0, creator: '$_id', eventHash: 1, generation: 1}
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
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

function _sortHistory(history) {
  // topologically sort history to ensure parents appear before children in
  // the result, that regular events appear just prior to the merge events
  // that merge them in, and such that lowest `basisBlockHeight` is first
  const sortedMap = new Map();

  const hashMap = new Map();
  const mergeEvents = [];
  for(const record of history) {
    hashMap.set(record.meta.eventHash, record);
    if(record.meta.continuity2017.type === 'm') {
      mergeEvents.push(record);
    }
  }

  // FIXME: we could have the database do this sort using its index
  // first sort merge events by `basisBlockHeight`
  mergeEvents.sort(_compareBasisBlockHeight);

  // split into `basisBlockHeight` groups to be sorted topologically
  const chunks = [];
  let lastChunk;
  for(const record of mergeEvents) {
    if(lastChunk) {
      const {event: {basisBlockHeight}} = record;
      const {event: {basisBlockHeight: lastBasisBlockHeight}} = lastChunk[0];
      if(lastBasisBlockHeight === basisBlockHeight) {
        lastChunk.push(record);
        continue;
      }
    }
    chunks.push(lastChunk = [record]);
  }

  // sort each chunk topologically, thus preserving `basisBlockHeight` order
  for(const chunk of chunks) {
    let next = chunk;
    while(next.length > 0) {
      const current = next;
      next = [];
      for(const record of current) {
        let defer = false;
        for(const parentHash of record.event.parentHash) {
          const parent = hashMap.get(parentHash);
          if(!(parent && parent.meta.continuity2017.type === 'm')) {
            // parent is not in the history or not a merge event, skip it
            continue;
          }
          // if parent hasn't been sorted yet, defer
          if(!sortedMap.has(parent.meta.eventHash)) {
            defer = true;
            break;
          }
        }
        if(defer) {
          next.push(record);
          continue;
        }
        // if the event has non-merge event parents, add them just before the
        // merge event to facilitate simpler truncation if limits are hit
        for(const parentHash of record.event.parentHash) {
          const parent = hashMap.get(parentHash);
          if(parent && parent.meta.continuity2017.type !== 'm') {
            sortedMap.set(parent.meta.eventHash, parent);
          }
        }
        sortedMap.set(record.meta.eventHash, record);
      }
    }
  }

  return sortedMap;
}

function _compareBasisBlockHeight(a, b) {
  return a.event.basisBlockHeight - b.event.basisBlockHeight;
}

function _truncateHistory({records, limit, basisBlockHeight, consensus}) {
  let maxRecords = records.length;
  if(maxRecords >= limit) {
    // if limit was reached, we do not return the last record, as it is only
    // present to determine whether or not there is more to request as well as
    // the parameters (such as required `basisBlockHeight`) for requesting it;
    // note that an even earlier record may be used to provide this information
    // instead (depending on where the trailing merge event is); also note that
    // `limit` is also necessarily larger than 1, meaning that if the limit is
    // hit, we will still check for a trailing merge event in the loop below
    maxRecords = limit - 1;
  }
  // if the records are for events that have reached consensus, then they
  // may be spread across multiple blocks and a block may start with an event
  // with a lower `basisBlockHeight` than an event that occurred in a prior
  // block; therefore, we must walk forward to find our starting point before
  // rewinding
  let recordCount;
  if(!consensus) {
    recordCount = maxRecords;
  } else {
    for(recordCount = 0; recordCount < maxRecords; ++recordCount) {
      if(records[recordCount].event.basisBlockHeight > basisBlockHeight) {
        break;
      }
    }
  }
  // walk backwards to find acceptable trailing merge event
  while(recordCount > 0) {
    /* Note: This works because the ordering algorithm used on `records` causes
    regular events to be inserted just prior to their child merge event instead
    of spreading them out breadth-first like merge events are. Therefore we
    should never have sorted non-consensus or a consensus block with an order
    like this: [r1,r2,m1,m2] as that would create a problem if we truncated
    after `m1` since `r2` would have no merge event transferred along with it.
    But the ordering algorithm used results in this ordering instead:
    `[r1,m1,r2,m2]`, so when we truncate after `m1` there's nothing that comes
    before it that is incomplete. */
    const {
      event: {basisBlockHeight: eventBasisBlockHeight},
      meta: {continuity2017: {type}},
    } = records[recordCount - 1];
    // must check `eventBasisBlockHeight` for case where `consensus=false`
    if(type === 'm' && eventBasisBlockHeight <= basisBlockHeight) {
      // acceptable trailing merge event found, break
      break;
    }
    recordCount--;
  }
  return recordCount;
}
