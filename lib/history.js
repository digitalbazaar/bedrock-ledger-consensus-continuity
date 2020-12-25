/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const _cache = require('./cache');
const bedrock = require('bedrock');
const {util: {BedrockError}} = bedrock;
const logger = require('./logger');

const api = {};
module.exports = api;

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

// FIXME rename `blockOrder` to `eventIndex` in the protocol (not in the db)
// returns a partition of the DAG for a (remote) peer with the given
// remoteInfo: {peerId, peerHeads, blockHeight, eventIndex}
api.partition = async ({ledgerNode, remoteInfo} = {}) => {
  const result = {
    blockHeight: 0,
    blockHash: null,
    consensusEvents: {
      blocks: [],
      next: false
    },
    nonConsensusEvents: {
      eventHash: [],
      truncated: false
    },
    totalEvents: 0
  };

  logger.verbose('Start history.partition');
  const timer = new _cache.Timer();
  timer.start({name: 'partition', ledgerNodeId: ledgerNode.id});

  // the remote peer's declared `blockHeight` is the `blockHeight` it knows
  // about; but it may be a partial block if `eventIndex` is also given;
  // determine the remote peer's next `blockHeight` based on this information
  let nextRemoteBlockHeight = remoteInfo.blockHeight;
  if(remoteInfo.eventIndex === undefined) {
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
      minBlockHeight: nextRemoteBlockHeight,
      minBlockOrder: remoteInfo.eventIndex
    });
    result.totalEvents = consensusEvents.eventCount;
    result.consensusEvents = {
      blocks: consensusEvents.blocks,
      next: consensusEvents.next
    };
    // if there is more consensus history, return early
    if(result.consensusEvents.next) {
      timer.stop();
      console.log('result', result);
      return result;
    }
  }

  // no more consensus history to return, so get non-consensus history...

  // inspect `peerHeads` to determine which are valid; cannot assume they are
  // valid as they could come from an untrusted peer
  const {validHeads} = await api._inspectPeerHeads(
    {ledgerNode, peerHeads: remoteInfo.peerHeads});

  // do a breadth-first graph search starting the local peer's non-consensus
  // peer heads and going through the ancestry of non-consensus events only
  const {peerId: remotePeerId} = remoteInfo;
  const startHeads = await api._getNonConsensusPeerHeads(
    {ledgerNode, remotePeerId});

  // FIXME: need to address the issue of potential forks; we need to ensure
  // that a peer can incrementally download events without getting stuck
  // receiving the same history and without returning holes in the DAG
  // ... we must ensure that if an attacker creates a fork with a line
  // ... of merges that is longer than the truncation limit that a valid
  // ... peer will still be able to make progress downloading events, i.e.,
  // ... the same set of truncated events won't always be returned to them
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
  they may still be events that occurred before the remote peer's declared
  known `blockHeight` if the remote peer is not following protocol. At this
  point we've determined that the declared `blockHeight` is the same as the
  local peer's so we're only dealing with events that have not reached
  consensus on the local peer. Therefore, the query here ensures we always stop
  searching at any event that has not achieved consensus so we do not rely only
  on the `peerHeads` alone. This prevents remote peers that aren't following
  protocol from causing us to search too far back into the graph. */

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
    ledgerNode, minBlockHeight: nextBlockInfo.blockHeight,
    startHeads, endHeads: validHeads
  });

  // if there is no history, return early
  if(history.length === 0) {
    timer.stop();
    return result;
  }

  // sort history
  const sortedHistory = _sortHistory(history);

  // truncate history to permissible number of event hashes to be returned
  // FIXME: make `1000` configurable
  const maxEvents = 1000 - result.totalEvents;
  if(sortedHistory.length > maxEvents) {
    sortedHistory.length = maxEvents;
    result.nonConsensusEvents.truncated = true;
  }
  result.nonConsensusEvents.eventHash = sortedHistory;
  result.totalEvents += sortedHistory.length;

  timer.stop();
  // FIXME: should we return `startHeads` ... is that valuable?
  return result;
};

// given a set of heads from an untrusted peer, returning which ones are
// known to be valid, the rest having an unknown status
api._inspectPeerHeads = async ({ledgerNode, peerHeads} = {}) => {
  const headMap = new Map();
  const eventHashes = [];
  for(const head of peerHeads) {
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
    headMap.set(eventHash, head);
  }

  // FIXME: consider moving to `bedrock-ledger-consensus-continuity-storage`
  // FIXME: make sure this is a covered query
  // FIXME: we could also limit the search by blockHeight OR consensus=false
  // ... determine if that would help/hurt performance
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
  // 1. `consensus` - Stop at any merge event that reached consensus.
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
      _id: 0, '_parents.meta.eventHash': 1, '_parents.event.parentHash': 1
    }},
    {$unwind: '$_parents'},
    {$replaceRoot: {newRoot: '$_parents'}},
  ];
  const cursor = await collection.aggregate(
    pipeline, {allowDiskUse: true});
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.toArray();
};

api._getConsensusEvents = async function({
  ledgerNode, minBlockHeight, minBlockOrder = 0
} = {}) {
  const result = {
    blocks: [],
    next: false,
    eventCount: 0
  };

  // ensure there's always an additional record that can be used to determine
  // if we truncated the results
  // FIXME: make limit configurable
  const limit = 1001;
  const records = await api._getConsensusHistory(
    {ledgerNode, minBlockHeight, minBlockOrder, limit});

  // combine results by block height
  const resultMap = new Map();
  let recordCount = records.length;
  if(recordCount === limit) {
    recordCount--;
    const {meta: {blockHeight, blockOrder}} = records[recordCount];
    result.next = {blockHeight, eventIndex: blockOrder};
  }
  result.eventCount = recordCount;
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
  ledgerNode, minBlockHeight, minBlockOrder = 0, limit = 1000, explain = false
} = {}) {
  const query = {
    'meta.blockHeight': {$gte: minBlockHeight}
  };
  const projection = {
    _id: 0,
    'meta.blockHeight': 1,
    'meta.blockOrder': 1,
    'meta.eventHash': 1
  };
  const {collection} = ledgerNode.storage.events;
  const cursor = await collection.find(query, {projection})
    .sort({'meta.blockHeight': 1, 'meta.blockOrder': 1})
    .skip(minBlockOrder)
    .limit(limit);
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.toArray();
};

/**
 * Gets all non-consensus peer heads from the perspective of a remote
 * peer.
 *
 * @param {string} peerId the peer ID of the remote peer, i.e., the
 *   one to NOT get heads for.
 * @param {integer} [limit=1000] the maximum number of heads to return.
 * @param {boolean} [explain=false] return statistics for query profiling.
 *
 * @returns {Promise} the head information.
 */
// FIXME: move to bedrock-ledger-consensus-continuity-storage
api._getNonConsensusPeerHeads = async ({
  ledgerNode, remotePeerId, limit = 1000, explain = false
} = {}) => {
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    // find non-consensus events
    {
      $match: {
        'meta.continuity2017.creator': {$ne: remotePeerId},
        'meta.continuity2017.type': 'm',
        'meta.consensus': false
      }
    },
    // sort by generation
    {
      $sort: {'meta.continuity2017.generation': -1}
    },
    // group by creator ID to make them distinct and return
    // needed head information; only the highest generation for a creator
    // that has forked will be returned
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
    // map `_id` to `creatorId`
    {
      $project: {_id: 0, creatorId: '$_id', eventHash: 1, generation: 1}
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
  // Note: Uses Kahn's algorithm to topologically sort history to ensure that
  // parent events appear before their children in the result.
  const sortedHistory = [];

  const hashMap = new Map();
  for(const record of history) {
    hashMap.set(record.meta.eventHash, record);
  }

  // FIXME: rewrite to be non-recursive, see event sorting in `continuity.js`
  for(const record of history) {
    if(!record._p) {
      visit(record);
    }
  }

  return sortedHistory;

  function visit(record) {
    for(const parentHash of record.event.parentHash) {
      const n = hashMap.get(parentHash);
      if(n && !n._p) {
        visit(n);
      }
    }
    record._p = true;
    sortedHistory.push(record.meta.eventHash);
  }
}
