/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const _cache = require('./cache');
const _peers = require('./peers');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {config, util: {BedrockError}} = bedrock;
const logger = require('./logger');

const api = {};
module.exports = api;

api.aggregate = async ({
  creatorFilter, creatorRestriction, eventTypeFilter, ledgerNode, startHash,
  truncated
}) => {
  logger.verbose('Start history.aggregate');
  const timer = new _cache.Timer();
  timer.start({name: 'aggregate', ledgerNodeId: ledgerNode.id});

  const startParentHash = [startHash];

  // if the peer is not catching up, allow gossip regarding events that have not
  // yet been merged locally
  if(!truncated) {
    // FIXME: childless hashes are no longer tracked in this way, determine
    // if we want to continue doing so or if the new gossip protocol won't
    // require it and we don't need this adjustment anymore

    // startParentHash includes the head for the local node, as well as heads
    // for other nodes that have not yet been merged by the local node
    // const {childlessHashes} = await _cache.events.getChildlessHashes(
    //   {ledgerNodeId: ledgerNode.id});
    // startParentHash.push(...childlessHashes);
  }

  const {aggregateHistory} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  const history = await aggregateHistory({
    creatorFilter, creatorRestriction, eventTypeFilter, startHash,
    startParentHash
  });
  if(history.length === 0) {
    return [];
  }

  const sortedHistory = _sortHistory(history);
  timer.stop();
  return sortedHistory;
};

api.getCreatorHeads = async (
  {latest = false, ledgerNode, localCreatorId, peerId}) => {
  // FIXME: this function will be removed or revised to work on a blockHeight
  // get ids for all the creators known to the node
  // FIXME: this is problematic... we can't possibly return ALL creators
  // ever known to the node when the network (and its history) gets quite
  // large ... need to negotiate something sane with the peer that we are
  // getting the creator heads for in the first place?
  // FIXME: we should only get childless heads for gossip purposes
  const localCreators = await _peers.getPeerIds(
    {creatorId: peerId, ledgerNode});

  const heads = {};
  // ensure that localCreatorId and peerId is included in `creators`. If the
  // peer has never been seen before, it will not be included by
  // localCreators. In this case, the head for peerId will be set to genesis
  //  merge event.
  const creators = [...new Set([localCreatorId, peerId, ...localCreators])];
  await Promise.all(creators.map(async creatorId => {
    const head = await api.getHead({creatorId, latest, ledgerNode});
    heads[creatorId] = head;
  }));

  return {localCreators, heads};
};

// Gets the heads for each of the given creator IDs that are known to the
// given ledgerNode.
// FIXME: determine if this should replace `getCreatorHeads` or not
api.getHeads = async ({ledgerNode, creatorIds, latest = false}) => {
  // FIXME: should be a way to get the "heads" based on a blockHeight
  const heads = new Map();
  const creators = [...new Set(creatorIds)];
  await Promise.all(creators.map(async creatorId => {
    // FIXME: support checking for forks? how to handle?
    const head = await api.getHead({ledgerNode, creatorId, latest});
    if(head) {
      heads.set(creatorId, head);
    }
  }));
  return {heads};
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

// TODO: document! what are `creatorHeads`
api.partition = async (
  {creatorHeads, creatorId, eventTypeFilter, fullEvent = false, peerId}) => {
  // FIXME: this should only need to be a shallow copy and perhaps could be
  // avoided entirely
  const _creatorHeads = bedrock.util.clone(creatorHeads);
  // NOTE: it is normal for creatorHeads not to include creatorId (this node)
  // if this node has never spoken to the peer before

  // FIXME: why this is commented needs an explanation/or this should be
  // remove entirely
  // *do not!* remove the local creator from the heads
  // delete _creatorHeads[creatorId];

  // FIXME: voter and ledgerNode can be passed in some cases
  const ledgerNodeId = await _peers.getLedgerNodeId(creatorId);
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);

  // FIXME: document how `localHeads` are different from `creatorHeads`!
  // ... appears `creatorHeads` are passed from a peer by callers of this API
  const [genesisHead, localHeads] = await Promise.all([
    api.getGenesisHead({ledgerNode}),
    api.getCreatorHeads({localCreatorId: creatorId, ledgerNode, peerId})]);

  // get starting hash for aggregate search
  const {eventHash: startHash, truncated} = await _getStartHash(
    {creatorId, _creatorHeads, localHeads, genesisHead, peerId, ledgerNode});

  // NOTE: if this is the first contact with the peer, the head
  // for the local node will be set to genesisHead as well

  // localCreators contains a list of all creators this node knows about
  const peerCreators = Object.keys(creatorHeads);
  const localCreators = localHeads.localCreators;
  // remove heads for nodes this node doesn't know about
  peerCreators.forEach(c => {
    if(localCreators.includes(c)) {
      return;
    }
    delete _creatorHeads[c];
  });
  // TODO: seems like handle missing heads w/genesisHead both here and in
  // _getStartHash, can we make more DRY?
  // if the peer did not provide a head for a locally known creator,
  // set the head to genesis merge
  localCreators.forEach(c => {
    if(_creatorHeads[c]) {
      return;
    }
    _creatorHeads[c] = genesisHead;
  });

  const creatorRestriction = Object.keys(_creatorHeads).map(c => ({
    creator: c, generation: _creatorHeads[c].generation
  }));
  const history = await api.aggregate({
    creatorRestriction,
    creatorId,
    eventTypeFilter,
    fullEvent,
    ledgerNode,
    startHash,
    truncated,
  });

  return {
    creatorHeads: localHeads.heads,
    history,
    truncated
  };
};

// FIXME: new version
// returns a partition of the DAG for a (remote) peer with the given
// remoteInfo: {peerId, peerHeads, blockHeight, blockOrder, eventHash?}
api.partition2 = async ({ledgerNode, remoteInfo} = {}) => {
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

  logger.verbose('Start history.partition2');
  const timer = new _cache.Timer();
  timer.start({name: 'partition2', ledgerNodeId: ledgerNode.id});

  // if the remote peer's declared `blockHeight` is before the local peer's,
  // then first do a fast lookup against consensus history to get ordered
  // events for catch-up
  const nextBlockInfo = await _blocks.getNextBlockInfo({ledgerNode});
  result.blockHeight = nextBlockInfo.blockHeight - 1;
  result.blockHash = nextBlockInfo.previousBlockHash;
  if(remoteInfo.blockHeight < result.blockHeight) {
    const consensusEvents = await api._getConsensusEvents({
      ledgerNode,
      minBlockHeight: remoteInfo.blockHeight,
      minBlockOrder: remoteInfo.blockOrder
    });
    result.totalEvents = consensusEvents.eventCount;
    result.consensusEvents = {
      blocks: consensusEvents.blocks,
      next: consensusEvents.next
    };
    // if there is more consensus history, return early
    if(result.consensusEvents.next) {
      timer.stop();
      return result;
    }
  }

  // no more consensus history to return, so get non-consensus history...

  // inspect `peerHeads` to determine which are valid; cannot assume they are
  // valid as they could come from an untrusted peer
  const {validHeads} = await api.inspectPeerHeads(
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
  const startTime = Date.now();
  const history = await api._aggregateNonConsensusHistory({
    ledgerNode, minBlockHeight: nextBlockInfo.blockHeight,
    startHeads, endHeads: validHeads
  });
  console.log('total time=', Date.now() - startTime, 'ms');

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
api.inspectPeerHeads = async ({ledgerNode, peerHeads} = {}) => {
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
api._aggregateNonConsensusHistory = async function({
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
    result.next = {blockHeight, blockOrder};
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

// get the branch head for the specified creatorId
api.getHead = async ({
  ledgerNode, creatorId, latest = false, useCache = true, useGenesisHead = true
} = {}) => {
  if(latest && !useCache) {
    throw new Error(
      'If "latest" is "true", then "useCache" must also be "true".');
  }
  const ledgerNodeId = ledgerNode.id;

  let head;

  // `latest` flag is set, so using a head that is not yet committed to
  // storage, if available, is valid
  if(latest) {
    // NOTE: The uncommitted head is the *very* latest head in the event
    // pipeline which may not have been written to storage yet. This head is
    // useful during gossip, but not for merging events because it lacks
    // stability.
    head = await _cache.history.getUncommittedHead({creatorId, ledgerNodeId});
  }

  // no head found yet; get head from cache if allowed
  if(!head && useCache) {
    head = await _cache.history.getHead({creatorId, ledgerNodeId});
  }

  // no head found yet; use latest local merge event
  if(!head) {
    const {getHead} = ledgerNode.storage.events.plugins['continuity-storage'];
    const records = await getHead({creatorId});
    if(records.length > 0) {
      const [{
        event: {basisBlockHeight, mergeHeight},
        meta: {eventHash, continuity2017: {generation, localAncestorGeneration}}
      }] = records;
      head = {
        eventHash,
        generation,
        basisBlockHeight,
        mergeHeight,
        localAncestorGeneration
      };

      // update cache if flag is set
      if(useCache) {
        await _cache.history.setHead({creatorId, ledgerNodeId, ...head});
      }
    }
  }

  // *still* no head, so use genesis head
  if(!head && useGenesisHead) {
    head = await api.getGenesisHead({ledgerNode});
  }

  return head || null;
};

api.getGenesisHead = async ({ledgerNode}) => {
  // get genesis head from cache
  const ledgerNodeId = ledgerNode.id;
  let head = await _cache.history.getGenesisHead({ledgerNodeId});
  if(head) {
    return head;
  }

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
  head = {
    eventHash: meta.eventHash,
    generation: 0,
    basisBlockHeight: 0,
    mergeHeight: 0,
    localAncestorGeneration: 0
  };

  // update cache before returning head so it can be used next time
  await _cache.history.setGenesisHead(
    {eventHash: head.eventHash, ledgerNodeId});

  return head;
};

function _sortHistory(history) {
  // Note: Uses Kahn's algorithm to topologically sort history to ensure that
  // parent events appear before their children in the result.
  const sortedHistory = [];

  const hashMap = new Map();
  for(const record of history) {
    hashMap.set(record.meta.eventHash, record);
  }

  // FIXME: rewrite to be non-recursive
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

// FIXME: document `_creatorHeads` ... should it be `headsFromPeer`?
async function _getStartHash(
  {creatorId, _creatorHeads, localHeads, genesisHead, peerId, ledgerNode}) {
  // heads are: `{eventHash, generation, basisBlockHeight, mergeHeight,
  // localAncestorGeneration}`
  const localNodeHead = localHeads.heads[creatorId];
  const {maxDepth} = config['ledger-consensus-continuity'].gossip;
  // FIXME: the comment below seems to indicate that `_creatorHeads` should be
  // `headsFromPeer`... and here we are ensuring that the peer supplied the
  // head for our own node... but if not, the peer hasn't seen us (or is
  // byzantine, so we use the genesisHead to indicate that they are indicating
  // they they don't have any history from us at all)... clean up param names

  // peer should be provide a head for this node, just insurance here
  const peerLocalNodeHead = _creatorHeads[creatorId] || genesisHead;

  // node is within range, proceed normally
  if(localNodeHead.generation - peerLocalNodeHead.generation <= maxDepth) {
    return {eventHash: localNodeHead.eventHash, truncated: false};
  }
  // the peer is more than maxDepth behind, give them last hash within
  // maximum depth
  logger.debug('Truncating history, peer is catching up.', {
    localNodeHead, maxDepth, peerId, peerLocalNodeHead
  });
  const targetGeneration = peerLocalNodeHead.generation + maxDepth;
  const {getStartHash} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  const eventHash = await getStartHash({creatorId, targetGeneration});
  return {eventHash, truncated: true};
}
