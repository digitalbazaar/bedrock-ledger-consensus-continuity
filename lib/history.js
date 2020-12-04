/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

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
    // startParentHash includes the head for the local node, as well as heads
    // for other nodes that have not yet been merged by the local node
    const {childlessHashes} = await _cache.events.getChildlessHashes(
      {ledgerNodeId: ledgerNode.id});
    startParentHash.push(...childlessHashes);
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

// called from consensus worker
api.getRecent = async ({creatorId, ledgerNode}) => {
  logger.verbose('Start history.getRecent');
  const timer = new _cache.Timer();
  timer.start({name: 'recentHistoryMergeOnly', ledgerNodeId: ledgerNode.id});

  const localBranchHead = await api.getHead({creatorId, ledgerNode});
  if(!localBranchHead) {
    throw new BedrockError(
      'Could not locate branch head.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }

  const {events, eventMap} = await _cache.history.getRecent(
    {ledgerNodeId: ledgerNode.id});

  const duration = timer.stop();
  logger.verbose('End history.getRecent', {duration});

  return {events, eventMap, localBranchHead};
};

api.getCreatorHeads = async (
  {latest = false, ledgerNode, localCreatorId, peerId}) => {
  // get ids for all the creators known to the node
  // FIXME: this is problematic... we can't possibly return ALL creators
  // ever known to the node when the network (and its history) gets quite
  // large ... need to negotiate something sane with the peer that we are
  // getting the creator heads for in the first place?
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

// TODO: document! what are `creatorHeads`
api.partition = async (
  {creatorHeads, creatorId, eventTypeFilter, fullEvent = false, peerId}) => {
  const _creatorHeads = bedrock.util.clone(creatorHeads);
  // NOTE: it is normal for creatorHeads not to include creatorId (this node)
  // if this node has never spoken to the peer before

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

// get the branch head for the specified creatorId
api.getHead = async (
  {creatorId, latest = false, ledgerNode, useCache = true}) => {
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
    if(records.length === 1) {
      const [{meta: {eventHash, continuity2017: {generation}}}] = records;
      head = {eventHash, generation};

      // update cache if flag is set
      if(useCache) {
        await _cache.history.setHead(
          {creatorId, eventHash, generation, ledgerNodeId});
      }
    }
  }

  // *still* no head, so use genesis head
  if(!head) {
    head = await api.getGenesisHead({ledgerNode});
  }

  return head;
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
  // generation for genesis is always zero
  head = {eventHash: meta.eventHash, generation: 0};

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
  // heads are `{eventHash, generation}`
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
