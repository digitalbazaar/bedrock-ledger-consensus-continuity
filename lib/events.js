/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _blocks = require('./blocks');
const _cacheKey = require('./cache-key');
const _operations = require('./operations');
const _util = require('./util');
const _voters = require('./voters');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const {callbackify, BedrockError} = bedrock.util;
const {config} = bedrock;
const continuityStorage = require('./storage');
const database = require('bedrock-mongodb');
const jsonld = bedrock.jsonld;
const logger = require('./logger');
const {promisify} = require('util');
const signature = require('./signature');
const uuid = require('uuid/v4');
const voters = require('./voters');

// TODO: remove once bedrock-validation supports promises
const validate = promisify(require('bedrock-validation').validate);

const eventsConfig = config['ledger-consensus-continuity'].events;
const operationsConfig = config['ledger-consensus-continuity'].operations;

const api = {};
module.exports = api;

// FIXME: remove once bedrock-mongodb supports promises
const brCreateIndexes = promisify(database.createIndexes);

// TODO: update docs

/**
 * Adds a new event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.add = callbackify(async (
  {continuity2017, event, eventHash,
    genesis, genesisBlock, ledgerNode, needed}) => {
  if(genesisBlock) {
    // do not mutate genesisBlock
    genesisBlock = bedrock.util.clone(genesisBlock);
  }
  const configEventRecord = await _writeEvent(
    {continuity2017, event, eventHash, genesis, ledgerNode, needed});
  if(!genesis) {
    // no need to create genesis block, return early
    return configEventRecord;
  }

  // Note: The follow code only executes for the *genesis* block.
  await _addContinuityIndexes({ledgerNode});

  // need to write the genesis block, either from `options.genesisBlock`
  // to mirror an existing ledger, or create it ourselves for a new ledger
  const now = Date.now();

  // Note: merge event is automatically inserted as having achieved consensus
  // ...but config record is not; config record is updated after writing the
  // block
  const {mergeEventRecord, creator} = await _addGenesisMergeEvent(
    {configEventRecord, genesisBlock, ledgerNode, now});

  if(genesisBlock) {
    // genesisBlock given (we are cloning an existing ledger)
    // so simply replace the full event docs with hashes for storage
    genesisBlock.event = [
      configEventRecord.meta.eventHash,
      mergeEventRecord.meta.eventHash
    ];
  } else {
    // no genesisBlock given (we are creating a new ledger)
    // so create genesis block
    genesisBlock = {
      '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
      id: configEventRecord.event.ledgerConfiguration.ledger + '/blocks/0',
      type: 'WebLedgerEventBlock',
      consensusMethod: 'Continuity2017',
      event: [
        configEventRecord.meta.eventHash,
        mergeEventRecord.meta.eventHash
      ],
      // TODO: consensusProof should be stored as a hash
      consensusProof: [mergeEventRecord.event],
      blockHeight: 0,
      // FIXME: remove `publicKey`; unnecessary, given via proofs
      publicKey: [{
        id: creator.publicKey.id,
        type: 'Ed25519VerificationKey2018',
        owner: creator.id,
        publicKeyBase58: creator.publicKey.publicKeyBase58
      }]
    };
  }

  await _blocks.writeGenesis({block: genesisBlock, ledgerNode});

  // mark config event has having achieved consensus
  await ledgerNode.storage.events.update({
    eventHash: configEventRecord.meta.eventHash, patch: [{
      op: 'set',
      changes: {
        meta: {
          consensus: true,
          consensusDate: now
        }
      }
    }]
  });

  // FIXME: remove public key once public key simplification is complete
  const key = Object.assign({}, genesisBlock.publicKey[0]);
  key['@context'] = config.constants.WEB_LEDGER_CONTEXT_V1_URL;
  key.seeAlso = genesisBlock.id;
  try {
    await continuityStorage.keys.addPublicKey(ledgerNode.id, key);
  } catch(e) {
    if(e.name !== 'DuplicateError') {
      throw e;
    }
    // attempt to update key instead
    await continuityStorage.keys.updatePublicKey(ledgerNode.id, key);
  }

  return configEventRecord;
});

api.aggregateHistory = callbackify(async ({
  creatorFilter, creatorRestriction, eventTypeFilter,
  ledgerNode, startHash
}) => {
  logger.verbose('Start aggregateHistory');
  const collection = ledgerNode.storage.events.collection;
  // regular events *should* be included, regardless of created
  const restrictSearchWithMatch = {
    $nor: []
  };
  if(creatorRestriction && creatorRestriction.length !== 0) {
    creatorRestriction.forEach(r => restrictSearchWithMatch.$nor.push({
      'meta.continuity2017.type': 'm',
      'meta.continuity2017.creator': r.creator,
      'meta.continuity2017.generation': {$lte: r.generation}
    }));
  }

  if(Array.isArray(creatorFilter) && creatorFilter.length !== 0) {
    restrictSearchWithMatch.$nor.push(
      {'meta.continuity2017.creator': {$in: creatorFilter}});
  }

  if(eventTypeFilter) {
    const type = eventTypeFilter === 'ContinuityMergeEvent' ? 'm' : 'r';
    restrictSearchWithMatch['meta.continuity2017.type'] = type;
  }

  // genesisMerge should never be included in the history
  // FIXME: should not be necessary to add this
  // restrictSearchWithMatch.$nor.push({
  //   'meta.continuity2017.generation': 0
  // });
  const pipeline = [
    {$match: {'meta.eventHash': startHash}},
    {$graphLookup: {
      from: collection.s.name,
      startWith: '$meta.eventHash',
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
  const startTime = Date.now();
  const history = await collection.aggregate(
    pipeline, {allowDiskUse: true}).toArray();
  if(history.length === 0) {
    return [];
  }

  const sortedHistory = _sortHistory(history);

  // set duration timer (ignore errors, for stats only)
  cache.client.set(`aggregate|${ledgerNode.id}`, Date.now() - startTime)
    .catch(() => {});

  return sortedHistory;
});

// TODO: document (create a regular local event w/operations)
api.create = callbackify(async ({ledgerNode}) => {
  logger.verbose('Attempting to create an event.');
  try {
    // get a chunk of operations to put into the event
    const {operations, operationHash, truncated, opKeys} =
      await _getOperationChunk({ledgerNode});

    // TODO: could fetch `voter` and head concurrently with fetching operations
    // once it's clear that those operations exist
    const ledgerNodeId = ledgerNode.id;
    const {id: creatorId} = await voters.get({ledgerNodeId});
    const {eventHash: headHash} = await api._getLocalBranchHead(
      {creatorId, ledgerNode});

    // create the event and its hash
    const event = {
      '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
      type: 'WebLedgerOperationEvent',
      operationHash,
      parentHash: [headHash],
      treeHash: headHash
    };
    const eventHash = await _util.hasher(event);

    // store the operations first (if this fails, the same event hash will be
    // generated on retry)
    await _operations.write({eventHash, ledgerNode, operations});

    // store the event
    await api.add({event, eventHash, ledgerNode});

    // TODO: move to a separate redis/cache file/module
    // clear the events from the redis operation queue
    const opSelectedListKey = _cacheKey.operationSelectedList(ledgerNodeId);
    await cache.client.multi()
      // remove the selected list entirely
      .del(opSelectedListKey)
      // delete the keys that contain the operation documents
      .del(opKeys)
      .exec();

    return {truncated};
  } catch(e) {
    // FIXME: remove try/catch and just return early by checking
    // operations.length === 0
    if(e.name === 'AbortError') {
      return {truncated: false};
    }
    throw e;
  }
});

// called from consensus worker
api.getRecentHistory = callbackify(async ({creatorId, ledgerNode}) => {
  logger.verbose('Start getRecentHistory');
  const startTime = Date.now();

  const localBranchHead = await api._getLocalBranchHead(
    {creatorId, ledgerNode});
  if(!localBranchHead) {
    throw new BedrockError(
      'Could not locate branch head.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }

  // TODO: document
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNode.id);
  const keys = await cache.client.smembers(outstandingMergeKey);

  const events = [];
  const eventMap = {};
  if(keys && keys.length > 0) {
    // TODO: document (...if any result is `null` then events are missing)
    const result = await cache.client.mget(keys);
    if(result.indexOf(null) !== -1) {
      throw new BedrockError(
        'One or more events are missing from the cache.',
        'InvalidStateError', {
          httpStatusCode: 400,
          public: true,
        });
    }
    // TODO: document the data model here
    for(const eventSummaryJson of result) {
      const parsed = JSON.parse(eventSummaryJson);
      const {eventHash} = parsed;
      const {parentHash, treeHash, type} = parsed.event;
      const {creator} = parsed.meta.continuity2017;
      const doc = {
        _children: [],
        _parents: [],
        eventHash,
        event: {parentHash, treeHash, type},
        meta: {continuity2017: {creator}}
      };
      events.push(doc);
      eventMap[doc.eventHash] = doc;
    }
  }

  // ignore errors, just stats calculations
  const duration = Date.now() - startTime;
  cache.client.set(`recentHistoryMergeOnly|${ledgerNode.id}`, duration)
    .catch(() => {});
  logger.verbose('End getRecentHistory', {duration});

  return {events, eventMap, localBranchHead};
});

api.merge = callbackify(async ({creatorId, ledgerNode}) => {
  // TODO: move to redis/cache separate file/module
  // if no childless events to be merged, return
  const childlessKey = _cacheKey.childless(ledgerNode.id);
  const gossipBehind = _cacheKey.gossipBehind(ledgerNode.id);
  const multi = cache.client.multi();
  multi.get(gossipBehind);
  multi.smembers(childlessKey);
  const result = await multi.exec();

  // if the gossipBehind key is set, do not merge
  if(result[0] !== null) {
    return null;
  }

  // if there are no childless events, do not merge
  const parentHashes = result[1];
  if(parentHashes.length === 0) {
    return null;
  }

  // if no outstanding regular events to merge, return
  if(!await _hasOutstandingRegularEvents({ledgerNode})) {
    return null;
  }

  // get local branch head to merge on top of and compute next generation
  const {eventHash: treeHash, generation} =
    await api._getLocalBranchHead({creatorId, ledgerNode});
  const nextGeneration = generation + 1;

  // truncate as needed to maximum number of events to merge at once
  let truncated = false;
  const {maxEvents} = config['ledger-consensus-continuity'].merge;
  if(parentHashes.length > maxEvents) {
    truncated = true;
    parentHashes.length = maxEvents;
  }

  // create, sign, and hash merge event
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash: [treeHash, ...parentHashes],
    treeHash,
  };
  const ledgerNodeId = ledgerNode.id;
  const signed = await voters.sign({ledgerNodeId, doc: event});
  const eventHash = await _util.hasher(signed);

  // local merge events must be written directly to storage
  const meta = {
    continuity2017: {
      creator: creatorId,
      generation: nextGeneration,
      type: 'm'
    },
    eventHash
  };
  const record = await ledgerNode.storage.events.add({event: signed, meta});

  // update cache
  await _updateCache({eventRecord: record, ledgerNodeId});

  // FIXME: return {record, truncated} instead of mixing truncated into record
  //return {record, truncated};
  return {...record, truncated};
});

api.repairCache = callbackify(async ({eventHash, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const eventRecord = await ledgerNode.storage.events.get(eventHash);
  const updateCache = await _updateCache({eventRecord, ledgerNodeId});
  return {updateCache};
});

api.getCreatorHeads = callbackify(async (
  {latest = false, ledgerNode, peerId}) => {
  // get ids for all the creators known to the node
  // FIXME: create and use a key storage API or another API here
  // FIXME: this is problematic... as the number of peers could grow to be
  // quit large... and because the key cache is going to be removed soon
  const localCreators = (await database.collections.continuity2017_key.find(
    {ledgerNodeId: ledgerNode.id}, {_id: 0, 'publicKey.owner': 1})
    .toArray())
    .map(k => k.publicKey.owner);

  const heads = {};
  // ensure that peerId is included in creators, if the peer has never been
  // contacted before, it will not be included by localCreators
  // in this case, the head for peerId will be set to genesis merge event
  const creators = _.uniq([peerId, ...localCreators]);
  await Promise.all(creators.map(async creatorId => {
    const head = await api._getLocalBranchHead({creatorId, latest, ledgerNode});
    heads[creatorId] = head;
  }));

  return {localCreators, heads};
});

// TODO: document! what are `creatorHeads`
// TODO: move this and APIs related `history` to new `history.js`
api.partitionHistory = callbackify(async (
  {creatorHeads, creatorId, eventTypeFilter, fullEvent = false, peerId}) => {
  const _creatorHeads = bedrock.util.clone(creatorHeads);
  // NOTE: it is normal for creatorHeads not to include creatorId (this node)
  // if this node has never spoken to the peer before

  // *do not!* remove the local creator from the heads
  // delete _creatorHeads[creatorId];

  // FIXME: voter and ledgerNode can be passed in some cases
  const ledgerNodeId = await _voters.getLedgerNodeId(creatorId);
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);

  // FIXME: document how `localHeads` are different from `creatorHeads`!
  // ... appears `creatorHeads` are passed from a peer by callers of this API
  const [genesisHead, localHeads] = await Promise.all([
    api.getGenesisHead({ledgerNode}),
    api.getCreatorHeads({ledgerNode, peerId})]);

  // get starting hash for aggregate search
  const {eventHash: startHash, truncated} = await _getStartHash(
    {creatorId, _creatorHeads, localHeads, genesisHead, peerId, ledgerNode});
  if(startHash === null) {
    // peer has provided current local head so there's no missing history
    return {
      creatorHeads: localHeads.heads,
      history: [],
      truncated
    };
  }

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
  const history = await api.aggregateHistory({
    creatorRestriction,
    creatorId,
    eventTypeFilter,
    fullEvent,
    ledgerNode,
    startHash
  });

  return {
    creatorHeads: localHeads.heads,
    history,
    truncated
  };
});

// one use case is passing in a list of treeHashes
api.getHeads = callbackify(async ({eventHashes, ledgerNode}) => {
  // redis mget throws an error if an empty array is passed as an argument
  if(!(Array.isArray(eventHashes) && eventHashes.length !== 0)) {
    // FIXME: should this be returning an empty Map instead of `undefined`?
    return;
  }
  if(!ledgerNode) {
    throw new TypeError('"ledgerNode" is required.');
  }
  // eventHash, generation
  const headGenerationMap = new Map();
  // add every hash to headGenerationMap to ensure order is preserved
  eventHashes.forEach(eventHash => headGenerationMap.set(eventHash, null));
  const ledgerNodeId = ledgerNode.id;

  // first, get heads from redis cache
  const keys = eventHashes.map(
    eventHash => _cacheKey.headGeneration({eventHash, ledgerNodeId}));
  const cacheMisses = [];
  const result = await cache.client.mget(keys);
  result.forEach((generation, i) => {
    // redis returns null for misses
    if(generation === null) {
      // the eventHash, not the cache key
      cacheMisses.push(eventHashes[i]);
      return;
    }
    // the eventHash, not the cache key
    headGenerationMap.set(eventHashes[i], parseInt(generation, 10));
  });

  // for every cache miss, get heads from storage
  if(cacheMisses.length > 0) {
    const {collection} = ledgerNode.storage.events;
    const query = {'meta.eventHash': {$in: cacheMisses}};
    const projection = {
      _id: 0, 'meta.eventHash': 1, 'meta.continuity2017.generation': 1
    };
    const toBeCached = new Map();
    // FIXME: update once using new mongodb driver which returns a promise
    // for when `forEach` completes
    const cursor = await collection.find(query, projection);
    const forEach = promisify(cursor.forEach.bind(cursor));
    await forEach(({meta}) => {
      headGenerationMap.set(meta.eventHash, meta.continuity2017.generation);
      toBeCached.set(meta.eventHash, meta.continuity2017.generation);
    });

    // FIXME: move to redis/cache file/module
    // update cache
    if(toBeCached.size > 0) {
      const txn = cache.client.multi();
      for(const [eventHash, generation] of toBeCached) {
        const headGenerationKey = _cacheKey.headGeneration(
          {eventHash, ledgerNodeId});
        txn.set(headGenerationKey, generation, 'EX', 36000);
      }
      await txn.exec();
    }
  }

  for(const [eventHash, generation] of headGenerationMap) {
    // if this happens, then the batch references a treeHash that does not exist
    if(generation === null) {
      throw new BedrockError(
        'The eventHash could not be found.',
        'NotFoundError', {
          eventHash,
          httpStatusCode: 404,
          public: true,
        });
    }
  }

  return headGenerationMap;
});

// FIXME: make this a regular API call.  It's used in worker.js as well.
// and use an appropriate name since
// this is not just for local branch, this finds the latest merge event from
// the specified creator
api._getLocalBranchHead = callbackify(async (
  {useCache = true, creatorId, latest = false, ledgerNode}) => {
  if(!useCache && latest) {
    // FIXME: the conditional does not match this error message
    // ... if both "useCache" and "latest" are `false` this won't trigger
    throw new Error('"useCache" and "latest" arguments must both be `true`.');
  }
  const ledgerNodeId = ledgerNode.id;
  const eventsCollection = ledgerNode.storage.events.collection;
  const headCacheKey = _cacheKey.head({creatorId, ledgerNodeId});

  let head;

  // `latest` flag is set, so using `latestPeerHead`, if available, is valid
  if(latest) {
    // NOTE: latestPeerHead is the *very* latest head in the event pipeline
    // which may not have been written to mongo yet. This head is useful
    // during gossip, but not for merging events.
    const latestPeerHeadKey = _cacheKey.latestPeerHead(
      {creatorId, ledgerNodeId});
    const [eventHash, generation] = await cache.client.hmget(
      latestPeerHeadKey, 'h', 'g');
    // redis returns null if key is not found
    if(eventHash !== null) {
      head = {eventHash, generation: parseInt(generation, 10)};
    }
  }

  // no head found yet; get it from cache if allowed
  if(!head && useCache) {
    // return regular heads from cache/mongo even if latest flag is set
    const [eventHash, generation] = await cache.client.hmget(
      headCacheKey, 'h', 'g');
    // redis returns null if key is not found
    if(eventHash !== null) {
      head = {eventHash, generation: parseInt(generation, 10)};
    }
  }

  // no head found yet; use latest local merge event
  if(!head) {
    const query = {
      'meta.continuity2017.type': 'm',
      'meta.continuity2017.creator': creatorId,
      'meta.continuity2017.generation': {$exists: true}
    };
    const projection = {
      _id: 0, 'meta.eventHash': 1, 'meta.continuity2017.generation': 1
    };
    const records = await eventsCollection.find(query, projection)
      .sort({'meta.continuity2017.generation': -1})
      .limit(1).toArray();
    if(records.length === 1) {
      const [{meta}] = records;
      const eventHash = meta.eventHash;
      const generation = meta.continuity2017.generation;
      head = {eventHash, generation};

      // update cache if flag is set
      if(useCache) {
        await cache.client.hmset(headCacheKey, 'h', eventHash, 'g', generation);
      }
    }
  }

  // *still* no head, so use genesis head
  if(!head) {
    head = await api.getGenesisHead({ledgerNode});
  }

  return head;
});

api.getGenesisHead = callbackify(async ({ledgerNode}) => {
  // generation for genesis is always zero
  const generation = 0;

  // get genesis event hash from cache
  const genesisCacheKey = _cacheKey.genesis(ledgerNode.ledger);
  const genesisCache = await cache.client.get(genesisCacheKey);
  if(genesisCache) {
    return {eventHash: genesisCache, generation: 0};
  }

  // get genesis hash from storage
  const query = {'meta.continuity2017.type': 'm'};
  const projection =
    {_id: 0, 'meta.eventHash': 1, 'meta.continuity2017.generation': 1};
  const eventsCollection = ledgerNode.storage.events.collection;
  const [{meta}] = await eventsCollection.find(query, projection)
    .sort({'meta.continuity2017.generation': 1}).limit(1)
    .toArray();
  if(!(meta && meta.continuity2017.generation === 0)) {
    throw new BedrockError(
      'The genesis merge event was not found.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }
  const head = {eventHash: meta.eventHash, generation};

  // update cache before returning head so it can be used next time
  await cache.client.set(genesisCacheKey, head.eventHash);

  return head;
});

// return hashes that do not exist in redis or mongo
async function _diffManifest({ledgerNode, manifest, eventHashes}) {
  if(eventHashes.length === 0) {
    return;
  }
  // of the `eventHashes` given, determine which are not found in our
  // event queue or in storage via `_diff`
  const notFound = await _diff({eventHashes, ledgerNode});
  // TODO: why is this a duplicate count instead of just a count of what
  // events we don't have?
  const dupCount = eventHashes.length - notFound.length;
  if(dupCount !== 0) {
    // for statistics purposes only, ignore errors
    cache.client.incrby(`dup-${Math.round(Date.now() / 60000)}`, dupCount)
      .catch(() => {});
  }
  // remove any hashes that *are* found from the manifest, so that the
  // manifest only has not found hashes when it is returned
  const _manifest = bedrock.util.clone(manifest);
  _.remove(_manifest, e => !notFound.includes(e.eventHash));
  return _manifest;
}

// all ancestors should exist in redis or mongo, diff should be empty
async function _checkAncestors({ledgerNode, parentHash}) {
  const result = await _diff({eventHashes: parentHash, ledgerNode});
  if(result.length !== 0) {
    throw new BedrockError(
      'Events from `parentHash` and/or `treeHash` are missing.',
      'InvalidStateError', {
        httpStatusCode: 400,
        // TODO: a more accurate missing array can be computed
        // taking into account the cache hits
        missing: result,
        public: true,
      });
  }
}

// TODO: move to redis/cache file/module for reuse
async function _diff({eventHashes, ledgerNode}) {
  if(eventHashes.length === 0) {
    return [];
  }
  const ledgerNodeId = ledgerNode.id;
  const manifestId = uuid();
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  const manifestKey = _cacheKey.manifest({ledgerNodeId, manifestId});

  // TODO: this could be implemented as smembers as well and diff the hashes
  // as an array, if the eventQueueSetKey contains a large set, then the
  // existing implementation is good

  // Note: Here we add an entry to redis just to perform a diff...
  // 1. Add `manifestKey` with `eventHashes` array as the value
  // 2. Run `sdiff` to do diff that value with what is in the event queue
  //    for the ledger node (using key `eventQueueSetKey`)
  // 3. Delete the `manifestKey` once we're done running the diff
  // ... and results of `sadd` is in result[0], `sdiff` is in result[1], so
  // we destructure result[1] into `notFound` (i.e. events not in the queue)
  const [, notFound] = await cache.client.multi()
    .sadd(manifestKey, eventHashes)
    .sdiff(manifestKey, eventQueueSetKey)
    .del(manifestKey)
    .exec();
  // ...of the events not found in the event queue (redis), return those that
  // are also not in storage (mongo), i.e. we haven't stored them at all
  return ledgerNode.storage.events.difference(notFound);
}

api.processEvents = callbackify(async ({events, ledgerNode}) => {
  logger.verbose('Processing events.', {
    eventCount: events.length
  });

  const rawManifest = events;
  const rawManifestEventHashes = rawManifest.map(e => e.eventHash);
  const manifest = await _diffManifest(
    {ledgerNode, manifest: rawManifest,
      eventHashes: rawManifestEventHashes});

  // start with de-duped manifest
  // TODO: change to use this commented line
  //const allAncestors = [].concat(...manifest.map(r => r.event.parentHash));
  const allAncestors = [];
  for(let i = 0; i < manifest.length; ++i) {
    Array.prototype.push.apply(
      allAncestors, manifest[i].event.parentHash);
  }
  // remove dups and any ancestors that are contained in the
  // manifest itself
  const eventHashes = _.uniq(allAncestors).filter(
    a => !rawManifestEventHashes.includes(a));
  await _checkAncestors({eventHashes, ledgerNode});

  // add in serial to ensure that events are stored in the proper order
  for(const e of manifest) {
    await ledgerNode.events.add({continuity2017: {peer: true}, event: e.event});
  }
});

async function _genesisProofCreate({ledgerNode, eventHash}) {
  const doc = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash: [eventHash],
  };
  const ledgerNodeId = ledgerNode.id;
  const creator = await voters.get({ledgerNodeId, publicKey: true});
  const mergeEvent = await voters.sign({doc, ledgerNodeId});
  const mergeHash = await _util.hasher(mergeEvent);
  return {creator, mergeEvent, mergeHash};
}

// determine if there are any non-consensus events of type
// `r` (WebLedgerOperationEvent) or `c` (WebLedgerConfigurationEvent)
async function _hasOutstandingRegularEvents({ledgerNode}) {
  const {collection} = ledgerNode.storage.events;
  const query = {
    'meta.continuity2017.type': {$in: ['c', 'r']},
    'meta.consensus': {$exists: false},
  };
  // covered query under `continuity2` index
  const record = await collection.findOne(query, {
    _id: 0, 'meta.continuity2017.type': 1
  });
  return !!record;
}

// looks in queue and database for an event
async function _getGeneration({eventHash, ledgerNode}) {
  // first check cache
  const headGenerationKey = _cacheKey.headGeneration(
    {eventHash, ledgerNodeId: ledgerNode.id});
  const generation = await cache.client.get(headGenerationKey);
  if(generation !== null) {
    // cache hit
    return parseInt(generation, 10);
  }

  // check storage
  const {meta} = await ledgerNode.storage.events.get(eventHash);
  return meta.continuity2017.generation;
}

async function _processLocalEvent({event, eventHash, ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;
  const creator = await voters.get({ledgerNodeId});

  // FIXME: remove validate step
  // this is an unnecessary step since events are generated locally
  await validate('continuity.webLedgerEvents', event);

  const meta = {
    continuity2017: {creator: creator.id, type: 'r'},
    eventHash,
  };
  if(jsonld.hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
    meta.continuity2017.type = 'c';
  }
  return {event, meta};
}

async function _processOperations({ledgerNode, operations}) {
  return Promise.all(operations.map(async operation => {
    await ledgerNode.operations.validate(operation);
    const operationHash = await _util.hasher(operation);
    // TODO: do we need `recordId`? what is it for, statistics?
    const recordId = _util.generateRecordId({ledgerNode, operation});
    return {meta: {operationHash}, operation, recordId};
  }));
}

async function _processPeerConfigurationEvent({event, ledgerNode, needed}) {
  await ledgerNode.config.validate(event.ledgerConfiguration);
  const eventHash = await _util.hasher(event);
  if(!needed.includes(eventHash)) {
    throw new BedrockError(
      'The event supplied by the peer was not requested.',
      'DataError', {event, eventHash, needed});
  }
  const meta = {continuity2017: {type: 'c'}, eventHash};
  return {event, meta};
}

async function _processPeerRegularEvent({event, ledgerNode, needed}) {
  const _event = bedrock.util.clone(event);
  const {operation: ops} = _event;
  delete _event.operation;
  const operationRecords = await _processOperations(
    {ledgerNode, operations: ops});
  // eventHash is calculated based on the hashes of `operation` documents;
  // the order of the operationHash array does not affect the hash
  _event.operationHash = operationRecords.map(o => o.meta.operationHash);
  const eventHash = await _util.hasher(_event);

  // TODO: more performant if implemented with a Set?
  if(!needed.includes(eventHash)) {
    throw new BedrockError(
      'The event supplied by the peer was not requested.',
      'DataError', {event, eventHash, needed});
  }
  const meta = {continuity2017: {type: 'r'}, eventHash};

  // lexicographic sort on the hash of the operation determines the
  // order of operations in events
  _sortOperations(operationRecords);

  for(let i = 0; i < operationRecords.length; ++i) {
    const {meta} = operationRecords[i];
    meta.eventHash = eventHash;
    meta.eventOrder = i;
  }

  // put operation documents into _event
  _event.operationRecords = operationRecords;

  return {event: _event, meta};
}

async function _processPeerMergeEvent({event, ledgerNode, needed}) {
  const ledgerNodeId = ledgerNode.id;

  const [
    eventHash,
    parentGeneration,
    {id: localCreatorId},
    {keyOwner: {id: creator}}
  ] = await Promise.all([
    _util.hasher(event),
    _getGeneration({eventHash: event.treeHash, ledgerNode}),
    voters.get({ledgerNodeId}),
    // Note: signature.verify throws if signature is invalid
    signature.verify({doc: event, ledgerNodeId})
  ]);

  if(!needed.includes(eventHash)) {
    throw new BedrockError(
      'The event supplied by the peer was not requested.',
      'DataError', {event, eventHash, needed});
  }

  if(creator === localCreatorId) {
    throw new BedrockError(
      'Merge events created by the local node cannot be added with this API.',
      'NotSupportedError', {
        httpStatusCode: 400,
        public: true,
      });
  }

  const generation = parentGeneration + 1;
  const meta = {
    continuity2017: {creator, generation, type: 'm'},
    eventHash,
  };
  return {event, meta};
}

// TODO: these processing functions are candidates to move into a worker pool
// which would parallelize hashing and signature verification
async function _processPeerEvent({event, ledgerNode, needed}) {
  // ensure that all ancestors are in redis or mongo
  await _checkAncestors({ledgerNode, parentHash: event.parentHash});

  if(jsonld.hasValue(event, 'type', 'WebLedgerOperationEvent')) {
    return _processPeerRegularEvent({event, ledgerNode, needed});
  }
  if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
    return _processPeerMergeEvent({event, ledgerNode, needed});
  }
  if(jsonld.hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
    return _processPeerConfigurationEvent({event, ledgerNode, needed});
  }

  // unknown event type
  throw new BedrockError(
    'Unknown event type.',
    'DataError', {
      event,
      httpStatusCode: 404,
      public: true,
    });
}

async function _queueEvent({event, ledgerNodeId, meta}) {
  const {eventHash} = meta;
  const {creator: creatorId, generation, type} = meta.continuity2017;
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  const eventJson = JSON.stringify({event, eventHash, meta});

  // NOTE: redis multi is an atomic operation
  const multi = cache.client.multi();
  if(type === 'r') {
    const opCountKey = _cacheKey.opCountPeer(
      {ledgerNodeId, second: Math.round(Date.now() / 1000)});
    multi.incrby(opCountKey, event.operationRecords.length);
    multi.expire(opCountKey, operationsConfig.counter.ttl);
  }
  if(type === 'm') {
    const headGenerationKey = _cacheKey.headGeneration(
      {eventHash, ledgerNodeId});
    const latestPeerHeadKey = _cacheKey.latestPeerHead(
      {creatorId, ledgerNodeId});
    // expired the key in an hour, incase the peer/creator goes dark
    multi.hmset(latestPeerHeadKey, 'h', eventHash, 'g', generation);
    multi.expire(latestPeerHeadKey, 3600);
    // this key is set to expire in the event-writer
    multi.set(headGenerationKey, generation);
  }
  // add the hash to the set used to check for dups and ancestors
  multi.sadd(eventQueueSetKey, eventHash);
  // create a key that contains the event and meta
  multi.set(eventKey, eventJson);
  // push to the list that is handled in the event-writer
  multi.rpush(eventQueueKey, eventKey);
  multi.publish(`continuity2017|peerEvent|${ledgerNodeId}`, 'new');
  return multi.exec();
}

// sort an array of operation records, mutates operations
function _sortOperations(operations) {
  operations.sort((a, b) => a.meta.operationHash.localeCompare(
    b.meta.operationHash));
}

async function _updateCache({eventRecord, ledgerNodeId}) {
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const {creator: creatorId, generation} = eventRecord.meta.continuity2017;
  const {eventHash} = eventRecord.meta;
  const headKey = _cacheKey.head({creatorId, ledgerNodeId});
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const {parentHash, treeHash, type} = eventRecord.event;
  const parentHashes = _.without(parentHash, treeHash);
  // no need to set a headGeneration key here, those are only used for
  // processing peer merge events
  // TODO: `creator` is quite a long URL, can a substitution be made?

  // the redis cached event format puts `eventHash` at the top level, not in
  // `meta` like in ledger storage (e.g. mongo)
  const cacheEvent = JSON.stringify({
    eventHash,
    event: {parentHash, treeHash, type},
    meta: {continuity2017: {creator: creatorId}}
  });
  try {
    const result = cache.client.multi()
      .srem(childlessKey, parentHashes)
      .set(eventKey, cacheEvent)
      .sadd(outstandingMergeKey, eventKey)
      .hmset(headKey, 'h', eventHash, 'g', generation)
      .publish(`continuity2017|event|${ledgerNodeId}`, 'merge')
      .exec();
    // result is inspected in unit tests
    return result;
  } catch(e) {
    // FIXME: fail gracefully
    // failure here means head information would be corrupt which
    // cannot be allowed
    logger.error('Could not set head.', {
      creatorId,
      // FIXME: fix when logger.error works properly
      err1: e,
      generation,
      headKey,
      ledgerNodeId,
    });
    throw e;
  }
}

async function _addContinuityIndexes({ledgerNode}) {
  const {id: ledgerNodeId} = ledgerNode;
  const {id: localCreatorId} = await voters.get({ledgerNodeId});

  // add indexes specific to Continuity
  const collection = ledgerNode.storage.events.collection.s.name;
  await brCreateIndexes([{
    collection,
    fields: {
      'meta.eventHash': 1
    },
    options: {
      unique: true, background: false, name: 'continuity_meta_eventHash'
    }
  }, {
    // not for searches but to ensure that local node never forks
    collection,
    fields: {
      'meta.continuity2017.generation': 1
    },
    options: {
      unique: true, background: false, name: 'continuity_fork_prevention',
      partialFilterExpression: {
        'meta.continuity2017.type': 'm',
        'meta.continuity2017.creator': localCreatorId
      }
    }
  }, {
    collection,
    fields: {
      'meta.continuity2017.type': 1,
      'meta.continuity2017.creator': 1,
      'meta.continuity2017.generation': 1,
      'meta.eventHash': 1,
    },
    options: {
      sparse: false, unique: false, background: false, name: 'continuity1'
    }
  }, {
    collection,
    fields: {
      'meta.consensus': 1,
      'meta.continuity2017.type': 1,
      'meta.continuity2017.creator': 1,
    },
    options: {
      sparse: false, unique: false, background: false, name: 'continuity2'
    }
  }]);
}

async function _addGenesisMergeEvent(
  {configEventRecord, genesisBlock, ledgerNode, now}) {
  const blockHeight = 0;

  // update config event with block information
  const {eventHash} = configEventRecord.meta;
  await ledgerNode.storage.events.update({
    eventHash,
    patch: [{
      op: 'set',
      changes: {
        meta: {
          blockHeight,
          blockOrder: 0,
          consensus: true,
          consensusDate: now,
          updated: now
        }
      }
    }]
  });

  let result;
  if(genesisBlock) {
    // genesis block to clone given; verify it
    result = await _verifyGenesisEvents(
      {eventHash, genesisBlock, ledgerNodeId: ledgerNode.id});
  } else {
    // generate genesis proof
    result = await _genesisProofCreate({eventHash, ledgerNode});
  }

  // add merge event
  const {creator, mergeHash, mergeEvent: event} = result;
  const meta = {
    blockHeight,
    blockOrder: 1,
    consensus: true,
    consensusDate: now,
    continuity2017: {creator: creator.id, generation: 0, type: 'm'},
    created: now,
    updated: now,
    eventHash: mergeHash
  };
  const mergeEventRecord = await ledgerNode.storage.events.add({event, meta});
  return {mergeEventRecord, creator};
}

async function _verifyGenesisEvents({eventHash, genesisBlock, ledgerNodeId}) {
  // genesis block must contain a config event and a merge event
  if(genesisBlock.event.length !== 2) {
    throw new BedrockError(
      'The genesis block must contain two events.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }
  if(!genesisBlock.consensusProof || genesisBlock.consensusProof.length !== 1) {
    throw new BedrockError(
      'The genesis block `consensusProof` must contain exactly one event.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }
  const proof = genesisBlock.consensusProof[0];
  const mergeEvent = genesisBlock.event[1];
  // ensure the the gensis merge is a child of the genesis config
  // eventHash === hash of the genesis config
  // ensures events are in the proper order in `event`
  if(!(mergeEvent && mergeEvent.parentHash &&
    eventHash === mergeEvent.parentHash[0])) {
    throw new BedrockError(
      'The genesis merge event is invalid.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }

  // Note: `signature.verify` throws an error if verification fails
  const [{keyOwner: creator}, proofHash, mergeHash] = await Promise.all([
    signature.verify({doc: mergeEvent, ledgerNodeId}),
    _util.hasher(proof),
    _util.hasher(mergeEvent)]);
  if(mergeHash !== proofHash) {
    throw new BedrockError(
      'The genesis proof event must be the genesis merge event.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      });
  }
  return {creator, mergeEvent, mergeHash};
}

// eventHash is precomputed and passed in for local regular events
async function _writeEvent(
  {continuity2017, event, eventHash, genesis, ledgerNode, needed}) {
  const ledgerNodeId = ledgerNode.id;
  const {storage} = ledgerNode;
  const fromPeer = (continuity2017 && continuity2017.peer);

  // process event (create `meta`, do any extra validation, etc.)
  let eventRecord;
  if(fromPeer) {
    if(!needed) {
      throw new TypeError('"needed" argument is required.');
    }
    eventRecord = await _processPeerEvent({event, ledgerNode, needed});
  } else {
    if(!eventHash) {
      throw new TypeError('"eventHash" argument is required.');
    }
    if(genesis) {
      eventRecord = {event, meta: {eventHash}};
    } else {
      eventRecord = await _processLocalEvent({event, eventHash, ledgerNode});
    }
  }

  // write peer events to the queue and local events to storage
  if(fromPeer) {
    // events from peers that can be re-acquired go into redis queue
    const {event, meta} = eventRecord;
    await _queueEvent({event, ledgerNodeId, meta});
    // return `eventRecord` as is
    return eventRecord;
  }

  // only local regular events pass here
  try {
    eventRecord = await storage.events.add(eventRecord);
  } catch(e) {
    // ignore duplicates, may be recovering from a failed create operation
    if(e.name !== 'DuplicateError') {
      throw e;
    }
  }

  // finished if writing genesis config event
  if(genesis) {
    return eventRecord;
  }

  // the cache needs to be updated here as local events were committed
  // directly to mongo that need consensus
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const eventCountKey = _cacheKey.eventCountLocal(
    {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  await cache.client.multi()
    .sadd(childlessKey, eventRecord.meta.eventHash)
    .incr(eventCountKey)
    .expire(eventCountKey, eventsConfig.counter.ttl)
    // inform ConsensusAgent and other listeners about new regular events
    .publish(`continuity2017|event|${ledgerNodeId}`, 'regular')
    .exec();

  return eventRecord;
}

function _sortHistory(history) {
  // Note: Uses Kahn's algorithm to topologically sort history to ensure that
  // parent events appear before their children in the result.
  const sortedHistory = [];

  const hashMap = new Map();
  for(const record of history) {
    hashMap.set(record.meta.eventHash, record);
  }

  // FIXME: rewrite to be non-recursive
  let x;
  let s = 0;
  do {
    // using _.findIndex is faster than _.find
    x = _.findIndex(history, e => !e._p, s++);
    if(x === -1) {
      continue;
    }
    visit(history[x]);
  } while(x !== -1);

  // TODO: _.uniq??
  return sortedHistory;

  function visit(e) {
    for(const parentHash of e.event.parentHash) {
      const n = hashMap.get(parentHash);
      if(n && !n._p) {
        visit(n);
      }
    }
    e._p = true;
    sortedHistory.push(e.meta.eventHash);
  }
}

// TODO: move to a redis/cache file and rename
async function _getOperationChunk({ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;
  const opListKey = _cacheKey.operationList(ledgerNodeId);
  const opSelectedListKey = _cacheKey.operationSelectedList(ledgerNodeId);
  let truncated = false;

  // TODO: move into a redis function `operationQueue.pop` or similar
  const getOperationsTxn = cache.client.multi();

  // find redis keys for operations for the event...
  // first, check for selected ops which indicates a previous event creation
  let opKeys = await cache.client.lrange(opSelectedListKey, 0, -1);
  if(opKeys.length > 0) {
    // previous keys found, reuse to retry creation process
    getOperationsTxn.mget(opKeys);
  } else {
    // no previous operation keys found, so get a new list of operations
    // from the redis operation queue
    const {maxOperations} = eventsConfig;
    const [listLength, chunk] = await cache.client.multi()
      .llen(opListKey)
      .lrange(opListKey, 0, maxOperations - 1)
      .exec();
    if(listLength === 0) {
      logger.debug('No new operations.');
      throw new BedrockError(
        'No new operations to create an event with.', 'AbortError');
    }
    if(listLength > maxOperations) {
      truncated = true;
    }
    logger.debug(`New operations found: ${listLength}`);
    opKeys = chunk;
    getOperationsTxn.mget(opKeys);

    // remove selected operations from the master operation list
    // ltrim *keeps* items from start to end
    getOperationsTxn.ltrim(opListKey, opKeys.length, -1);

    // record the selected keys in case event creation fails
    getOperationsTxn.rpush(opSelectedListKey, opKeys);
  }

  // get operations to put into the event
  const [opJsons] = await getOperationsTxn.exec();
  const ops = [];
  for(const opJson of opJsons) {
    ops.push(JSON.parse(opJson));
  }
  // lexicographic sort on the hash of the operation determines the
  // order of operations in events
  _sortOperations(ops);
  const operations = [];
  const operationHash = [];
  for(let i = 0; i < ops.length; ++i) {
    operations.push(ops[i]);
    operationHash.push(ops[i].meta.operationHash);
  }
  return {operations, operationHash, truncated, opKeys};
}

// FIXME: move to a new `history.js` file
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
  if(peerLocalNodeHead.eventHash === localNodeHead.eventHash) {
    return {eventHash: null, truncated: false};
  }
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
  const collection = ledgerNode.storage.events.collection;
  const query = {
    'meta.continuity2017.type': 'm',
    'meta.continuity2017.creator': creatorId,
    'meta.continuity2017.generation': targetGeneration
  };
  const projection = {_id: 0, 'meta.eventHash': 1};
  const record = await collection.findOne(query, projection);
  return {eventHash: record.meta.eventHash, truncated: true};
}
