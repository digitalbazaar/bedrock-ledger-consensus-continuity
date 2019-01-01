/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _blocks = require('./blocks');
const _cache = require('./cache');
const _continuityConstants = require('./continuityConstants');
const _operations = require('./operations');
const _util = require('./util');
const _voters = require('./voters');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {callbackify, BedrockError} = bedrock.util;
const {config} = bedrock;
const database = require('bedrock-mongodb');
const jsonld = bedrock.jsonld;
const logger = require('./logger');
const {promisify} = require('util');
const signature = require('./signature');
const voters = require('./voters');

const api = {};
module.exports = api;

api._addTestEvent = callbackify(async ({event, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const eventMap = new Map();
  const {event: processedEvent, meta} = await _processPeerEvent(
    {event, eventMap, ledgerNode});
  await _cache.events.setEventGossip(
    {event, eventHash: meta.eventHash, ledgerNodeId, meta});
  await _cache.events.addPeerEvent({event: processedEvent, ledgerNodeId, meta});
});

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
  {event, eventHash, genesis, genesisBlock, ledgerNode}) => {
  if(genesisBlock) {
    // do not mutate genesisBlock
    genesisBlock = bedrock.util.clone(genesisBlock);
  }
  const configEventRecord = await _writeEvent(
    {event, eventHash, genesis, ledgerNode});
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
  const {mergeEventRecord} = await _addGenesisMergeEvent(
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
      blockHeight: 0
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

  return configEventRecord;
});

api.addBatch = async ({events, ledgerNode, needed}) => {

  // generate meta data for incoming events
  const {eventHashes, eventMap} = await _processBatch(
    {events, needed, ledgerNode});

  // inspect all the provided events
  await _validateGraph({eventHashes, eventMap, ledgerNode});

  // ensure that all events are valid. The peer suggested events that the local
  // node needed. The local node filtered those events which resulted in the
  // neededSet. It was confirmed that the events provided matched the needed
  // set. Now, if all events are not marked as valid, then that means that
  // the peer improperly told the local node it needed events that have no
  // relation to the other events in the batch.
  const ledgerNodeId = ledgerNode.id;
  eventMap.forEach(eventRecord => {
    if(!eventRecord._temp.valid) {
      throw new BedrockError(
        'An unrelated event was detected in the batch.', 'DataError', {
          eventRecord,
          httpStatusCode: 400,
          public: true,
        });
    }
  });

  // all the events are valid
  for(let i = 0; i < eventHashes.length; ++i) {
    const eventHash = eventHashes[i];
    const {event, meta} = eventMap.get(eventHash);
    // the unprocessed event goes into the cache for future gossip
    await _cache.events.setEventGossip(
      {event: events[i], eventHash, ledgerNodeId, meta});
    await _cache.events.addPeerEvent({event, ledgerNodeId, meta});
  }
};

api.aggregateHistory = callbackify(async ({
  creatorFilter, creatorRestriction, eventTypeFilter, ledgerNode, startHash,
  truncated
}) => {
  logger.verbose('Start aggregateHistory');
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
});

// TODO: document (create a regular local event w/operations)
api.create = callbackify(async ({ledgerNode}) => {
  logger.verbose('Attempting to create an event.');

  const ledgerNodeId = ledgerNode.id;
  const queue = new _cache.OperationQueue({ledgerNodeId});
  if(!await queue.hasNextChunk()) {
    logger.debug('No new operations.');
    return {truncated: false};
  }

  // get chunk of operations to put into the event concurrently with
  // fetching the head hash to merge on top of
  const [{basisBlockHeight, hasMore, operations}, {eventHash: headHash}] =
    await Promise.all([
      queue.getNextChunk(),
      voters.get({ledgerNodeId})
        .then(({id: creatorId}) => api.getHead({creatorId, ledgerNode})),
    ]);

  const baseEvent = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerOperationEvent',
    basisBlockHeight,
    parentHash: [headHash],
    treeHash: headHash
  };

  // the event with all operation documents goes into the cache for gossip
  const cacheEvent = Object.assign({}, baseEvent, {operation: operations.map(
    op => op.operation)});

  // create the event and its hash
  const event = Object.assign({}, baseEvent, {operationHash: operations.map(
    op => op.meta.operationHash)});

  const eventHash = await _util.hasher(event);

  // store the operations first (if this fails, the same event hash will be
  // generated on retry)
  await _operations.write({eventHash, ledgerNode, operations});

  // store the event
  await Promise.all([
    // add the event to the cache for gossip purposes
    _cache.events.setEventGossip(
      {event: cacheEvent, eventHash, ledgerNodeId, meta: {
        continuity2017: {type: 'r'},
        eventHash
      }}),
    api.add({event, eventHash, ledgerNode}),
  ]);

  // event successfully written, can now pop the chunk off the queue
  await queue.popChunk();

  return {hasMore};
});

api.getEventsForGossip = async ({eventHash, ledgerNodeId}) => {
  const needed = [];
  const eventStrings = [];
  const events = await _cache.events.getEvents({eventHash, ledgerNodeId});
  for(let i = 0; i < events.length; ++i) {
    if(events[i].event === null) {
      needed.push(eventHash[i]);
    }
    eventStrings.push(events[i].event);
  }
  // all the events were found in the cache
  if(needed.length === 0) {
    return eventStrings;
  }

  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);

  const hashMap = new Map();
  for(let i = 0; i < eventHash.length; ++i) {
    const {event} = events[i];
    hashMap.set(eventHash[i], event);
  }

  // TODO: update driver to get `promise` from `.forEach`
  let counter = 0;
  const cursor = ledgerNode.storage.events.getMany({eventHashes: needed});
  const fn = promisify(cursor.forEach.bind(cursor));
  // fill in the missing events in `hashMap`

  await fn(({event}) => {
    hashMap.set(needed[counter], JSON.stringify({event}));
    counter++;
  });
  return Array.from(hashMap.values());
};

api.getEvents = async ({eventHash, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  eventHash = [].concat(eventHash);
  const needed = [];
  const events = [];
  const cacheEvents = await _cache.events.getEvents(
    {eventHash, includeMeta: true, ledgerNodeId});
  for(let i = 0; i < cacheEvents.length; ++i) {
    if(cacheEvents[i].event === null) {
      needed.push(eventHash[i]);
      events.push(null);
      continue;
    }
    // combine `{event: {...}}` and `{meta: {...}}`
    const {event} = JSON.parse(cacheEvents[i].event);
    const {meta} = JSON.parse(cacheEvents[i].meta);
    events.push({event, meta});
  }
  // all the events were found in the cache
  if(needed.length === 0) {
    return events;
  }

  const hashMap = new Map();
  for(let i = 0; i < eventHash.length; ++i) {
    hashMap.set(eventHash[i], events[i]);
  }

  // TODO: update driver to get `promise` from `.forEach`
  let counter = 0;
  const cursor = ledgerNode.storage.events.getMany({eventHashes: needed});
  const fn = promisify(cursor.forEach.bind(cursor));
  // fill in the missing events in `hashMap`

  await fn(({event, meta}) => {
    hashMap.set(needed[counter], {event, meta});
    counter++;
  });
  return Array.from(hashMap.values());
};

// called from consensus worker
api.getRecentHistory = callbackify(async ({creatorId, ledgerNode}) => {
  logger.verbose('Start getRecentHistory');
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

  const {events, eventMap} = await _cache.events.getRecentHistory(
    {ledgerNodeId: ledgerNode.id});

  const duration = timer.stop();
  logger.verbose('End getRecentHistory', {duration});

  return {events, eventMap, localBranchHead};
});

api.merge = callbackify(async ({creatorId, ledgerNode}) => {
  // get hashes of events that can be merged and set them as the parent hashes
  // for the potential new merge event
  const parentHashes = await _getMergeableHashes({ledgerNode});

  // nothing to merge
  if(parentHashes.length === 0) {
    return null;
  }

  // get local branch head to merge on top of and compute next generation
  const {eventHash: treeHash, generation} =
    await api.getHead({creatorId, ledgerNode});
  const nextGeneration = generation + 1;

  // truncate as needed to maximum number of events to merge at once
  let truncated = false;
  const {mergeEvents: {maxEvents}} = _continuityConstants;
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
  const signed = await voters.sign({event, ledgerNodeId});
  const eventHash = await _util.hasher(signed);

  // local merge events must be written directly to storage
  const meta = {
    consensus: false,
    continuity2017: {
      creator: creatorId,
      generation: nextGeneration,
      type: 'm'
    },
    eventHash
  };
  const record = await ledgerNode.storage.events.add({event: signed, meta});

  // update cache
  await _cache.events.addLocalMergeEvent({...record, ledgerNodeId});

  // FIXME: return {record, truncated} instead of mixing truncated into record
  //return {record, truncated};
  return {...record, truncated};
});

api.repairCache = callbackify(async ({eventHash, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const eventRecord = await ledgerNode.storage.events.get(eventHash);
  // FIXME: update tests to make return value make more sense
  const updateCache = await _cache.events.addLocalMergeEvent(
    {...eventRecord, ledgerNodeId});
  return {updateCache};
});

api.getCreatorHeads = callbackify(async (
  {latest = false, ledgerNode, localCreatorId, peerId}) => {
  // get ids for all the creators known to the node
  // FIXME: this is problematic... we can't possibly return ALL creators
  // ever known to the node when the network (and its history) gets quite
  // large ... need to negotiate something sane with the peer that we are
  // getting the creator heads for in the first place?
  const localCreators = await _voters.getPeerIds(
    {creatorId: peerId, ledgerNode});

  const heads = {};
  // ensure that localCreatorId and peerId is included in `creators`. If the
  // peer has never been seen before, it will not be included by
  // localCreators. In this case, the head for peerId will be set to genesis
  //  merge event.
  const creators = _.uniq([localCreatorId, peerId, ...localCreators]);
  await Promise.all(creators.map(async creatorId => {
    const head = await api.getHead({creatorId, latest, ledgerNode});
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
  const history = await api.aggregateHistory({
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
});

// get the branch head for the specified creatorId
api.getHead = callbackify(async (
  {creatorId, latest = false, ledgerNode, useCache = true}) => {
  if(!useCache && latest) {
    // FIXME: the conditional does not match this error message
    // ... if both "useCache" and "latest" are `false` this won't trigger
    throw new Error('"useCache" and "latest" arguments must both be `true`.');
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
    head = await _cache.events.getUncommittedHead({creatorId, ledgerNodeId});
  }

  // no head found yet; get head from cache if allowed
  if(!head && useCache) {
    head = await _cache.events.getHead({creatorId, ledgerNodeId});
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
        await _cache.events.setHead(
          {creatorId, eventHash, generation, ledgerNodeId});
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
  // get genesis head from cache
  const ledgerNodeId = ledgerNode.id;
  let head = await _cache.events.getGenesisHead({ledgerNodeId});
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
  await _cache.events.setGenesisHead({eventHash: head.eventHash, ledgerNodeId});

  return head;
});

api.difference = async ({eventHashes, ledgerNode}) => {
  // first check event queue in the cache...
  const notFound = await _cache.events.difference(
    {eventHashes, ledgerNodeId: ledgerNode.id});
  if(notFound.length === 0) {
    return notFound;
  }
  // ...of the events not found in the event queue (redis), return those that
  // are also not in storage (mongo), i.e. we haven't stored them at all
  return ledgerNode.storage.events.difference(notFound);
};

async function _genesisProofCreate({ledgerNode, eventHash}) {
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash: [eventHash],
  };
  const ledgerNodeId = ledgerNode.id;
  const creator = await voters.get({ledgerNodeId, publicKey: true});
  const mergeEvent = await voters.sign({event, ledgerNodeId});
  const mergeHash = await _util.hasher(mergeEvent);
  return {creator, mergeEvent, mergeHash};
}

async function _getMergeableHashes({ledgerNode}) {
  const {gossipBehind, childlessHashes} = await _cache.events.getMergeStatus(
    {ledgerNodeId: ledgerNode.id});
  // if behind in gossip, do not merge
  if(gossipBehind) {
    return [];
  }

  // if there are no childless events, nothing to merge
  if(childlessHashes.length === 0) {
    return [];
  }

  // if no outstanding regular events to merge, nothing to merge
  const {hasOutstandingRegularEvents} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  if(!await hasOutstandingRegularEvents()) {
    return [];
  }

  return childlessHashes;
}

// looks in cache and storage for an event's generation
async function _getGeneration({eventHash, eventMap, ledgerNode}) {
  // check the map
  const event = eventMap.get(eventHash);
  if(event) {
    const {meta: {continuity2017: {generation}}} = event;
    return generation;
  }
  // first check cache
  const generation = await _cache.events.getGeneration(
    {eventHash, ledgerNodeId: ledgerNode.id});
  if(generation !== null) {
    // cache hit
    return generation;
  }

  // check storage
  const {meta} = await ledgerNode.storage.events.get(eventHash);
  return meta.continuity2017.generation;
}

async function _processBatch({events, needed, ledgerNode}) {
  // process events in the order they were received, merge events are
  // augmented with `generation` which means that the direct ancestor
  // `treeHash` must already be in cache/storage or contained in the batch
  const eventMap = new Map();
  const eventHashes = [];
  const neededSet = new Set(needed);
  for(const event of events) {
    const {event: processedEvent, meta} = await _processPeerEvent(
      {event, eventMap, ledgerNode});
    const {eventHash} = meta;
    eventHashes.push(eventHash);
    eventMap.set(eventHash, {
      event: processedEvent, meta, _temp: {valid: false}
    });
    if(!neededSet.has(eventHash)) {
      throw new BedrockError(
        'The event supplied by the peer was not requested.',
        'DataError', {event, eventHash, neededSet});
    }
    neededSet.delete(meta.eventHash);
  }

  // ensure that all the needed events are included in the batch
  if(neededSet.size !== 0) {
    throw new BedrockError(
      'The batch does not include all the needed events.',
      'DataError', {
        httpStatusCode: 400,
        missingEventHashes: [...neededSet],
        public: true,
      });
  }
  return {eventHashes, eventMap};
}

async function _processLocalEvent({event, eventHash, ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;
  const creator = await voters.get({ledgerNodeId});
  const meta = {
    consensus: false,
    continuity2017: {creator: creator.id, type: 'r'},
    eventHash,
  };
  if(jsonld.hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
    meta.continuity2017.type = 'c';
  }
  return {event, meta};
}

async function _processOperations({basisBlockHeight, ledgerNode, operations}) {
  return Promise.all(operations.map(async operation => {
    const result = await ledgerNode.operations.validate(
      {basisBlockHeight, ledgerNode, operation});
    if(!result.valid) {
      throw result.error;
    }
    const {hash: operationHash, canonizedBytes} =
      await _util.rdfCanonizeAndHash(operation);
    const {operations: {maxBytes}} = _continuityConstants;
    if(canonizedBytes > _continuityConstants.operations.maxBytes) {
      throw new BedrockError(
        'The operation exceeds the byte size limit.',
        'DataError', {
          canonizedBytes,
          httpStatusCode: 400,
          maxBytes,
          operation,
          public: true,
        });
    }
    // the `recordId` property is indexed in the storage layer
    const recordId = _util.generateRecordId({ledgerNode, operation});
    return {meta: {operationHash}, operation, recordId};
  }));
}

async function _processPeerConfigurationEvent({event, ledgerNode}) {
  const {basisBlockHeight} = event;
  const result = await ledgerNode.config.validate({
    basisBlockHeight, ledgerConfiguration: event.ledgerConfiguration,
    ledgerNode
  });
  if(!result.valid) {
    throw result.error;
  }
  const eventHash = await _util.hasher(event);
  const meta = {consensus: false, continuity2017: {type: 'c'}, eventHash};
  return {event, meta};
}

async function _processPeerRegularEvent({event, ledgerNode}) {
  const _event = bedrock.util.clone(event);
  const {basisBlockHeight, operation: ops} = _event;
  delete _event.operation;
  const operationRecords = await _processOperations(
    {basisBlockHeight, ledgerNode, operations: ops});
  // eventHash is calculated based on the hashes of `operation` documents;
  // the order of the operationHash array does not affect the hash
  _event.operationHash = operationRecords.map(o => o.meta.operationHash);
  const eventHash = await _util.hasher(_event);

  // after `eventHash` has been computed `operationHash` is no longer needed
  delete _event.operationHash;

  const meta = {consensus: false, continuity2017: {type: 'r'}, eventHash};

  // lexicographic sort on the hash of the operation determines the
  // order of operations in events
  _util.sortOperations(operationRecords);

  for(let i = 0; i < operationRecords.length; ++i) {
    const {meta} = operationRecords[i];
    meta.eventHash = eventHash;
    meta.eventOrder = i;
  }

  // put operation documents into _event
  _event.operationRecords = operationRecords;

  return {event: _event, meta};
}

async function _processPeerMergeEvent({event, eventMap, ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;

  const [
    eventHash,
    parentGeneration,
    {id: localCreatorId},
    {keyOwner: {id: creator}}
  ] = await Promise.all([
    _util.hasher(event),
    _getGeneration({eventHash: event.treeHash, eventMap, ledgerNode}),
    voters.get({ledgerNodeId}),
    // Note: signature.verify throws if signature is invalid
    signature.verify({event})
  ]);

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
    consensus: false,
    continuity2017: {creator, generation, type: 'm'},
    eventHash,
  };
  return {event, meta};
}

// TODO: these processing functions are candidates to move into a worker pool
// which would parallelize hashing and signature verification
async function _processPeerEvent({event, eventMap, ledgerNode}) {
  if(jsonld.hasValue(event, 'type', 'WebLedgerOperationEvent')) {
    return _processPeerRegularEvent({event, ledgerNode});
  }
  if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
    return _processPeerMergeEvent({event, eventMap, ledgerNode});
  }
  if(jsonld.hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
    return _processPeerConfigurationEvent({event, ledgerNode});
  }
  throw new BedrockError(
    'Unknown event type.',
    'DataError', {
      event,
      httpStatusCode: 400,
      public: true,
    });
}

async function _addContinuityIndexes({ledgerNode}) {
  const {id: ledgerNodeId} = ledgerNode;
  const {id: localCreatorId} = await voters.get({ledgerNodeId});

  // FIXME: determine the right way to handle this index. It is exceptional
  // because it requires knowledge of `localCreatorId`. Perhaps the
  // continuity-storage plugin can provide an `addIndex` method.

  // add indexes specific to Continuity
  const collection = ledgerNode.storage.events.collection.s.name;
  await brCreateIndexes([{
    // not for searches but to ensure that local node never forks
    collection,
    fields: {'meta.continuity2017.generation': 1},
    options: {
      unique: true, background: false,
      name: 'event.continuity2017.forkPrevention',
      partialFilterExpression: {
        'meta.continuity2017.type': 'm',
        'meta.continuity2017.creator': localCreatorId
      }
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
          updated: now,
          continuity2017: {
            type: 'c'
          }
        }
      }
    }]
  });

  let result;
  if(genesisBlock) {
    // genesis block to clone given; verify it
    result = await _verifyGenesisEvents({eventHash, genesisBlock});
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

async function _getGenesisCreator({ledgerNode}) {
  const {eventHash: genesisHeadHash} = await api.getGenesisHead(
    {ledgerNode});
  const [{meta: {continuity2017: {creator: genesisCreator}}}] =
    await api.getEvents({eventHash: genesisHeadHash, ledgerNode});
  return genesisCreator;
}

// iterate over the eventMap in reverse order to validate graph integrity
// identify merge events, then validate the regular event ancestors.  ensure:
// `operation.creator` in WebLedgerOperationEvent is proper
// `event.creator` in WebLedgerConfigurationEvent is proper
// ancestors referenced outside the batch are merge events
async function _validateGraph({eventHashes, eventMap, ledgerNode}) {
  for(let i = eventHashes.length - 1; i >= 0; --i) {
    const eventRecord = eventMap.get(eventHashes[i]);
    const {_temp, event, meta} = eventRecord;
    if(meta.continuity2017.type === 'm') {
      // ContinuityMergeEvent
      // the validated creator
      const {continuity2017: {
        creator: requiredCreator, generation: topLevelGeneration
      }} = meta;
      const {treeHash: mergeTreeHash} = event;
      // track the creators for the merge events in parentHash
      const creatorSet = new Set();
      let allowGenesisCreatorExecption = true;
      for(const ancestorHash of event.parentHash) {
        let ancestorRecord;
        let outsideBatch = false;
        ancestorRecord = eventMap.get(ancestorHash);
        if(!ancestorRecord) {
          outsideBatch = true;
          [ancestorRecord] = await api.getEvents(
            {eventHash: ancestorHash, ledgerNode});
        }
        const {meta: {continuity2017: {creator, generation, type}}} =
          ancestorRecord;
        let treeHash;
        if(generation !== 0) {
          ({event: {treeHash}} = ancestorRecord);
        }
        // merge events must be based on previous merge by same creator
        // with the exception of generation 1 events which are based on the
        // genesis merge which was created by the genesis peer
        if(ancestorHash === mergeTreeHash && generation !== 0 &&
          !(type === 'm' && creator === requiredCreator)
        ) {
          throw new BedrockError(
            'Peers must base merge events on their own merge events.',
            'DataError', {
              ancestorRecord,
              eventRecord,
              httpStatusCode: 400,
              public: true,
            });
        }
        if(type === 'm') {
          // use simplest possible conditional for the common case
          if(!creatorSet.has(creator)) {
            creatorSet.add(creator);
          } else {
            // a generation 1 merge event is an exception here because it may
            // be based on the genesis merge event and *also* a generation N
            // merge event from the genesis peer. An exception for exactly one
            // additional merge event by the genesis creator is made here.
            if(!(topLevelGeneration === 1 && allowGenesisCreatorExecption)) {
              throw new BedrockError(
                'Merge events must not descend from multiple merge events ' +
                'from the same creator.', 'DataError', {
                  allowGenesisCreatorExecption,
                  creator,
                  creatorSet: [...creatorSet],
                  ancestorRecord,
                  ancestorHash,
                  eventRecord,
                  httpStatusCode: 400,
                  outsideBatch,
                  public: true,
                });
            }
            const genesisCreator = await _getGenesisCreator({ledgerNode});
            if(creator === genesisCreator) {
              allowGenesisCreatorExecption = false;
            } else {
              throw new BedrockError(
                'Merge events must not descend from multiple merge events ' +
                'from the same creator.', 'DataError', {
                  allowGenesisCreatorExecption,
                  creator,
                  creatorSet: [...creatorSet],
                  ancestorRecord,
                  ancestorHash,
                  eventRecord,
                  genesisCreator,
                  httpStatusCode: 400,
                  outsideBatch,
                  public: true,
                });
            }
          }
        } else { // regular and configuration events types c || r
          // the only ancestors that are not in the batch must be merge events
          if(outsideBatch) {
            throw new BedrockError(
              'Invalid ancestor detected.',
              'DataError', {
                ancestorRecord,
                eventRecord,
                httpStatusCode: 400,
                public: true,
              });
          }
          // it is common to find merge events and regular events that are
          // sibilings in `parentHash`, but they must have the same creator
          if(treeHash !== mergeTreeHash) {
            let directAncestor;
            // get the direct ancestor of the ancestor
            directAncestor = eventMap.get(treeHash);
            if(!directAncestor) {
              [directAncestor] = await api.getEvents(
                {eventHash: treeHash, ledgerNode});
            }
            const {meta: {continuity2017: {creator, type}}} = directAncestor;
            if(!(type === 'm' && creator === requiredCreator)) {
              throw new BedrockError(
                'Peers must base regular events on their own merge events.',
                'DataError', {
                  ancestorRecord,
                  directAncestor,
                  eventRecord,
                  httpStatusCode: 400,
                  public: true,
                });
            }
          }
          if(type === 'r') {
            // WebLedgerOperationEvent
            const {event: {operationRecords}} = ancestorRecord;
            for(const {operation} of operationRecords) {
              if(operation.creator !== requiredCreator) {
                throw new BedrockError(
                  '`operation.creator` must correspond to the creator of ' +
                  'the merge event.', 'DataError', {
                    ancestorRecord,
                    eventRecord,
                    httpStatusCode: 400,
                    public: true,
                    requiredCreator
                  });
              }
            }
          } else if(type === 'c') {
            // WebLedgerConfigurationEvent
            const {event: {ledgerConfiguration: {ledger: expectedLedger}}} =
              await ledgerNode.storage.events.getLatestConfig();
            const {creator, ledger} = ancestorRecord.event.ledgerConfiguration;
            if(ledger !== expectedLedger) {
              throw new BedrockError(
                `'ledger' must correspond to the existing configuration.`,
                'SyntaxError', {
                  ancestorRecord,
                  eventRecord,
                  expectedLedger,
                  httpStatusCode: 400,
                  ledger,
                  public: true,
                });
            }
            if(creator !== requiredCreator) {
              throw new BedrockError(
                `'creator' must correspond to the creator of ` +
                ' the merge event.', 'SyntaxError', {
                  ancestorRecord,
                  creator,
                  eventRecord,
                  httpStatusCode: 400,
                  public: true,
                  requiredCreator
                });
            }
          }
          ancestorRecord._temp.valid = true;
        }
      }
      // all the events referenced in the merge event are valid, so it is valid
      _temp.valid = true;
    }
  }
}

async function _verifyGenesisEvents({eventHash, genesisBlock}) {
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
    signature.verify({event: mergeEvent}),
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
async function _writeEvent({event, eventHash, genesis, ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;
  const {storage} = ledgerNode;

  // process event (create `meta`, do any extra validation, etc.)
  let eventRecord;
  if(!eventHash) {
    throw new TypeError('"eventHash" argument is required.');
  }
  if(genesis) {
    eventRecord = {event, meta: {eventHash, effectiveConfiguration: true}};
  } else {
    eventRecord = await _processLocalEvent({event, eventHash, ledgerNode});
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

  // the cache needs to be updated here as a local event was committed
  // to storage and it now needs to be merged...
  // FIXME: is this note true? it appears that storage will not ever be
  // consulted if this call fails... do we recover properly?
  // Note: If this call fails, the consensus agent will still eventually
  // find the event in storage and merge it in; updating the cache just
  // expedites the process
  await _cache.events.addLocalRegularEvent({eventHash, ledgerNodeId});

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
