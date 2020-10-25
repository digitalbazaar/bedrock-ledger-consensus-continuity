/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _blocks = require('./blocks');
const _cache = require('./cache');
const _continuityConstants = require('./continuityConstants');
const _operations = require('./operations');
const _signature = require('./signature');
const _util = require('./util');
const _voters = require('./voters');
const bedrock = require('bedrock');
const brCooldown = require('bedrock-cooldown');
const brLedgerNode = require('bedrock-ledger-node');
const {util: {callbackify, hasValue, BedrockError}} = bedrock;
const {config} = bedrock;
const database = require('bedrock-mongodb');
const logger = require('./logger');
const pImmediate = require('p-immediate');
const pLimit = require('p-limit');
const {promisify} = require('util');

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

api.addBatch = async ({
  blockHeight, events, eventMap, ledgerNode, mergePermits,
  processedEventHashes = new Set()
}) => {
  const ledgerNodeId = ledgerNode.id;
  // contains a record of the original unaltered events
  const deferredEvents = [];
  const eventHashes = [...eventMap.keys()];
  let mergePermitsConsumed = 0;
  const alert = {};
  for(let i = 0; i < eventHashes.length; ++i) {
    if(mergePermitsConsumed >= mergePermits) {
      deferredEvents.push(events[i]);
      continue;
    }
    const eventHash = eventHashes[i];
    const {event, meta, _temp} = eventMap.get(eventHash);

    const {basisBlockHeight} = event;
    // regular events and configuration events have basisBlockHeight
    if(Number.isInteger(basisBlockHeight) && basisBlockHeight > blockHeight) {
      const blocksBehind = basisBlockHeight - blockHeight;
      logger.verbose('blocks behind', {
        basisBlockHeight,
        blocksBehind,
        blockHeight
      });
      if(blocksBehind >= 5) {
        if(blocksBehind > (alert.blocksBehind || 0)) {
          alert.blocksBehind = blocksBehind;
          alert.basisBlockHeight = basisBlockHeight;
          alert.blockHeight = blockHeight;
          // set cooldown
          logger.verbose('Cooldown Activated: Continuity is in catchup mode', {
            module: 'ledger-consensus-continuity',
            message: 'Continuity is in catchup mode.',
            ...alert
          });
          await brCooldown.set('ledger-consensus-continuity:catchup', {
            module: 'ledger-consensus-continuity',
            message: 'Continuity is in catchup mode.',
            ...alert
          }, 10 * 1000);
        }
      }
      // do not attempt to validate yet
      deferredEvents.push(events[i]);
      continue;
    }

    // events here could be regular, configuration or merge events
    if(hasValue(event, 'type', 'WebLedgerOperationEvent')) {
      const limit = pLimit(100);
      await Promise.all(event.operationRecords.map(({operation}) =>
        limit(async () => {
          await pImmediate();
          const result = await ledgerNode.operations.validate(
            {basisBlockHeight, ledgerNode, operation});
          if(!result.valid) {
            throw result.error;
          }
        })
      ));
    } else if(hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
      const result = await ledgerNode.config.validate({
        basisBlockHeight, ledgerConfiguration: event.ledgerConfiguration,
        ledgerNode
      });
      if(!result.valid) {
        throw result.error;
      }
    } else if(hasValue(event, 'type', 'ContinuityMergeEvent')) {
      if(deferredEvents.length > 0 && _temp.insideBatch.length > 0) {
        // if no events have been deferred, it is not necessasry to check the
        // processedEventHashes against the insideBatch list. We know that the
        // ancestors were ordered in the batch prior to this event. If none
        // of those ancestors were deferred, it is safe to proceed without the
        // additional check.

        // ensure every event referenced inside the batch has been processed.
        // Some of this merge event's ancestors inside this batch may have been
        // deferred. If so, this event must be deferred as well.
        if(!_temp.insideBatch.every(
          eventHash => processedEventHashes.has(eventHash))) {
          // some ancestors were excluded from processing
          deferredEvents.push(events[i]);
          continue;
        }
      }
      // this merge event will be included in the batch
      mergePermitsConsumed++;
    }

    // the original unaltered event goes into the cache for future gossip
    await _cache.events.setEventGossip(
      {event: events[i], eventHash, ledgerNodeId, meta});
    await _cache.events.addPeerEvent({event, ledgerNodeId, meta});

    // the event was successfully processed, record the hash and delete from map
    processedEventHashes.add(eventHash);
    eventMap.delete(eventHash);
  }
  return {deferredEvents, mergePermitsConsumed, processedEventHashes};
};

api.prepBatch = async ({events, ledgerNode, needed}) => {
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
  return {eventMap};
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
    return {hasMore: false};
  }

  // get chunk of operations to put into the event concurrently with
  // fetching the head hash to merge on top of
  const [{basisBlockHeight, hasMore, operations}, {eventHash: headHash}] =
    await Promise.all([
      queue.getNextChunk(),
      _voters.get({ledgerNodeId})
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

  // TODO: an iteration over events can be avoided if cache getEvents
  // returns `needed`
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

  const cursor = ledgerNode.storage.events.getMany({eventHashes: needed});

  // fill in the missing events in `hashMap`
  let counter = 0;
  for await (const {event} of cursor) {
    hashMap.set(needed[counter], JSON.stringify({event}));
    counter++;
  }

  return Array.from(hashMap.values());
};

api.getEvents = async ({eventHash, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  if(!Array.isArray(eventHash)) {
    eventHash = [eventHash];
  }
  const needed = [];
  const events = [];

  // TODO: an iteration over events can be avoided if cache getEvents
  // returns `needed`
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

  let counter = 0;
  const cursor = ledgerNode.storage.events.getMany({eventHashes: needed});

  // fill in the missing events in `hashMap`
  for await (const {event, meta} of cursor) {
    hashMap.set(needed[counter], {event, meta});
    counter++;
  }

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

api.merge = callbackify(async ({
  creatorId, ledgerNode, priorityPeers, mergeStatus
}) => {
  if(!mergeStatus) {
    // merge status was not provided, retrieve it on demand
    mergeStatus = await api.getMergeStatus(
      {ledgerNode, creatorId, priorityPeers});
  }

  if(!mergeStatus.mergeable) {
    // nothing to merge
    return null;
  }

  // determine hashes of events to be used as parents for new merge event...
  const {mergeEvents: {maxEvents}} = _continuityConstants;
  const {peerChildlessHashes, localChildlessHashes} = mergeStatus;

  // use up to 50% of parent spots using peer merge events
  const maxPeerEvents = Math.ceil(maxEvents / 2);
  const parentHashes = peerChildlessHashes.slice(0, maxPeerEvents);

  // fill remaining spots with regular events, leaving one spot for `treeHash`
  const maxRegularEvents = maxEvents - parentHashes.length - 1;
  parentHashes.push(...localChildlessHashes.slice(0, maxRegularEvents));

  // FIXME: need to ensure that any `parentHashes` weren't already merged...
  // this could potentially be solved by recomputing `childlessHashes` at the
  // start of every work session (this is to avert potential danger that a
  // merge happened but then the cache wasn't updated to remove childless
  // hashes for some reason)

  // nothing to merge
  if(parentHashes.length === 0) {
    return null;
  }

  // set `truncated` to true if there are still more hashes to merge
  const total = peerChildlessHashes.length + localChildlessHashes.length;
  const truncated = total > parentHashes.length;

  // get local branch head to merge on top of and compute next generation
  const {eventHash: treeHash, generation} =
    await api.getHead({creatorId, ledgerNode});
  const nextGeneration = generation + 1;

  // create, sign, and hash merge event
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash: [treeHash, ...parentHashes],
    treeHash,
  };
  const ledgerNodeId = ledgerNode.id;
  const signed = await _signature.sign({event, ledgerNodeId});
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

api.getMergeStatus = async ({ledgerNode, creatorId, priorityPeers = []}) => {
  const ledgerNodeId = ledgerNode.id;
  const status = await _cache.events.getMergeStatus({ledgerNodeId});
  const {peerChildlessHashes, localChildlessHashes} = status;

  // report outstanding regular events
  const {hasOutstandingRegularEvents, getHead} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  status.hasOutstandingRegularEvents = await hasOutstandingRegularEvents();

  // report outstanding local operations
  // TODO: optimize checking for outstanding operations
  const queue = new _cache.OperationQueue({ledgerNodeId});
  status.hasOutstandingLocalOperations = await queue.hasNextChunk();

  // report any outstanding operations (already in regular events or not)
  status.hasOutstandingOperations = status.hasOutstandingRegularEvents ||
    status.hasOutstandingLocalOperations;

  // if no outstanding regular events and no outstanding local operations,
  // then there is nothing to merge
  if(!status.hasOutstandingOperations) {
    return {mergeable: false, ...status};
  }

  // if there are no peer events to merge and there are `priorityPeers`
  // but `creatorId` is not one of them...
  if(peerChildlessHashes.length === 0 &&
    (priorityPeers.length > 0 && !priorityPeers.includes(creatorId))) {
    // if we don't have anything local that needs merging, do not merge
    const localOutstanding =
      (localChildlessHashes.length > 0 || status.hasOutstandingLocalOperations);
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
    const [{event: headMergeEvent}] = await api.getEvents({
      eventHash: headRecord.meta.eventHash,
      ledgerNode
    });
    const eventHash = headMergeEvent.parentHash.filter(
      h => h !== headMergeEvent.treeHash);
    const events = await api.getEvents({eventHash, ledgerNode});
    if(events.some(({event: {type}}) => type === 'WebLedgerOperationEvent')) {
      return {mergeable: false, ...status};
    }
  }

  return {mergeable: true, ...status};
};

async function _genesisProofCreate({ledgerNode, eventHash}) {
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash: [eventHash],
  };
  const ledgerNodeId = ledgerNode.id;
  const creator = await _voters.get({ledgerNodeId, publicKey: true});
  const mergeEvent = await _signature.sign({event, ledgerNodeId});
  const mergeHash = await _util.hasher(mergeEvent);
  return {creator, mergeEvent, mergeHash};
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
  const creator = await _voters.get({ledgerNodeId});
  const meta = {
    consensus: false,
    continuity2017: {creator: creator.id, type: 'r'},
    eventHash,
  };
  if(hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
    meta.continuity2017.type = 'c';
  }
  return {event, meta};
}

async function _hashOperations({ledgerNode, operations}) {
  const limit = pLimit(100);
  return Promise.all(operations.map(operation =>
    limit(async () => {
      await pImmediate();
      const {hash: operationHash, canonizedBytes} =
        await _util.rdfCanonizeAndHash(operation);
      const {operations: {maxBytes}} = _continuityConstants;
      if(canonizedBytes > maxBytes) {
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
    })
  ));
}

async function _processPeerConfigurationEvent({event}) {
  const eventHash = await _util.hasher(event);
  const meta = {consensus: false, continuity2017: {type: 'c'}, eventHash};
  return {event, meta};
}

async function _processPeerRegularEvent({event, ledgerNode}) {
  const _event = bedrock.util.clone(event);
  const {basisBlockHeight, operation: ops} = _event;
  delete _event.operation;
  const operationRecords = await _hashOperations(
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
    _voters.get({ledgerNodeId}),
    // Note: signature.verify throws if signature is invalid
    _signature.verify({event})
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
  if(hasValue(event, 'type', 'WebLedgerOperationEvent')) {
    return _processPeerRegularEvent({event, ledgerNode});
  }
  if(hasValue(event, 'type', 'ContinuityMergeEvent')) {
    return _processPeerMergeEvent({event, eventMap, ledgerNode});
  }
  if(hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
    return _processPeerConfigurationEvent({event});
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
  const {id: localCreatorId} = await _voters.get({ledgerNodeId});

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
      // used to track events from parentHash that are inside this batch
      const insideBatch = [];
      for(const ancestorHash of event.parentHash) {
        let ancestorRecord;
        let outsideBatch = false;
        ancestorRecord = eventMap.get(ancestorHash);
        if(!ancestorRecord) {
          outsideBatch = true;
          [ancestorRecord] = await api.getEvents(
            {eventHash: ancestorHash, ledgerNode});
        } else {
          insideBatch.push(ancestorHash);
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
        } else if(!outsideBatch) {
          // regular and configuration events types c || r
          // some regular or configuration events may have been found outside
          // this batch, in the event pipeline. It is possible that a regular
          // event gets recorded without its corresponding merge event if the
          // gossip session times out before all deferred events are processed.
          // In this case, the merge event would include regular events with
          // multiple basisBlockHeight values. During gossip processing, some
          // of the events were allowed to pass, while those with greater
          // basisBlockHeight values were deferred along with the merge event.
          // It is then possibile that the gossip session times out before
          // all the deferred events are processed. In that case the remaining
          // deferred events will be discarded and then reacquired during
          // the next gossip session.

          // if a regular or configuration event was found outside this batch
          // it already passed these tests

          // it is common to find merge events and regular events that are
          // sibilings in `parentHash`, but they must have the same creator
          // with the exception of events based on the genesis merge event
          if(treeHash !== mergeTreeHash) {
            let directAncestor;
            // get the direct ancestor of the ancestor
            directAncestor = eventMap.get(treeHash);
            if(!directAncestor) {
              [directAncestor] = await api.getEvents(
                {eventHash: treeHash, ledgerNode});
            }
            const {meta: {continuity2017: {creator, type, generation}}} =
              directAncestor;
            if(type !== 'm') {
              throw new BedrockError(
                'Peers must base regular events on a merge event.',
                'DataError', {
                  ancestorRecord,
                  directAncestor,
                  eventRecord,
                  httpStatusCode: 400,
                  public: true,
                });
            }
            // creators must match after the genesis merge event (generation 0)
            if(generation > 0 && creator !== requiredCreator) {
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
      _temp.insideBatch = insideBatch;
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
    _signature.verify({event: mergeEvent}),
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
  const isConfig = hasValue(event, 'type', 'WebLedgerConfigurationEvent');
  await _cache.events.addLocalRegularEvent({eventHash, ledgerNodeId, isConfig});

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
