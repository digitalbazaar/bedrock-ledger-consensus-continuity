/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const _cache = require('./cache');
const _history = require('./history');
const _operations = require('./operations');
const _peers = require('./peers');
const _signature = require('./signature');
const _util = require('./util');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {util: {hasValue, BedrockError}} = bedrock;
const {config} = bedrock;
const database = require('bedrock-mongodb');
const logger = require('./logger');

const api = {};
module.exports = api;

/**
 * Adds a new event. If adding a non-genesis event, this must be called
 * from within a work session, passing the `worker` that is running the
 * session.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.add = async (
  {event, eventHash, genesis, genesisBlock, ledgerNode, worker}) => {
  if(genesisBlock) {
    // do not mutate genesisBlock
    genesisBlock = bedrock.util.clone(genesisBlock);
  }
  const configEventRecord = await _writeEvent(
    {event, eventHash, genesis, ledgerNode, worker});
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
};

// TODO: document (create a regular local event w/operations)
// if adding a non-genesis event, this must be called from within a work
// session, passing the `worker` that is running the session
api.create = async ({ledgerNode, worker}) => {
  logger.verbose('Attempting to create an operation event.');

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
      _peers.get({ledgerNodeId})
        .then(({id: creatorId}) => _history.getHead({creatorId, ledgerNode})),
    ]);

  const baseEvent = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerOperationEvent',
    basisBlockHeight,
    parentHash: [headHash],
    treeHash: headHash
  };

  // the event with all operation documents goes into the cache for gossip
  const cacheEvent = {
    ...baseEvent,
    operation: operations.map(op => op.operation)
  };

  // create the event and its hash
  const event = {
    ...baseEvent,
    operationHash: operations.map(op => op.meta.operationHash)
  };

  const eventHash = await _util.hasher(event);

  // store the operations first (if this fails, the same event hash will be
  // generated on retry)
  await _operations.write({eventHash, ledgerNode, operations});

  // store the event
  await Promise.all([
    // FIXME: do we want to do this before it has been merged? we should not
    // gossip something that doesn't have a merge event to go along with it
    // as it cannot be fully validated; but maybe that is prevented somewhere
    // via gossip rules anyhow -- if so, we should update this comment to that

    // add the event to the cache for gossip purposes
    _cache.events.setEventGossip(
      {event: cacheEvent, eventHash, ledgerNodeId, meta: {
        continuity2017: {type: 'r'},
        eventHash
      }}),
    api.add({event, eventHash, ledgerNode, worker}),
  ]);

  // event successfully written, can now pop the chunk off the queue
  await queue.popChunk();

  // return whether or not there are more operations and the `eventHash` of
  // the created event
  return {hasMore, eventHash};
};

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

  return [...hashMap.values()];
};

// FIXME: remove this entirely once server can cold-load events from database
api.repairCache = async ({eventHash, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const eventRecord = await ledgerNode.storage.events.get(eventHash);
  // FIXME: update tests to make return value make more sense
  const updateCache = await _cache.events.addLocalMergeEvent(
    {...eventRecord, ledgerNodeId});
  return {updateCache};
};

async function _genesisProofCreate({ledgerNode, eventHash}) {
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    // `basisBlockHeight` is always 0 for genesis merge events
    basisBlockHeight: 0,
    // `mergeHeight` is always 0 for genesis merge events
    mergeHeight: 0,
    parentHash: [eventHash],
  };
  const ledgerNodeId = ledgerNode.id;
  const creator = await _peers.get({ledgerNodeId, publicKey: true});
  const mergeEvent = await _signature.sign({event, ledgerNodeId});
  const mergeHash = await _util.hasher(mergeEvent);
  return {creator, mergeEvent, mergeHash};
}

async function _addContinuityIndexes({ledgerNode}) {
  const {id: ledgerNodeId} = ledgerNode;
  const {id: localCreatorId} = await _peers.get({ledgerNodeId});

  // FIXME: determine the right way to handle this index. It is exceptional
  // because it requires knowledge of `localCreatorId`. Perhaps the
  // continuity-storage plugin can provide an `addIndex` method.

  // add indexes specific to Continuity
  const collection = ledgerNode.storage.events.collection.collectionName;
  await database.createIndexes([{
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
    continuity2017: {
      creator: creator.id,
      generation: 0,
      localAncestorGeneration: 0,
      type: 'm'
    },
    created: now,
    updated: now,
    eventHash: mergeHash
  };
  const mergeEventRecord = await ledgerNode.storage.events.add({event, meta});
  return {mergeEventRecord, creator};
}

// eventHash is precomputed and passed in for local regular events
async function _writeEvent({event, eventHash, genesis, ledgerNode, worker}) {
  // non-genesis events must be added from within a work session; the
  // presence of `worker` implies this function is being called from within
  // a work session or by a unit test; adding non-genesis events outside of
  // a work session could cause database corruption and invalidation of the
  // peer on the network because a concurrently running work session assumes
  // it is the only process adding events
  if(!genesis && !worker) {
    throw new Error(
      '"worker" is required to add non-genesis events. Non-genesis events ' +
      'must not be added outside of a work session.');
  }

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
    // FIXME: this is not valid; adding local events outside of a work session
    // is not permitted except with the genesis event
    eventRecord = await _createLocalEventRecord({event, eventHash, ledgerNode});
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

  /* Note: The worker state needs to be updated here as a local event was
  committed to storage and it now needs to be merged; if the worker state is
  not updated, then a merge event could be created that excludes it, causing
  all operations in the regular event to be lost or causing a subsequent merge
  event to be invalid by merging in a regular event that is not its sibling; if
  this update fails, the work session should exit and it will be corrected when
  the new work session begins. Every new work session creates a new worker
  state that is in sync the database. */
  worker.pendingLocalRegularEventHashes.add(eventHash);
  const isConfig = hasValue(event, 'type', 'WebLedgerConfigurationEvent');
  await _cache.events.addLocalRegularEvent({eventHash, ledgerNodeId, isConfig});

  return eventRecord;
}

async function _createLocalEventRecord({event, eventHash, ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;
  const creator = await _peers.get({ledgerNodeId});
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
