/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const _operations = require('./operations');
const _localPeers = require('./localPeers');
const _signature = require('./signature');
const _util = require('./util');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {util: {hasValue, BedrockError}} = bedrock;
const {config} = bedrock;
const database = require('bedrock-mongodb');
const logger = require('./logger');
const LRU = require('lru-cache');

const api = {};
module.exports = api;

let GOSSIP_EVENT_CACHE;
let GOSSIP_EVENT_PROMISE_CACHE;
let WITHHELD_EVENT_CACHE;

bedrock.events.on('bedrock.init', () => {
  // FIXME: determine if we want this cache to live across ledger nodes or
  // if we want it to be per ledger node; it is key'd off of event hashes so
  // they should be unique, however, sharing the cache would mean enabling
  // ledger nodes that do not actually have certain events to serve them and
  // that if any ledger node has a bug, they all will; however, they are all
  // running on the same software anyway
  // FIXME: determine cache size by using string length; note: that is not
  // byte size, but character size -- international chars will use more space
  GOSSIP_EVENT_CACHE = new LRU({max: 1000});
  /*GOSSIP_EVENT_CACHE = new LRU({
    // `max` is maximum number of total characters since `length` returns
    // character length for the items
    // FIXME: use `bedrock.config` for this
    // 500 MiB
    max: 1024 * 1024 * 500,
    length(item) {
      return item.length;
    }
  });*/

  /* This cache is not shared across ledger nodes because it contains promises
  that must be executed against specific ledger node storage; if a promise was
  shared between two or more ledger nodes with different storage, it would
  resolve with the wrong result. */
  // FIXME: configure using `bedrock.config` ... how many promises for
  // specific events should it be able to hold?
  GOSSIP_EVENT_PROMISE_CACHE = new LRU({max: 10000});

  /* This cache is not shared across ledger nodes because it contains withheld
  events that are specific to each ledger node's current consensus state. It
  exists to ensure that the withheld events that a peer has committed to can
  persist across work sessions until consensus is reached on the commitment,
  without having to store the events in the database. This is vital to
  preventing long-lived spam as the withheld events may be detected as spam
  when the commitment reaches consensus, which should result in them being
  dropped rather than stored.

  Note that if different processes service the same ledger node then the
  withheld events will need to be committed to and downloaded again if a
  different process takes over the work session that does not have the
  withheld events in its cache for the related ledger node.

  This cache is only read from when work sessions start and, provided that
  a work session is sufficiently long, there should low risk to having to
  repeat work when scheduled across different processes.

  This cache is key'd off of ledger node ID; each ledger node gets to store
  one set of withheld events. Withheld events can store up to:

  MAX_OPS * OP_SIZE + MERGE_EVENT_OVERHEAD.

  For example, with MAX_OPS = 60, OP_SIZE = 4 KiB, and
  MERGE_EVENT_OVERHEAD = 1600 bytes, each entry would hold ~240 KiB, so
  a cache size of 100 would only take ~24 MiB. This allows for up to
  100 ledger nodes to store the withheld events they are committing to which
  should be more than sufficient. In many setups, there will only be one
  ledger node running per process. */
  // FIXME: configure using `bedrock.config`
  WITHHELD_EVENT_CACHE = new LRU({max: 100});
});

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

  // Note: The following code only executes for the *genesis* block.

  // create the local peer ID for the ledger node if not already created
  try {
    await _localPeers.generate({ledgerNodeId: ledgerNode.id});
  } catch(e) {
    if(e.name !== 'DuplicateError') {
      throw e;
    }
  }

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
      // FIXME: consensusProof should be removed
      consensusProof: [mergeEventRecord.event],
      blockHeight: 0
    };
  }

  const creator = mergeEventRecord.meta.continuity2017.creator;
  await _blocks.writeGenesis({block: genesisBlock, creator, ledgerNode});

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

  const {operationQueue: queue} = worker;
  if(!await queue.hasNextChunk()) {
    logger.debug('No new operations.');
    return {hasMore: false};
  }

  // get chunk of operations to put into the event
  const {hasMore, operations} = await queue.getNextChunk();

  const {head} = worker;
  const baseEvent = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerOperationEvent',
    basisBlockHeight: worker.consensusState.blockHeight,
    parentHash: [head.eventHash],
    treeHash: head.eventHash
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
  await api.add({event, eventHash, ledgerNode, worker});

  // event successfully written, can now pop the chunk off the queue
  await queue.popChunk();

  // return whether or not there are more operations and the `eventHash` of
  // the created event
  return {hasMore, eventHash};
};

api.getEventsForGossip = async ({eventHashes, ledgerNodeId}) => {
  // prepare to fetch any events from LRU/database that do not already
  // have a promise associated with them
  const toFetch = [];
  // create a new promise that will be placed on the microtask queue. It
  // will not be resolved until the current execution contexts yields
  // execution, in this case, via `await`. The `eventHashesPromise`
  // will be awaited in `_deferredGetEventsForGossip` in the next tick,
  // which will allow the loop below to fill the `toFetch` array with
  // event hashes that do not yet have a pending promise
  const eventHashesPromise = Promise.resolve(toFetch);
  // this promise will not be awaited until `toFetch` is populated below
  const eventMapPromise = _deferredGetEventsForGossip(
    {ledgerNodeId, eventHashesPromise});

  // keep track of total promises and those promises that we add ourselves
  // (`managedPromises`) as we need to remove them from the LRU cache
  const promises = [];
  const managedPromises = new Map();
  for(const eventHash of eventHashes) {
    // cached promises must be bound to a specific ledger node because
    // different ledger nodes have different event storage
    const key = `${eventHash}|${ledgerNodeId}`;
    let promise = GOSSIP_EVENT_PROMISE_CACHE.get(key);
    if(promise) {
      promises.push(promise);
      continue;
    }
    toFetch.push(eventHash);
    promise = _deferredGetEventForGossip({eventHash, eventMapPromise});
    GOSSIP_EVENT_PROMISE_CACHE.set(key, promise);
    promises.push(promise);
    managedPromises.set(key, promise);
  }

  // ensure all managed promises have settled, removing any from the cache
  // that throw errors; any errors will be rethrown when awaiting `promises`
  for(const [key, managedPromise] of managedPromises) {
    try {
      await managedPromise;
    } catch(e) {
      GOSSIP_EVENT_PROMISE_CACHE.del(key);
    }
  }

  // await all promises and filter out `undefined` results
  const eventJsons = await Promise.all(promises);
  const results = [];
  for(const eventJson of eventJsons) {
    if(eventJson) {
      results.push(eventJson);
    }
  }
  return results;
};

api.getEvents = async ({worker, eventHash}) => {
  let eventHashes;
  if(Array.isArray(eventHash)) {
    eventHashes = eventHash;
  } else {
    eventHashes = [eventHash];
  }

  // FIXME: if the new flow is such that the number of uncommitted events
  // is always zero -- and remove `_getUncommittedEvents` function from worker

  // use worker's uncommitted events if given
  let needed;
  let uncommittedEvents;
  if(worker) {
    needed = [];
    uncommittedEvents = worker._getUncommittedEvents({eventHashes});
    for(const eventHash of eventHashes) {
      if(!uncommittedEvents.has(eventHash)) {
        needed.push(eventHash);
      }
    }
  } else {
    needed = eventHashes;
    uncommittedEvents = new Map();
  }

  // FIXME: add an LRU cache?

  // return events in order
  const {ledgerNode} = worker;
  const records = await ledgerNode.storage.events.getMany(
    {eventHashes: needed}).toArray();
  const eventMap = new Map();
  for(const record of records) {
    eventMap.set(record.meta.eventHash, record);
  }
  const results = [];
  for(const eventHash of eventHashes) {
    const record = eventMap.get(eventHash) || uncommittedEvents.get(eventHash);
    if(record) {
      const {event, meta} = record;
      results.push({event, meta});
    }
  }
  return results;
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
// gets non-consensus events that can eventually be merged (excludes any
// events by detected replayers)
api.getNonConsensusEvents = async ({ledgerNode, basisBlockHeight}) => {
  // Note: If `replayDetectedBlockHeight <= basisBlockHeight` then the events
  // can never be legally merged; omit these from valid non-consensus events
  return ledgerNode.storage.events.collection.find({
    'meta.consensus': false,
    $or: [
      {'meta.continuity2017.replayDetectedBlockHeight': -1},
      {'meta.continuity2017.replayDetectedBlockHeight': {
        $gt: basisBlockHeight
      }}
    ]
  }).project({
    // FIXME: check to see if this is still a covered query or if we need it
    // it to be (it may not be called often enough)
    _id: 0,
    'event.basisBlockHeight': 1,
    'event.mergeHeight': 1,
    'event.parentHash': 1,
    'event.parentHashCommitment': 1,
    'event.treeHash': 1,
    'meta.eventHash': 1,
    'meta.continuity2017.creator': 1,
    'meta.continuity2017.generation': 1,
    'meta.continuity2017.isLocalContributor': 1,
    'meta.continuity2017.lastLocalContributor': 1,
    'meta.continuity2017.localAncestorGeneration': 1,
    'meta.continuity2017.localReplayNumber': 1,
    'meta.continuity2017.localEventNumber': 1,
    'meta.continuity2017.replayDetectedBlockHeight': 1,
    'meta.continuity2017.type': 1
  }).toArray();
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
// gets the latest valid commitment that has reached consensus for the given
// creator, provided that it was for an event created by a peer that has not
// been detected as a replayer; this is for restoring `mergeCommitment` state
// when a worker initializes
api.getLatestParentHashCommitment = async (
  {ledgerNode, creator, minGeneration}) => {
  // fetch latest consensus parent hash commitment from creator
  const [record] = await ledgerNode.storage.events.collection.find({
    'meta.continuity2017.type': 'm',
    'meta.continuity2017.creator': creator,
    'meta.continuity2017.generation': {$gte: minGeneration},
    'meta.consensus': true,
    'meta.continuity2017.hasParentHashCommitment': true,
  }).sort({'meta.continuity2017.generation': -1}).limit(1).project({
    _id: 0,
    'meta.continuity2017.generation': 1,
    'event.parentHashCommitment': 1,
    'meta.eventHash': 1
  }).toArray();
  if(!record) {
    return null;
  }

  // ensure the commitment is for a non-replayer
  const {
    event: {parentHashCommitment},
    meta: {eventHash, continuity2017: {generation}}
  } = record;
  const [eventRecord] = await ledgerNode.storage.events.collection.find({
    'meta.eventHash': parentHashCommitment[0]
  }).project({
    _id: 0,
    'meta.blockHeight': 1,
    'meta.continuity2017.replayDetectedBlockHeight': 1
  }).limit(1).toArray();
  if(!eventRecord) {
    return null;
  }

  const result = {eventHash, generation, parentHashCommitment};

  const {
    meta: {blockHeight, continuity2017: {replayDetectedBlockHeight}}
  } = eventRecord;
  if(replayDetectedBlockHeight === -1) {
    // did not commit to a replayer
    return result;
  }
  if(blockHeight === -1 || blockHeight >= replayDetectedBlockHeight) {
    // committed to a replayer
    return null;
  }
  // did not commit to a replayer
  return result;
};

api.setWithheld = ({ledgerNode, withheld} = {}) => {
  if(withheld) {
    WITHHELD_EVENT_CACHE.set(ledgerNode.id, withheld);
    return;
  }
  WITHHELD_EVENT_CACHE.del(ledgerNode.id);
};

api.getWithheld = ({ledgerNode} = {}) => {
  return WITHHELD_EVENT_CACHE.get(ledgerNode.id);
};

async function _deferredGetEventForGossip({eventHash, eventMapPromise}) {
  const eventMap = await eventMapPromise;
  return eventMap.get(eventHash);
}

async function _deferredGetEventsForGossip({
  ledgerNodeId, eventHashesPromise
} = {}) {
  const eventMap = new Map();

  const eventHashes = await eventHashesPromise;
  if(eventHashes.length === 0) {
    // nothing to lookup, return early
    return eventMap;
  }

  // first check LRU cache
  const notFound = [];
  for(const eventHash of eventHashes) {
    const eventJson = GOSSIP_EVENT_CACHE.get(eventHash);
    if(eventJson) {
      eventMap.set(eventHash, eventJson);
    } else {
      notFound.push(eventHash);
    }
  }

  // all events found in the LRU cache, return early
  if(notFound === 0) {
    return eventMap;
  }

  // get remaining events from the database, stringify them, cache, and return
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
  const records = await ledgerNode.storage.events.getMany(
    {eventHashes: notFound}).toArray();
  for(const {event, meta} of records) {
    const eventJson = JSON.stringify({event});
    eventMap.set(meta.eventHash, eventJson);
    GOSSIP_EVENT_CACHE.set(meta.eventHash, eventJson);
  }
  return eventMap;
}

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
  const {signed: mergeEvent, peerId: creator} = await _signature.sign(
    {event, ledgerNodeId});
  const mergeHash = await _util.hasher(mergeEvent);
  return {creator, mergeEvent, mergeHash};
}

async function _addContinuityIndexes({ledgerNode}) {
  const {id: ledgerNodeId} = ledgerNode;
  const localPeerId = await _localPeers.getPeerId({ledgerNodeId});

  // FIXME: determine the right way to handle this index. It is exceptional
  // because it requires knowledge of `localPeerId`. Perhaps the
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
        'meta.continuity2017.creator': localPeerId
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
          updated: now
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
      creator,
      generation: 0,
      hasParentHashCommitment: false,
      // genesis event contributed the ledger configuration
      isLocalContributor: true,
      lastLocalContributor: mergeHash,
      localAncestorGeneration: 0,
      localEventNumber: 1,
      localReplayNumber: 0,
      replayDetectedBlockHeight: -1,
      requiredBlockHeight: 0,
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

  const {storage} = ledgerNode;

  // process event (create `meta`, do any extra validation, etc.)
  let eventRecord;
  if(!eventHash) {
    throw new TypeError('"eventHash" argument is required.');
  }
  if(genesis) {
    eventRecord = {
      event,
      meta: {
        eventHash,
        effectiveConfiguration: true,
        continuity2017: {
          type: 'c',
          localEventNumber: 0,
          replayDetectedBlockHeight: -1,
          requiredBlockHeight: 0
        }
      }
    };
  } else {
    eventRecord = await _createLocalEventRecord({worker, event, eventHash});
  }
  // only local regular events pass here
  try {
    eventRecord = await storage.events.add(eventRecord);
  } catch(e) {
    // ignore duplicates when creating the event; we may be recovering from a
    // previous failed attempt to initialize the ledger node
    if(!(genesis && e.name === 'DuplicateError')) {
      throw e;
    }
  }

  // finished if writing genesis config event
  if(genesis) {
    return eventRecord;
  }

  /* Note: The worker state needs to be updated here as a local event was
  committed to storage and it now needs to be merged. If the worker state is
  not updated, then a merge event could be created that excludes it, causing
  all operations in the regular event to be lost or causing a subsequent merge
  event to be invalid by merging in a regular event that is not its sibling. If
  this update fails, the work session should exit and it will be corrected when
  the new work session begins. Every new work session creates a new worker
  state that is in sync the database. */
  worker.pendingLocalRegularEventHashes.add(eventHash);

  return eventRecord;
}

async function _createLocalEventRecord({worker, event, eventHash}) {
  const {consensusState, localPeerId} = worker;
  // if peer is a witness, this is known to be the current block height,
  // otherwise, it will not be known until either a commitment for the
  // merge event that merges this event reaches consensus or if the peer
  // becomes a witness on a subsequent block
  const {blockHeight, witnesses} = consensusState;
  const requiredBlockHeight = witnesses.has(localPeerId) ?
    blockHeight : -1;
  const meta = {
    blockHeight: -1,
    consensus: false,
    continuity2017: {
      creator: localPeerId,
      type: 'r',
      localEventNumber: worker.nextLocalEventNumber++,
      replayDetectedBlockHeight: -1,
      requiredBlockHeight
    },
    eventHash
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
  const [{controller: creator}, proofHash, mergeHash] = await Promise.all([
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
