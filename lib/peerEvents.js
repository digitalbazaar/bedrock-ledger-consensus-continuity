/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _continuityConstants = require('./continuityConstants');
const _events = require('./events');
const _history = require('./history');
const _peers = require('./peers');
const _signature = require('./signature');
const _util = require('./util');
const bedrock = require('bedrock');
const brJobs = require('bedrock-jobs');
const brLedgerNode = require('bedrock-ledger-node');
const {config, util: {hasValue, BedrockError}} = bedrock;
const logger = require('./logger');

const api = {};
module.exports = api;

let jobQueue;

bedrock.events.on('bedrock.start', async () => {
  jobQueue = brJobs.addQueue({name: 'continuity-event-validation'});
  const cfg = config['ledger-consensus-continuity'];

  // setup Bedrock worker to process gossip events
  if(cfg.gossip.batchProcess.enable) {
    const gossipProcessingConcurrency =
      cfg.gossip.batchProcess.concurrentEventsPerWorker;
    jobQueue.process(gossipProcessingConcurrency, async opts => {
      try {
        const result = await _validateEvent(opts);
        return result;
      } catch(e) {
        logger.error('An error occurred during gossip processing.', {error: e});
        throw e;
      }
    });
  }
});

api.addBatch = async ({blockHeight, events, ledgerNode, needed}) => {
  const ledgerNodeId = ledgerNode.id;
  let mergePermitsConsumed = 0;
  const {eventMap} = await _validateEvents({
    blockHeight, events, ledgerNode, needed
  });

  const deferredEvents = [];
  for(const [eventHash, {event, meta, rawEvent, _temp}] of eventMap) {
    const {valid, requiredBlockHeight} = _temp;
    if(!valid) {
      throw new BedrockError(
        'An unrelated event was detected in the batch.', 'DataError', {
          // eventRecord,
          httpStatusCode: 400,
          public: true,
        });
    }
    if(requiredBlockHeight) {
      deferredEvents.push({eventHash, requiredBlockHeight});
      // remove this merge event from the eventMap
      eventMap.delete(eventHash);
      continue;
    }

    if(meta.continuity2017.type === 'm') {
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
          eventHash => eventMap.has(eventHash))) {
          // some ancestors were excluded from processing
          // remove this merge event from the eventMap
          eventMap.delete(eventHash);
        }
        continue;
      }
      mergePermitsConsumed++;
    }

    // place the unaltered rawEvent into the cache for future gossip
    await _cache.events.setEventGossip({
      event: rawEvent,
      eventHash,
      ledgerNodeId,
      meta
    });
    await _cache.events.addPeerEvent({event, ledgerNodeId, meta});
  }

  return {deferredEvents, mergePermitsConsumed};
};

api.createPeerEventRecord = async function({event, eventMap, ledgerNode}) {
  if(hasValue(event, 'type', 'WebLedgerOperationEvent')) {
    return _createPeerRegularEventRecord({event, ledgerNode});
  }
  if(hasValue(event, 'type', 'ContinuityMergeEvent')) {
    return _createPeerMergeEventRecord({event, eventMap, ledgerNode});
  }
  if(hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
    return _createPeerConfigurationEventRecord({event});
  }
  throw new BedrockError(
    'Unknown event type.',
    'DataError', {
      event,
      httpStatusCode: 400,
      public: true,
    });
};

async function _validateEvents({blockHeight, events, ledgerNode, needed}) {
  const ledgerNodeId = ledgerNode.id;

  // FIXME: temporary fix for clearing completed jobs
  // clears jobs that were *completed* more than 5 seconds ago
  // no need to await this
  jobQueue.clean(5000);

  // FIXME: use removeOnComplete when fixed
  // see: https://github.com/OptimalBits/bull/issues/1906
  const jobDefinitions = events.map(event => ({
    data: {blockHeight, event, ledgerNodeId},
    // max time it should take to validate an event
    opts: {timeout: 5000},
  }));
  const jobs = await jobQueue.addBulk(jobDefinitions);

  // job.finished() returns a promise that resolves when the job completes
  const results = await Promise.all(jobs.map(job => job.finished()));

  const eventMap = new Map();
  const eventHashes = [];
  const neededSet = new Set(needed);
  let index = 0;
  for(const {event, requiredBlockHeight, meta} of results) {
    const {eventHash} = meta;
    eventHashes.push(eventHash);
    const _temp = {valid: false, requiredBlockHeight};
    eventMap.set(eventHash, {
      event,
      meta,
      rawEvent: events[index++],
      _temp
    });

    // populate `generation` for merge events, some merge events may have
    // immediate ancestors inside the batch which are found in `eventMap`
    if(meta.continuity2017.type === 'm') {
      const parentGeneration = await _getGeneration(
        {eventHash: event.treeHash, eventMap, ledgerNode});
      meta.continuity2017.generation = parentGeneration + 1;
    }

    // if delete returns false, the eventHash was not present in the set
    if(!neededSet.delete(meta.eventHash)) {
      throw new BedrockError(
        'The event supplied by the peer was not requested.',
        'DataError', {event, eventHash, neededSet});
    }
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

  // inspect all the provided events
  await _validateGraph({eventHashes, eventMap, ledgerNode});

  return {eventHashes, eventMap};
}

// called by a bedrock-job, job contains `data` payload
async function _validateEvent({data: {blockHeight, event, ledgerNodeId}}) {
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);

  const {event: processedEvent, meta} = await api.createPeerEventRecord(
    {event, ledgerNode});

  const {basisBlockHeight} = event;
  // regular events and configuration events have basisBlockHeight
  if(Number.isInteger(basisBlockHeight) && basisBlockHeight > blockHeight) {
    // do not attempt to validate yet
    return {event: processedEvent, requiredBlockHeight: basisBlockHeight, meta};
  }

  // events type could be regular 'r', configuration 'c' or merge events 'm'
  if(meta.continuity2017.type === 'r') {
    await _util.processChunked({
      tasks: processedEvent.operationRecords,
      fn: _validateOperation,
      chunkSize: 25,
      args: [ledgerNode, basisBlockHeight]
    });
  } else if(meta.continuity2017.type === 'c') {
    const result = await ledgerNode.config.validate({
      basisBlockHeight, ledgerConfiguration: event.ledgerConfiguration,
      ledgerNode
    });
    if(!result.valid) {
      throw result.error;
    }
  }

  return {event: processedEvent, meta};
}

async function _createPeerConfigurationEventRecord({event}) {
  const eventHash = await _util.hasher(event);
  const meta = {consensus: false, continuity2017: {type: 'c'}, eventHash};
  return {event, meta};
}

async function _createPeerRegularEventRecord({event, ledgerNode}) {
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

async function _createPeerMergeEventRecord({event, ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;

  const [
    eventHash,
    {id: localCreatorId},
    {keyOwner: {id: creator}}
  ] = await Promise.all([
    _util.hasher(event),
    _peers.get({ledgerNodeId}),
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

  const meta = {
    consensus: false,
    // generation will be assessed later
    continuity2017: {creator, generation: null, type: 'm'},
    eventHash,
  };
  return {event, meta};
}

async function _hashOperations({ledgerNode, operations}) {
  const hashedOperations = await _util.processChunked({
    tasks: operations, fn: _hashOperation, chunkSize: 25
  });

  for(const operation of hashedOperations) {
    // the `recordId` property is indexed in the storage layer
    const recordId = _util.generateRecordId({
      ledgerNode, operation: operation.operation
    });
    operation.recordId = recordId;
  }

  return hashedOperations;
}

async function _hashOperation(operation) {
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
  // `recordId` must be added a later point
  return {meta: {operationHash}, operation, recordId: ''};
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
          [ancestorRecord] = await _events.getEvents(
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
              [directAncestor] = await _events.getEvents(
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
                'the merge event.', 'SyntaxError', {
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

async function _validateOperation(
  {operation}, ledgerNode, basisBlockHeight) {
  const result = await ledgerNode.operations.validate(
    {basisBlockHeight, ledgerNode, operation});
  if(!result.valid) {
    throw result.error;
  }
}

async function _getGenesisCreator({ledgerNode}) {
  const {eventHash: genesisHeadHash} = await _history.getGenesisHead(
    {ledgerNode});
  const [{meta: {continuity2017: {creator: genesisCreator}}}] =
    await _events.getEvents({eventHash: genesisHeadHash, ledgerNode});
  return genesisCreator;
}
