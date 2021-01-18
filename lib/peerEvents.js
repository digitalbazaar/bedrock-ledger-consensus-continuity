/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _client = require('./client');
const _continuityConstants = require('./continuityConstants');
const _events = require('./events');
const _peers = require('./peers');
const _signature = require('./signature');
const _util = require('./util');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {config, util: {hasValue, BedrockError}} = bedrock;
const logger = require('./logger');

const api = {};
module.exports = api;

api.addBatch = async ({worker, events, neededHashes}) => {
  let mergeEventsReceived = 0;
  const {valid, error, eventMap} = await _validateEvents(
    {worker, events, neededHashes});
  if(!valid) {
    return {valid, error, mergeEventsReceived};
  }

  for(const {event, meta, _temp} of eventMap.values()) {
    const {valid} = _temp;
    if(!valid) {
      // FIXME: We should reject the whole batch here because the remote peer
      // is not following protocol. They need to fix that or we won't talk to
      // them again. We may want to accept what they sent, however, so we don't
      // have to request it again -- so do we want to make a note that the peer
      // misbehaved but keep what it sent? note that what we have that is
      // valid can be accepted due to new _validateGraph algorithm that ensures
      // we breadth-first validate
      continue;
    }

    if(meta.continuity2017.type === 'm') {
      mergeEventsReceived++;
    }

    // add `localEventNumber` and add event to be written in the next batch
    meta.continuity2017.localEventNumber = worker.nextLocalEventNumber++;
    await worker.peerEventWriter.add({event, meta});
  }

  return {valid: true, mergeEventsReceived};
};

api.createPeerEventRecord = async function({event, ledgerNode}) {
  if(hasValue(event, 'type', 'WebLedgerOperationEvent')) {
    return _createPeerRegularEventRecord({event, ledgerNode});
  }
  if(hasValue(event, 'type', 'ContinuityMergeEvent')) {
    return _createPeerMergeEventRecord({event, ledgerNode});
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

async function _validateEvents({worker, events, neededHashes}) {
  const {blockHeight} = worker.consensusState;
  const eventMap = new Map();
  const eventHashes = [];

  try {
    // FIXME: Use a value from the config to limit the number of in-flight
    // requests to events validation service
    const tasks = [];
    for(const event of events) {
      // event must have a valid `basisBlockHeight` that is not ahead of our
      // current blockHeight, otherwise it gets dropped; note that the peer
      // should not send us these events if it is following the gossip protocol
      // but we have to account for such violations
      const {basisBlockHeight} = event;
      if(!(Number.isInteger(basisBlockHeight) &&
        basisBlockHeight <= blockHeight)) {
        // the server MUST NOT send us data we didn't ask for and a peer that
        // does that should not be gossipped with; it is violating protocol,
        // so we do not even bother validating the events here
        throw new BedrockError(
          'The event supplied by the peer had a "basisBlockHeight" that ' +
          'was invalid or not requested.',
          'DataError', {event, basisBlockHeight});
      }

      // add event for validation
      tasks.push({worker, event, totalEventCount: events.length});
    }

    const {'ledger-consensus-continuity': {
      gossip: {eventsValidation}
    }} = config;

    // concurrency should be calculated based on the number of workers
    // and the maximum number of concurrent validation operations each of
    // them should perform
    // FIXME: need to ensure better load balancing with the workers and allow
    // for the number of workers to dynamically grow/shrink
    let chunkSize;
    const concurrency = chunkSize = eventsValidation.workers *
      eventsValidation.concurrency;

    let results;
    try {
      results = await _util.processChunked({
        tasks,
        fn: _validateEventViaService,
        concurrency,
        chunkSize
      });
    } catch(e) {
      logger.error('An error occurred during gossip processing.', {error: e});
      throw e;
    }

    // gather all of the validated events; at this point operations and
    // configurations in events have been validated based on ledger validators,
    // but DAG merge history has not yet been validated
    const neededSet = new Set(neededHashes);
    for(const {event, meta} of results) {
      const {eventHash} = meta;
      // if any event in the batch was not requested (or it appears so because
      // the computed hash does not match), then throw out entire batch
      if(!neededSet.has(eventHash)) {
        throw new BedrockError(
          'The event supplied by the peer was not requested.',
          'DataError', {event, eventHash, neededSet});
      }
      eventHashes.push(eventHash);
      // FIXME: we can remove `processed` and `valid` here -- instead always
      // throwing within `_validateGraph` if there are any problems and
      // rejecting the entire batch
      const _temp = {valid: false, processed: false};
      eventMap.set(eventHash, {event, meta, _temp});
    }

    // ensure that all the needed events are included in the batch
    if(neededSet.size !== eventHashes.length) {
      // FIXME: do we want to reject the whole batch on this basis or accept
      // what we have and simply report we didn't get every thing we asked for?
      // ... note that the new `_validateGraph` will ensure that we don't mark
      // ... any events valid if their parents are missing/not valid
      /*
      throw new BedrockError(
        'The batch does not include all the needed events.',
        'DataError', {
          httpStatusCode: 400,
          missingEventHashes: [...neededSet],
          public: true,
        });
      */
    }

    // inspect all the provided events; this will also set
    // `localAncestorGeneration` for each merge event
    await _validateGraph({worker, eventMap});
  } catch(e) {
    // do not throw error, just indicate that batch is invalid; throwing an
    // error would unnecessarily terminate the work session
    return {valid: false, error: e, eventHashes, eventMap};
  }

  return {valid: true, error: null, eventHashes, eventMap};
}

api.validateEvent = _validateEvent;

async function _validateEventViaService({worker, event, totalEventCount}) {
  const {ledgerNodeId, localPeerId, session: {id: session}} = worker;

  // keep trying to validate event when timeouts occur until work session halts
  let result;
  while(!worker.halt()) {
    try {
      result = await _client.validateEvent(
        {event, ledgerNodeId, localPeerId, session});
    } catch(e) {
      // ignore connection reset, service unavailable, and timeout errors
      // these types of errors are all non-fatal errors related to the
      // validation service being too busy or timing out
      if((e.name === 'FetchError' && e.code === 'ECONNRESET') ||
        (e.name === 'HTTPError' && e.response && e.response.status === 503) ||
        e.name === 'TimeoutError') {
        continue;
      }
      throw e;
    }
    return result;
  }

  // did not get a result before session halted, throw timeout error
  throw new BedrockError(
    'Timed out while validating events.',
    'TimeoutError', {
      httpStatusCode: 503,
      public: true,
      localPeerId,
      totalEventCount
    });
}

// FIXME: Remove eslint disable
// eslint-disable-next-line no-unused-vars
async function _validateEvent({event, ledgerNodeId, session}) {
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);

  // creating a peer event record also includes signature verification for
  // merge events and hash generation for every event type
  const {event: processedEvent, meta} = await api.createPeerEventRecord(
    {event, ledgerNode});

  const {basisBlockHeight} = event;

  // events type could be regular 'r', configuration 'c' or merge events 'm';
  // there are no additional validation rules for merge events here, but others
  // will be applied when validating the DAG shape later (outside this function)
  if(meta.continuity2017.type === 'r') {
    await _util.processChunked({
      tasks: processedEvent.operationRecords,
      fn: _validateOperation,
      concurrency: 25,
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
  const meta = {
    blockHeight: -1,
    consensus: false,
    continuity2017: {type: 'c'},
    eventHash
  };
  // event not validated yet, try to add `creator` to meta
  const {ledgerConfiguration} = event;
  if(ledgerConfiguration) {
    meta.continuity2017.creator = ledgerConfiguration.creator;
  }
  return {event, meta};
}

async function _createPeerRegularEventRecord({event, ledgerNode}) {
  const _event = event;

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

  const meta = {
    blockHeight: -1,
    consensus: false,
    continuity2017: {type: 'r'},
    eventHash
  };
  // event not validated yet, try to add `creator` to meta
  const record = operationRecords[0];
  if(record && record.operation) {
    meta.continuity2017.creator = record.operation.creator;
  }

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
      'Merge events created by the local peer cannot be added with this API.',
      'NotSupportedError', {
        httpStatusCode: 400,
        public: true,
      });
  }

  const meta = {
    blockHeight: -1,
    consensus: false,
    // generation and localAncestorGeneration will be assessed later
    continuity2017: {
      creator,
      generation: null,
      localAncestorGeneration: null,
      type: 'm'
    },
    eventHash,
  };
  return {event, meta};
}

async function _hashOperations({ledgerNode, operations}) {
  // the `chunkSize` should be a function of how many events be concurrently
  // validated by a given validation service; the maximum a validation request
  // has to wait is `maxValidationEventConcurrency * chunkSize`
  const hashedOperations = await _util.processChunked({
    tasks: operations, fn: _hashOperation, concurrency: 25, chunkSize: 25
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
  const meta = {operationHash, eventHash: '', eventOrder: 0};
  return {meta, operation, recordId: ''};
}

// iterate over the eventMap in reverse order to validate graph integrity
// identify merge events, then validate their parents. ensure:
// merge events are validated
// merge event parents that are not merge events are validated
// parents referenced outside the batch already exist and have been validated
async function _validateGraph({worker, eventMap}) {
  /* Here we get all parent merge events that exist outside of the batch that
  will be needed to validate those inside the batch; previous checks ensure
  that all of the events will be found -- these are fetched all at once to
  optimize for the common case where the batch will be valid. */
  const outsideBatchHashes = [];
  const mergeEvents = [];
  for(const eventRecord of eventMap.values()) {
    const {_temp, event, meta} = eventRecord;
    // only need parents of merge events
    if(meta.continuity2017.type !== 'm') {
      continue;
    }
    // FIXME: "dangling" non-merge events are a protocol violation -- update
    // this comment and the code to reject them -- the last event *MUST* be
    // a merge event
    // we will only process merge events and their parents in this function,
    // any "dangling" non-merge events will neither be marked valid nor
    // processed
    mergeEvents.push(eventRecord);

    // track whether parents are inside/outside of batch
    const insideBatch = [];
    for(const parentHash of event.parentHash) {
      const parentRecord = eventMap.get(parentHash);
      if(!parentRecord) {
        outsideBatchHashes.push(parentHash);
      } else {
        insideBatch.push(parentHash);
      }
    }
    _temp.insideBatch = insideBatch;
  }

  // fetch parent records from outside the batch and simultaneously fetch
  // genesis head for validation checks below
  const outsideBatchMap = new Map();
  const [outsideBatchEvents, genesisHead] = await Promise.all([
    _events.getEvents({worker, eventHash: outsideBatchHashes}),
    worker._getGenesisHead()
  ]);
  // build map of parent records outside of the batch
  for(const parentRecord of outsideBatchEvents) {
    outsideBatchMap.set(parentRecord.meta.eventHash, parentRecord);
  }

  // FIXME: events must already be in topological order, if they are not,
  // we should throw immediately, which means we don't need the extra defer
  // looping here, we just throw instead

  // ensure parents of events are validated before their children
  let next = mergeEvents;
  const genesisCreator = genesisHead.creator;
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const eventRecord of current) {
      const {_temp, event} = eventRecord;

      // FIXME: ensure every non-merge event, is followed by an event by the
      // same creator
      // if(previousEvent &&
      //   previousEventRecord.meta.continuity2017.type !== 'm' &&
      //   previousEventRecord.meta.continuity2017.creator !==
      //   eventRecord.meta.continuity2017.creator) { throw exception; }

      // FIXME: also ensure that the last event provided is a merge event

      // FIXME: remove `defer` code, just throw
      // if any merge parents haven't been validated yet the remote peer sent
      // events out of order which is a protocol violation
      let defer = false;
      for(const parentHash of event.parentHash) {
        const parentRecord = eventMap.get(parentHash);
        if(parentRecord && parentRecord.meta.continuity2017.type === 'm' &&
          !parentRecord._temp.processed) {
          defer = true;
          // FIXME: improve this error; but we MUST reject entire batch if
          // order is not proper, it's a protocol violation
          throw new Error('PROTOCOL VIOLATION 1');
        }
      }
      if(defer) {
        next.push(eventRecord);
        continue;
      }

      // keep track of the maximum `localAncestorGeneration` from every
      // `parentRecord`'s meta
      let localAncestorGeneration = 0;

      // track the creators for the merge events in parentHash
      const parentCreatorSet = new Set();
      for(const parentHash of event.parentHash) {
        // get the parent record from the batch or outside of it; if it comes
        // from outside of it, then it has been previously validated
        let outsideBatch = false;
        let parentRecord = eventMap.get(parentHash);
        if(!parentRecord) {
          outsideBatch = true;
          parentRecord = outsideBatchMap.get(parentHash);
          if(!parentRecord) {
            // event's parent is missing, consider the event invalid
            _temp.processed = true;
            // FIXME: improve error here, but we MUST throw due to a protocol
            // violation, don't keep processing
            throw new Error('PROTOCOL VIOLATION 2');
          }
        }

        const {meta: {continuity2017: {
          type: parentType,
          generation: parentGeneration,
          localAncestorGeneration: parentLocalAncestorGeneration = 0
        }}} = parentRecord;

        if(parentHash === event.treeHash) {
          // set event's generation based on its tree parent's
          eventRecord.meta.continuity2017.generation = parentGeneration + 1;

          // check tree-parent-specific validate rules
          await _validateTreeParent(
            {eventRecord, parentRecord, genesisCreator});
        } else if(parentType === 'm') {
          // validate non-tree parent merge event
          await _validateNonTreeParentMergeEvent(
            {eventRecord, parentRecord, parentCreatorSet});
        }

        // apply generic per-type validation rules
        if(parentType === 'm') {
          await _validateParentMergeEvent({worker, eventRecord, parentRecord});
          localAncestorGeneration = Math.max(
            localAncestorGeneration, parentLocalAncestorGeneration);
        } else if(!outsideBatch) {
          // must be a regular/config event...
          // non-merge event from outside the batch must have already been
          // validated, as this algorithm does not allow events into the
          // database that haven't been properly validated, so we only process
          // those not outside the batch here
          await _validateParentNonMergeEvent(
            {worker, eventRecord, parentRecord});
          parentRecord._temp.valid = parentRecord._temp.processed = true;
        }
      }

      // if the event has not been marked as processed yet, then it is now
      // fully processed and all parents referenced in the merge event are
      // valid, so it is valid; otherwise, it is missing some parents and
      // has already been marked processed and invalid
      if(!_temp.processed) {
        _temp.valid = _temp.processed = true;
        // ensure `localAncestorGeneration` is set for the merge event
        eventRecord.meta.continuity2017.localAncestorGeneration =
          localAncestorGeneration;
      }
    }
  }

  // now that the lineage of the events have been validated, make sure that
  // they do not descend from a detected forker, and if not, add any fork
  // meta data to any events that represent a fork that is not yet detected;
  // this must be done after checking the lineage, otherwise an event could
  // have referenced a `treeHash` belonging to another peer and caused our
  // fork checks to erroneously flag a well-behaved peer as a forker
  await _checkForks({worker, mergeEvents, genesisHead});
}

async function _validateTreeParent({
  eventRecord, parentRecord, genesisCreator
}) {
  const {meta: {continuity2017: {
    creator: eventCreator, generation: eventGeneration
  }}} = eventRecord;
  const {meta: {continuity2017: {
    creator: parentCreator, type: parentType
  }}} = parentRecord;

  // merge event tree parents must be merge events
  if(parentType !== 'm') {
    throw new BedrockError(
      'A merge event tree parent must be another merge event.',
      'DataError', {
        parentRecord,
        eventRecord,
        httpStatusCode: 400,
        public: true
      });
  }

  // if the merge event's generation is 1, then it must descend from
  // the genesis merge event (created by the `genesisCreator`)
  if(eventGeneration === 1) {
    if(parentCreator !== genesisCreator) {
      throw new BedrockError(
        'First generation merge events must descend directly from the ' +
        'genesis merge event.', 'DataError', {
          parentRecord,
          eventRecord,
          genesisCreator,
          httpStatusCode: 400,
          public: true
        });
    }
  } else if(parentCreator !== eventCreator) {
    // merge event must descend directly from its own creator
    throw new BedrockError(
      'A non-first generation merge event must descend directly from its ' +
      'own creator.', 'DataError', {
        parentRecord,
        eventRecord,
        eventGeneration,
        eventCreator,
        parentCreator,
        httpStatusCode: 400,
        public: true
      });
  }
}

async function _validateNonTreeParentMergeEvent({
  eventRecord, parentRecord, parentCreatorSet
}) {
  const {meta: {continuity2017: {creator: eventCreator}}} = eventRecord;
  const {meta: {continuity2017: {creator: parentCreator}}} = parentRecord;

  // merge events must not have a non-tree parent with the same creator
  if(eventCreator === parentCreator) {
    throw new BedrockError(
      'Merge events must not have a non-tree parent with the same creator.',
      'DataError', {
        parentRecord,
        eventRecord,
        httpStatusCode: 400,
        public: true
      });
  }

  // merge events must not descend from multiple merge events from the
  // same creator... these are tracked via `parentCreatorSet`; the common
  // case is that the merge event is valid and thus the given parent event's
  // creator is not yet in this set
  if(parentCreatorSet.has(parentCreator)) {
    throw new BedrockError(
      'Merge events must not descend directly from multiple merge events ' +
      'from the same creator.', 'DataError', {
        eventCreator,
        parentCreatorSet: [...parentCreatorSet],
        parentRecord,
        eventRecord,
        httpStatusCode: 400,
        public: true
      });
  }
  parentCreatorSet.add(parentCreator);
}

async function _validateParentMergeEvent({
  /*worker, eventRecord, parentRecord*/
}) {
  // any future parent merge event validation rules go here...

  // FIXME: ensure `parentRecord.meta.continuity2017.forkDetectedBlockHeight` <
  // eventRecord.event.basisBlockHeight
}

async function _validateParentNonMergeEvent({
  worker, eventRecord, parentRecord
}) {
  // parent regular and configuration events (types c || r)...

  // parent r/c events must descend from the same tree parent as the merge
  // event (they must be both parents and siblings)
  const {event: {treeHash: eventTreeHash}} = eventRecord;
  const {event: {treeHash: parentTreeHash}} = parentRecord;
  if(parentTreeHash !== eventTreeHash) {
    throw new BedrockError(
      'Merge event non-merge event parents must descend directly from its ' +
      'tree parent.', 'DataError', {
        eventTreeHash,
        parentTreeHash,
        parentRecord,
        eventRecord,
        httpStatusCode: 400,
        public: true
      });
  }

  const {meta: {continuity2017: {creator: eventCreator}}} = eventRecord;
  const {meta: {continuity2017: {type: parentType}}} = parentRecord;
  if(parentType === 'r') {
    // WebLedgerOperationEvent
    const {event: {operationRecords}} = parentRecord;
    for(const {operation} of operationRecords) {
      if(operation.creator !== eventCreator) {
        throw new BedrockError(
          'Merge event operation event parents must only contain operations ' +
          'that have the same creator as the merge event.',
          'DataError', {
            eventCreator,
            operationCreator: operation.creator,
            parentRecord,
            eventRecord,
            httpStatusCode: 400,
            public: true
          });
      }
    }
  } else if(parentType === 'c') {
    // WebLedgerConfigurationEvent
    const {ledgerNode} = worker;
    const {event: {ledgerConfiguration: {ledger: expectedLedger}}} =
      await ledgerNode.storage.events.getLatestConfig();
    const {creator: configurationCreator, ledger} =
      parentRecord.event.ledgerConfiguration;
    if(ledger !== expectedLedger) {
      throw new BedrockError(
        'Merge events must not descend from configuration events that ' +
        'apply to a different ledger.',
        'DataError', {
          parentRecord,
          eventRecord,
          expectedLedger,
          ledger,
          httpStatusCode: 400,
          public: true
        });
    }
    // parent config events must have the same creator as the event
    if(configurationCreator !== eventCreator) {
      throw new BedrockError(
        'Merge events must not descend from configuration events from ' +
        'another creator.', 'DataError', {
          eventCreator,
          configurationCreator,
          parentRecord,
          eventRecord,
          httpStatusCode: 400,
          public: true
        });
    }
  }
}

async function _validateOperation(
  {operation}, ledgerNode, basisBlockHeight) {
  const result = await ledgerNode.operations.validate(
    {basisBlockHeight, ledgerNode, operation});
  if(!result.valid) {
    throw result.error;
  }
}

async function _checkForks({worker, mergeEvents, genesisHead}) {
  /* Here we need to find whether any forks have occurred in the batch itself
  or if the batch will introduce any new forks based on what is already in the
  database.

  We do this first by checking the batch for repeated `treeHash` values (or
  repeated first generation creator peer IDs) on merge events within the batch.
  Then, for all `treeHash` values from events (and first generation creator
  peer IDs) in the batch that did not result in forks, we check the database
  for forks. The parentheticals here about "first generation creator peer IDs"
  refer to handling the special case where `treeHash` is the generation hash.
  In that single case, it is legal for multiple events to descend from that
  `treeHash`.

  If the database check returns any results, it means that this batch
  introduces new fork(s). This is because peer events can only be added to the
  database through the batch process, we have guaranteed at this point that
  we have not requested any events we don't already have, the events in the
  batch have a valid lineage (these fork checks MUST happen after that to
  prevent an attacker from deceiving us into thinking a well-behaved peer
  forked) and we *assume* that we will not generate any forks locally.

  Therefore, if the query for a merge event with a tree hash has any matches at
  all, it means it is for an event that is different from any in the batch yet
  it has the same tree hash, indicating a fork has been found.

  If the creator of the forked event(s) has been previously detected as a
  forker, the events will be marked as invalid, otherwise, if the events are
  otherwise valid, they will be accepted and the forks will be marked as
  detected at a particular `blockHeight` once they reach consensus so it is
  known that all peers that follow protocol have detected them. */
  const forkerMap = new Map();
  const treeHashSet = new Set();
  const treeHashesToCheck = new Set();
  const firstGenerationSet = new Set();
  const firstGenerationToCheck = new Set();
  for(const eventRecord of mergeEvents) {
    // track events by `treeHash` to find potential forks
    const {event: {treeHash}, meta: {continuity2017: {creator}}} = eventRecord;

    // special case: event descends from the genesis event, so `treeHash`
    // will be shared legally *across* peers, but not within a single peer
    const isFirstGeneration = treeHash === genesisHead.eventHash;
    if(isFirstGeneration) {
      // add event's `creator` to check for forks within the batch and track
      // `creator` as one to check for a first generation fork via the database
      // in case one isn't found in the batch itself
      if(!firstGenerationSet.has(creator)) {
        firstGenerationSet.add(creator);
        firstGenerationToCheck.add(creator);
        continue;
      }
    } else if(!treeHashSet.has(treeHash)) {
      // add event's `treeHash` to `treeHashSet` to check for forks within
      // the batch and track `treeHash` as one to check for a fork via the
      // database in case one isn't found in the batch itself
      treeHashSet.add(treeHash);
      treeHashesToCheck.add(treeHash);
      continue;
    }

    // `event.treeHash` has been seen twice in the batch in events by the same
    // creator, which means a fork has been found in the batch and we don't
    // need to check the database for one
    if(isFirstGeneration) {
      firstGenerationToCheck.delete(creator);
    } else {
      treeHashesToCheck.delete(treeHash);
    }

    // ensure the forker and the `treeHash` at which the forks occur is tracked
    const forker = forkerMap.get(creator);
    if(forker) {
      forker.treeHashSet.add(treeHash);
    } else {
      forkerMap.set(creator, {
        peerId: creator,
        treeHashSet: new Set([treeHash]),
        localForkNumber: null,
        forkDetectedBlockHeight: null
      });
    }
  }

  // get any forkers associated with the tree hashes
  const {ledgerNode} = worker;
  const forkers = await _getForkers({
    ledgerNode,
    treeHashes: [...treeHashesToCheck],
    firstGenerationPeers: [...firstGenerationToCheck],
    genesisHash: genesisHead.eventHash
  });

  // no forks found, return early
  if(forkerMap.size === 0 && forkers.length === 0) {
    return;
  }

  // add forkers and related tree hashes to forkerMap
  for(const forker of forkers) {
    const existing = forkerMap.get(forker.peerId);
    if(existing) {
      for(const treeHash of forker.treeHashes) {
        existing.treeHashSet.add(treeHash);
      }
    } else {
      forkerMap.set(forker.peerId, {
        peerId: forker.peerId,
        treeHashSet: new Set(forker.treeHashes),
        localForkNumber: null,
        forkDetectedBlockHeight: null
      });
    }
  }

  // get latest fork info for the given forkers and add it to forkers
  const latestForks = await _getLatestForks(
    {ledgerNode, peerIds: [...forkerMap.keys()]});
  for(const {peerId, localForkNumber, forkDetectedBlockHeight} of latestForks) {
    const forker = forkerMap.get(peerId);
    forker.localForkNumber = localForkNumber;
    forker.forkDetectedBlockHeight = forkDetectedBlockHeight;
  }

  // for all merge events in the batch, if its creator is a forker:
  // 1. If event's `basisBlockHeight` >= `forkDetectedBlockHeight`, throw.
  // 2. If the event's `treeHash` is in the forker's `treeHashSet`, increment
  //   `localForkNumber` and add it to the event's meta.
  // 3. Add `forkDetectedBlockHeight`, if not null, to the event's meta.
  for(const {event, meta} of mergeEvents) {
    const {continuity2017: {creator}} = meta;
    const forker = forkerMap.get(creator);
    if(!forker) {
      // event not created by a forker, continue
      continue;
    }
    // 1. Ensure event wasn't created after the fork was detected.
    // if `forkDetectedBlockHeight` is `null`, it means the fork hasn't been
    // detected yet
    const {forkDetectedBlockHeight, treeHashSet} = forker;
    const {basisBlockHeight} = event;
    if(forkDetectedBlockHeight !== null &&
      basisBlockHeight >= forkDetectedBlockHeight) {
      throw new BedrockError(
        'The event supplied by the peer was created by a detected forker.' +
        'DataError', {event, basisBlockHeight, forkDetectedBlockHeight});
    }
    if(treeHashSet.has(event.treeHash)) {
      // 2. Event is a new fork, increment `localForkNumber` and add it.
      if(forker.localForkNumber === null) {
        // first `localForkNumber` will be `1` due to increment on next line,
        // which is desirable
        forker.localForkNumber = 0;
      }
      meta.continuity2017.localForkNumber = ++forker.localForkNumber;
    }
    // 3. Add `forkDetectedBlockHeight` if any.
    if(forkDetectedBlockHeight !== null) {
      meta.continuity2017.forkDetectedBlockHeight = forkDetectedBlockHeight;
    }
  }
}

// checks the database for records that match any of the given `treeHashes`;
// this function MUST only called using tree hashes from events that have
// not been stored yet, such that it means that if any records match, the
// creator of those events has created one or more forks; only the peer IDs of
// creators of any forks (aka "forkers") will be returned; `treeHashes` MUST
// NOT include the genesis hash, that must be passed separately if that hash
// is being checked -- along with `firstGenerationPeers` to check
// FIXME: move to bedrock-ledger-consensus-continuity-storage?
async function _getForkers({
  ledgerNode, treeHashes, firstGenerationPeers, genesisHash, explain = false
}) {
  // FIXME: make this a covered query
  let $match = {
    'meta.continuity2017.type': 'm',
    'event.treeHash': {$in: treeHashes}
  };
  if(firstGenerationPeers.length > 0) {
    const $or = [$match];
    $match = {$or};
    for(const peerId of firstGenerationPeers) {
      $or.push({
        'meta.continuity2017.type': 'm',
        'event.treeHash': genesisHash,
        'meta.continuity2017.creator': peerId
      });
    }
  }
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    // find events with the given tree hashes
    {
      $match
    },
    // group by creator to return just `creator`
    {
      $group: {
        _id: '$meta.continuity2017.creator',
        // aggregate all found forked tree hashes
        treeHashes: {$addToSet: '$event.treeHash'}
      }
    },
    {
      $project: {
        _id: 0,
        peerId: '$_id',
        treeHashes: 1
      }
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
}

// FIXME: move to bedrock-ledger-consensus-continuity-storage?
async function _getLatestForks({ledgerNode, peerIds, explain = false}) {
  // FIXME: make this a covered query
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    {
      $match: {
        'meta.continuity2017.creator': {$in: peerIds},
        'meta.continuity2017.type': 'm'
      }
    },
    // sort by `localForkNumber`, descending, to get highest one per creator
    // Note: Other fields are present in the sort to ensure the index is
    // used -- it does not affect the output because we group by creator
    {
      $sort: {
        'meta.continuity2017.creator': 1,
        'meta.continuity2017.type': 1,
        'meta.continuity2017.localForkNumber': -1
      }
    },
    // group by creator to return just `creator`
    {
      $group: {
        _id: '$meta.continuity2017.creator',
        // can safely use `$first` here because we sorted by `localForkNumber`
        eventHash: {$first: '$meta.continuity2017.localForkNumber'}
      }
    },
    // limit number of results to number of peers
    {
      $limit: peerIds.length
    },
    // map `_id` to `peerId`
    {
      $project: {
        _id: 0,
        peerId: '$_id',
        localForkNumber: '$meta.continuity2017.localForkNumber',
        forkDetectedBlockHeight: '$meta.continuity2017.forkDetectedBlockHeight'
      }
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
}
