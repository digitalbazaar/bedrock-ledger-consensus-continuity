/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _client = require('./client');
const _continuityConstants = require('./continuityConstants');
const _events = require('./events');
const _localPeers = require('./localPeers');
const _signature = require('./signature');
const _util = require('./util');
const _witnesses = require('./witnesses');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const delay = require('delay');
const {config, util: {hasValue, BedrockError}} = bedrock;
const logger = require('./logger');

const api = {};
module.exports = api;

api.addBatch = async ({worker, events, neededHashes, remotePeerId}) => {
  let mergeEventsReceived = 0;
  const {valid, error, eventMap, withheld, fatal} = await _validateEvents(
    {worker, events, neededHashes, remotePeerId});
  if(!valid) {
    return {valid, error, mergeEventsReceived, withheld, fatal};
  }

  for(const {event, meta} of eventMap.values()) {
    if(meta.continuity2017.type === 'm') {
      mergeEventsReceived++;
    }
    // add `localEventNumber` and add event to be written in the next batch
    meta.continuity2017.localEventNumber = worker.nextLocalEventNumber++;
    await worker.peerEventWriter.add({event, meta});
  }

  // ensure all received events are written; it is important to flush this
  // before updating any cursor information or these events could get dropped
  // and we could have an invalid cursor causing us to get stuck
  await worker.writePeerEvents();

  // add `withheld` to worker so it can be merged once committed to
  if(withheld) {
    await worker._addWithheld({withheld});
    mergeEventsReceived++;
  }

  return {valid: true, mergeEventsReceived, withheld};
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

async function _validateEvents({worker, events, neededHashes, remotePeerId}) {
  const {blockHeight} = worker.consensusState;
  const eventMap = new Map();
  let withheld = null;
  let fatal = false;

  try {
    // FIXME: Use a value from the config to limit the number of in-flight
    // requests to events validation service
    const tasks = [];
    const totalEventCount = events.length;
    const pendingValidations = new Set();
    const sharedState = {pendingValidations, highWaterMark: Infinity};
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
      tasks.push({worker, event, totalEventCount, sharedState});
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
      // FIXME: need to ensure that protocol violations have errors with
      // name === `DataError` so that they are marked as `fatal`
      logger.error('An error occurred during gossip processing.', {error: e});
      throw e;
    }

    // gather all of the validated events; at this point operations and
    // configurations in events have been validated based on ledger validators,
    // but DAG merge history has not yet been validated
    const neededSet = new Set(neededHashes);
    let neededCount = 0;
    for(const {event, meta} of results) {
      const {eventHash} = meta;
      // if any event in the batch was not requested (or it appears so because
      // the computed hash does not match), then it is a protocol violation
      if(!neededSet.has(eventHash)) {
        throw new BedrockError(
          'The event supplied by the peer was not requested.',
          'DataError', {event, eventHash, neededSet});
      }
      neededCount++;
      eventMap.set(eventHash, {event, meta});
    }

    // ensure that all the needed events are included in the batch; if not, it
    // is a protocol violation
    if(neededSet.size !== neededCount) {
      throw new BedrockError(
        'The batch does not include all the needed events.',
        'DataError', {
          httpStatusCode: 400,
          missingEventHashes: [...neededSet],
          public: true,
        });
    }

    // inspect all the provided events and update `meta` for each merge event
    ({withheld} = await _validateGraph({worker, eventMap, remotePeerId}));
  } catch(e) {
    if(e.name === 'DataError') {
      fatal = true;
    }
    // do not throw error, just indicate that batch is invalid; throwing an
    // error would unnecessarily terminate the work session
    return {valid: false, error: e, eventMap, withheld, fatal};
  }

  return {valid: true, error: null, eventMap, withheld, fatal};
}

api.validateEvent = _validateEvent;

async function _validateEventViaService({
  worker, event, totalEventCount, sharedState
}) {
  const {ledgerNodeId, localPeerId, session: {id: session}} = worker;
  const {pendingValidations} = sharedState;

  // keep trying to validate event when timeouts occur until work session halts
  while(!worker.halt()) {
    // if the number of pending validations has reached the high water mark,
    // then wait for the validation service to be less busy
    if(pendingValidations.size >= sharedState.highWaterMark) {
      await _waitForValidationService({sharedState});
      continue;
    }

    let promise;
    try {
      // try to do event validation
      promise = _client.validateEvent(
        {event, ledgerNodeId, localPeerId, session});

      // await validation and do pending validation set management
      pendingValidations.add(promise);
      const result = await promise;
      pendingValidations.delete(promise);

      /* Since validation was successful, increment the high water mark to test
      if more CPU is now available. It is expected that this will, at most,
      allow two additional concurrent validation requests to be made. One can
      be made in place of the request that just successfully finished here, and
      another via the increment.

      In theory, if the CPU load hasn't changed, one will be successful and the
      other will timeout and reduce the high water mark again. If the CPU load
      has increased, both will timeout but the service will only be burdened by
      one additional request; after which the high water mark will be reduced
      again. If the CPU load has decreased, both will be successful and each
      will allow one extra request to be tried. Each subsequent time more CPU
      is available, 2x additional requests will make requests, but this
      doubling will reset as soon as the high water mark is reduced again. */
      sharedState.highWaterMark++;
      return result;
    } catch(e) {
      // remove failed validation from pending set
      if(promise) {
        pendingValidations.delete(promise);
      }
      // if validation can't be retried, throw error; will cause the whole
      // batch to be thrown out
      if(!_canRetryValidation(e)) {
        const validationService = _client.getValidationServiceUrl({
          localPeerId
        });
        e.validationService = validationService;
        throw e;
      }
      logger.verbose(
        'A non-critical error occurred while communicating with the event ' +
        'validation service.', {error: e});
      // allow looping to retry validation, but reduce the high water mark
      // that is shared across all validation tasks to the number of pending
      // validations; this number represents the most validations that are
      // currently possible given the CPU load... we allow this number to grow
      // elsewhere
      if(pendingValidations.size < sharedState.highWaterMark) {
        sharedState.highWaterMark = pendingValidations.size;
      }
    }
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

async function _waitForValidationService({sharedState}) {
  const {pendingValidations} = sharedState;
  if(pendingValidations.size === 0) {
    // if there are no pending validations, wait for a second before retry;
    // here we give the service a full second to try and free up its CPU,
    // it's so busy it can't validate even a single event
    await delay(1000);
    // reset the high water mark to allow maximum validations again
    sharedState.highWaterMark = Infinity;
    return;
  }

  // wait for any validation to settle, whether resolved or rejected
  try {
    await Promise.race([...pendingValidations]);
  } catch(e) {
    // do not throw there, we only wanted to wait to get on the queue
  }
}

function _canRetryValidation(e) {
  // can ignore connection reset, service unavailable, bad gateway, and timeout
  // errors; these types of errors are all non-critical errors related to the
  // validation service being too busy or timing out
  const errorCodes = [502, 503];
  return (
    (e.name === 'FetchError' &&
      (e.code === 'ECONNRESET' || e.code === 'EPIPE')) ||
    (e.name === 'HTTPError' && e.response &&
      errorCodes.includes(e.response.status)) ||
    e.name === 'TimeoutError');
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
    continuity2017: {
      type: 'c',
      replayDetectedBlockHeight: -1,
      requiredBlockHeight: -1
    },
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
    continuity2017: {
      type: 'r',
      replayDetectedBlockHeight: -1,
      requiredBlockHeight: -1
    },
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
    localPeerId,
    {controller: creator}
  ] = await Promise.all([
    _util.hasher(event),
    _localPeers.getPeerId({ledgerNodeId}),
    // Note: signature.verify throws if signature is invalid
    _signature.verify({event})
  ]);

  if(creator === localPeerId) {
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
    continuity2017: {
      // some of these fields will be updated later
      creator,
      generation: null,
      hasParentHashCommitment: false,
      isLocalContributor: false,
      lastLocalContributor: null,
      localAncestorGeneration: null,
      localReplayNumber: 0,
      replayDetectedBlockHeight: -1,
      requiredBlockHeight: -1,
      type: 'm'
    },
    eventHash
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

// iterate over the eventMap in to validate graph integrity and topological
// order; also ensure:
// merge events are validated
// merge event parents that are not merge events are validated
// parents referenced outside the batch already exist and have been validated
async function _validateGraph({worker, eventMap, remotePeerId}) {
  /* Here we get all parent merge events that exist outside of the batch that
  will be needed to validate those inside the batch. If any of these events
  are not found, it is a protocol violation. These parents are fetched all at
  once to optimize for the common case where the batch will be valid. */
  const outsideBatchHashes = [];
  const mergeEvents = [];
  let previousEventRecord;
  let count = 0;
  const seenEvents = new Set();
  for(const eventRecord of eventMap.values()) {
    ++count;
    const {event, meta} = eventRecord;

    // ensure every non-merge event is followed by an event by the same creator
    if(previousEventRecord &&
      previousEventRecord.meta.continuity2017.type !== 'm' &&
      previousEventRecord.meta.continuity2017.creator !==
      meta.continuity2017.creator) {
      throw new BedrockError(
        'All non-merge events must be followed by an event with the same ' +
        'creator.', 'DataError', {
          // report details as if previous event record was current
          eventRecord: previousEventRecord,
          nextEventRecord: eventRecord,
          httpStatusCode: 400,
          public: true
        });
    }

    // update previous event record and mark as seen before next turn of loop
    previousEventRecord = eventRecord;
    seenEvents.add(eventRecord);

    // only need parents of merge events
    if(meta.continuity2017.type !== 'm') {
      // ensure the last event is a merge event
      if(count === eventMap.size) {
        throw new BedrockError(
          'The last event in a batch must be a merge event.',
          'DataError', {
            eventRecord,
            httpStatusCode: 400,
            public: true
          });
      }
      // only merge events need additional processing
      continue;
    }

    // track merge event for further processing
    mergeEvents.push(eventRecord);

    // track whether parents are inside/outside of batch; if they are inside
    // the batch, they must have been seen before the current event as the
    // protocol rules require gossip batches to be topologically ordered
    for(const parentHash of event.parentHash) {
      const parentRecord = eventMap.get(parentHash);
      if(!parentRecord) {
        outsideBatchHashes.push(parentHash);
      } else if(!seenEvents.has(parentRecord)) {
        // event is not outside the batch so must have already been seen or
        // there is a protocol violation
        throw new BedrockError(
          'Events in a gossip batch must be topologically ordered.',
          'DataError', {
            parentRecord,
            eventRecord,
            httpStatusCode: 400,
            public: true
          });
      }
    }
  }

  // fetch records from outside the batch and simultaneously fetch
  // genesis head for validation checks below
  const outsideBatchMap = new Map();
  const [outsideBatchEvents, genesisHead] = await Promise.all([
    _events.getEvents({worker, eventHash: outsideBatchHashes}),
    worker._getGenesisHead()
  ]);
  // build map of records outside of the batch
  for(const record of outsideBatchEvents) {
    // non-merge parents MUST be present in the batch
    if(record.meta.continuity2017.type !== 'm') {
      throw new BedrockError(
        'All non-merge parent events must be present in a gossip batch.',
        'DataError', {
          parentRecord: record,
          httpStatusCode: 400,
          public: true
        });
    }
    outsideBatchMap.set(record.meta.eventHash, record);
  }

  // keep track of merge event => operations+configs for later replay detection
  const replayMap = new Map();
  const genesisCreator = genesisHead.creator;
  for(const eventRecord of mergeEvents) {
    const {event, meta} = eventRecord;

    // track all operations and configs referenced by the merge event's
    // regular event parents
    const operationSet = new Set();
    const configSet = new Set();
    replayMap.set(meta.eventHash, {operationSet, configSet});

    // keep track of the maximum `localAncestorGeneration` from every
    // `parentRecord`'s meta
    let localAncestorGeneration = 0;

    // track the creators for the merge events in parentHash
    const parentCreatorSet = new Set();
    for(const parentHash of event.parentHash) {
      // get the parent record from the batch or outside of it
      const parentRecord = eventMap.get(parentHash) ||
        outsideBatchMap.get(parentHash);
      if(!parentRecord) {
        // event's parent is missing which is a protocol violation
        throw new BedrockError(
          'Parent event is missing.',
          'DataError', {
            parentRecord,
            eventRecord,
            httpStatusCode: 400,
            public: true
          });
      }

      // destructure relevant parent record info
      const {meta: {continuity2017: {
        type: parentType,
        generation: parentGeneration,
        isLocalContributor: parentIsLocalContributor,
        lastLocalContributor: parentLastLocalContributor,
        localAncestorGeneration: parentLocalAncestorGeneration = 0,
        localReplayNumber: parentLocalReplayNumber,
        replayDetectedBlockHeight: parentReplayDetectedBlockHeight
      }}} = parentRecord;

      if(parentHash === event.treeHash) {
        // set event's info based on its tree parent's
        meta.continuity2017.generation = parentGeneration + 1;
        meta.continuity2017.lastLocalContributor =
          parentIsLocalContributor ? parentHash : parentLastLocalContributor;
        meta.continuity2017.localReplayNumber = parentLocalReplayNumber;
        meta.continuity2017.replayDetectedBlockHeight =
          parentReplayDetectedBlockHeight;

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
      } else {
        await _validateParentNonMergeEvent(
          {worker, eventRecord, parentRecord, operationSet, configSet});
      }
    }

    // set parent hash commitment flag
    meta.continuity2017.hasParentHashCommitment = !!event.parentHashCommitment;
    // ensure whether or not the event is a local contributor is set
    const isLocalContributor = operationSet.size > 0 || configSet.size > 0;
    meta.continuity2017.isLocalContributor = isLocalContributor;
    // ensure `localAncestorGeneration` is set for the merge event
    meta.continuity2017.localAncestorGeneration = localAncestorGeneration;
  }

  /* Now that the event lineages have been validated, make sure that local
  contribution rules, parent hash and peer commitment rules, and replay rules
  are not violated.

  For local contribution rules, a peer may not create another merge event
  that has local contributions until its last merge event that had local
  contributions has reached consensus.

  For parent hash commitment rules, a non-witness merge event may not become
  a parent (may not be merged) until another merge event has committed to
  it -- and that commitment has reached consensus without its creator being
  detected as a replayer.

  For peer commitment rules, the first merge event created by a peer that
  has never been a witness and is currently not a witness must have a parent
  that includes a peer commitment for the peer.

  For replay rules, ensure that merge events do not descend from a detected
  replayer, and if not, add any replay meta data to any events that represent
  a replay that is not yet detected.

  These checks must be done after checking the lineage, otherwise an event
  could have referenced a `treeHash` belonging to another peer and caused our
  rule checking to erroneously flag a well-behaved peer as a protocol
  violator. */
  const [{withheld}] = await Promise.all([
    _checkParentHashCommitments({worker, mergeEvents, eventMap, remotePeerId}),
    _checkPeerCommitments({worker, mergeEvents, eventMap, outsideBatchMap}),
    _checkLocalContributions({worker, mergeEvents, eventMap, outsideBatchMap}),
    _checkReplays({worker, mergeEvents, genesisHead, replayMap})
  ]);

  return {withheld};
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

  // FIXME: ensure parent merge event cannot be merged if the current merge
  // event's `basisBlockHeight >= replayDetectedBlockHeight` from the parent,
  // i.e., once a replay is detected, it cannot be merged, it is only possible
  // to merge it prior to its detection, i.e., *another* peer must merge it
  // before its own `basisBlockHeight` reaches the detected block height for
  // the parent in order for it to be accepted into storage on the ledger
}

async function _validateParentNonMergeEvent({
  worker, eventRecord, parentRecord, operationSet, configSet
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
    for(const {operation, meta: {operationHash}} of operationRecords) {
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
      // ensure operations referenced by a merge event do not repeat -- and
      // keep track of them all for checking for replays across merge event
      const {size} = operationSet;
      operationSet.add(operationHash);
      if(operationSet.size === size) {
        // duplicate operation within a merge event, throw immediately
        throw new BedrockError(
          'Merge event operation event parents must not replay operations.',
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
    // ensure configs referenced by a merge event do not repeat -- and
    // keep track of them all for checking for replays across merge event
    const {size} = configSet;
    configSet.add(parentRecord.meta.eventHash);
    if(configSet.size === size) {
      // duplicate config within a merge event, throw immediately
      throw new BedrockError(
        'Merge event configuration event parents must not replay ' +
        'configurations.',
        'DataError', {
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

// checks local contribution rules: peers may not merge more local
// contributions until their pending local contributions have reached consensus
async function _checkLocalContributions({
  worker, mergeEvents, eventMap, outsideBatchMap
}) {
  const deferredEvents = [];
  const eventsToFetch = new Set();
  for(const eventRecord of mergeEvents) {
    const {
      meta: {continuity2017: {isLocalContributor, lastLocalContributor}}
    } = eventRecord;
    if(!isLocalContributor) {
      // no need to check the event if it is not a local contributor
      continue;
    }
    // if the last local contributor is in the batch itself, then it follows
    // that we could not have marked it as having achieved consensus yet at
    // the `basisBlockHeight` we announced we support to the peer; this means
    // that it must be a protocol violation, i.e., `_validateLocalContributor`
    // *will* throw
    let lastLocalContributorRecord = eventMap.get(lastLocalContributor);
    if(lastLocalContributorRecord) {
      _validateLocalContributor({lastLocalContributorRecord, eventRecord});
    }
    // if the last local contributor has already been fetched, validate it
    // immediately
    lastLocalContributorRecord = outsideBatchMap.get(lastLocalContributor);
    if(lastLocalContributorRecord) {
      _validateLocalContributor({lastLocalContributorRecord, eventRecord});
      continue;
    }
    // we haven't fetched the last local contributor yet, defer
    deferredEvents.push(eventRecord);
    eventsToFetch.add(lastLocalContributor);
  }

  // if there are no events to fetch, return early
  if(eventsToFetch.size === 0) {
    return;
  }

  // build map of records to check from the database
  const events = await _events.getEvents(
    {worker, eventHash: [...eventsToFetch]});
  const lastLocalContributorMap = new Map();
  for(const parentRecord of events) {
    lastLocalContributorMap.set(parentRecord.meta.eventHash, parentRecord);
  }

  // validate deferred events
  for(const eventRecord of deferredEvents) {
    const {
      meta: {continuity2017: {lastLocalContributor}}
    } = eventRecord;
    const lastLocalContributorRecord = lastLocalContributorMap.get(
      lastLocalContributor);
    _validateLocalContributor({lastLocalContributorRecord, eventRecord});
  }
}

function _validateLocalContributor({
  lastLocalContributorRecord, eventRecord
}) {
  // ensure record indicates `consensus` is true and `blockHeight` is
  // less than or equal to event's `basisBlockHeight`
  const {event: {basisBlockHeight}} = eventRecord;
  const {meta: {consensus, blockHeight}} = lastLocalContributorRecord;
  if(!(consensus && blockHeight <= basisBlockHeight)) {
    throw new BedrockError(
      'A "local contributor" merge event must not be created until its ' +
      'creator\'s previous "local contributor" merge event has reached ' +
      'consensus.',
      'DataError', {
        lastLocalContributorRecord,
        eventRecord,
        httpStatusCode: 400,
        public: true
      });
  }

  // FIXME: if `eventRecord` was created by a non-witness, ensure that any
  // non-witness parent in `eventRecord` matches the commitment in
  // `lastLocalContributorRecord`, otherwise it is invalid; may need to track
  // eventRecord eventHash => non-witness parent in a map that is built when
  // validating parents -- and then do the commitment check here; Note that
  // if `eventRecord` was created by a witness, this same requirement does
  // not apply -- it can merge any non-witness merge event that was committed
  // to by *any* other peer (which is checked elsewhere)
}

// ensures that every non-witness merge event has a valid commitment for it
// that has reached consensus
async function _checkParentHashCommitments({
  worker, mergeEvents, eventMap, remotePeerId
}) {
  const {ledgerNode, consensusState} = worker;

  // gather hashes for all potential non-witness merge events and set the
  // corresponding `requiredBlockHeight` for every known non-witness event
  const nonWitnessEventMap = new Map();
  const witnessesMap = new Map();
  for(const eventRecord of mergeEvents) {
    const {
      event: {basisBlockHeight},
      meta: {continuity2017: {creator}, eventHash}
    } = eventRecord;
    /* The protocol rules state that a merge event that was created by a
    peer that is a non-witness at `basisBlockHeight + 1` must have a valid
    commitment associated with it or must be the last merge event and be
    created by the remote peer -- or the peer must become a witness in a
    a subsequent block (`basisBlockHeight + 2 or later`). Note that the remote
    peer knows the maximum block height that we know, so it MUST not send us a
    non-witness event that we cannot verify complies with these rules or it
    is a protocol violation by the remote peer. */

    // if creator is presently a witness
    // note: `consensusState.blockHeight >= `basisBlockHeight + 1`
    if(consensusState.witnesses.has(creator)) {
      // set `requiredBlockHeight` to `consensusState.blockHeight` and skip
      _setRequiredBlockHeight(
        {eventRecord, blockHeight: consensusState.blockHeight, eventMap});
      continue;
    }

    // see if creator was a witness at the block height at which the event
    // was created, if it is different from `consensusState.blockHeight + 1`
    const blockHeight = basisBlockHeight + 1;
    if(blockHeight < (consensusState.blockHeight + 1)) {
      let witnesses = witnessesMap.get(blockHeight);
      if(!witnesses) {
        ({witnesses} = await _witnesses.getBlockWitnesses(
          {ledgerNode, blockHeight}));
        witnessesMap.set(blockHeight, witnesses);
      }
      if(witnesses.has(creator)) {
        // set `requiredBlockHeight` to `basisBlockHeight` and skip
        _setRequiredBlockHeight(
          {eventRecord, blockHeight: basisBlockHeight, eventMap});
        continue;
      }
    }

    // we need to check to see if the creator was a witness between
    // `basisBlockHeight + 1` and `consensusState.blockHeight + 1` or if
    // there is a commitment for the event
    nonWitnessEventMap.set(eventHash, eventRecord);
  }

  // check database for commitments or witness conversions (non-witness
  // becoming a witness) for all non-witness merge events
  const nonWitnessHashes = [...nonWitnessEventMap.keys()];
  const [commitments, conversions] = await Promise.all([
    _getParentHashCommitments(
      {ledgerNode, nonWitnessHashes, blockHeight: consensusState.blockHeight}),
    _getWitnessConversions(
      {ledgerNode, nonWitnessEvents: [...nonWitnessEventMap.values()]})
  ]);

  // create map of peer ID => conversion block heights
  const conversionMap = new Map();
  for(const {witness, blockHeight} of conversions) {
    conversionMap.set(witness, blockHeight);
  }

  // gather all valid commitments
  const commitmentMap = new Map();
  for(const {parentHashCommitment, valid, blockHeight} of commitments) {
    if(!valid) {
      continue;
    }
    commitmentMap.set(parentHashCommitment, blockHeight);
  }

  /* Go through the non-witness events and ensure each one has a valid
  commitment or witness conversion. If one does not, ensure it is the
  last merge event in the batch and created by the remote peer, otherwise
  it is a protocol violation. For each commitment/conversion, choose the
  lowest  block height as the `requiredBlockHeight` for the event to encourage
  the event to be distributed as early as it can be. */
  const lastEventRecord = mergeEvents[mergeEvents.length - 1];
  let mustWithhold = false;
  for(const eventHash of nonWitnessHashes) {
    const eventRecord = nonWitnessEventMap.get(eventHash);
    const {meta: {continuity2017: {creator}}} = eventRecord;

    const commitmentBlockHeight = commitmentMap.get(eventHash);
    const conversionBlockHeight = conversionMap.get(creator);
    if(commitmentBlockHeight === undefined &&
      conversionBlockHeight === undefined) {
      if(eventRecord === lastEventRecord &&
        creator === remotePeerId) {
        mustWithhold = true;
        break;
      }
      throw new BedrockError(
        'There was no valid commitment nor witness conversion found for a ' +
        'non-witness merge event that requires one.',
        'DataError', {eventHash, creator, remotePeerId});
    }

    // choose minimum of `commitmentBlockHeight` and `conversionBlockHeight`,
    // knowing that at least one of them is defined at this point and, if
    // defined, must be `> 0`
    const blockHeight = Math.min(
      commitmentBlockHeight || Infinity, conversionBlockHeight || Infinity);
    _setRequiredBlockHeight({eventRecord, blockHeight, eventMap});
  }

  // simple case: nothing to withhold
  if(!mustWithhold) {
    return {withheld: null};
  }

  // build withheld information and remove it from the event map
  const withheld = {
    regularEvents: [],
    mergeEvent: lastEventRecord
  };
  // note: at this point, it has already been verified that every regular
  // event is in the batch and therefore in the `eventMap`
  const {event: {parentHash}, meta: {eventHash}} = lastEventRecord;
  for(const parent of parentHash) {
    const parentRecord = eventMap.get(parent);
    if(parentRecord && parentRecord.meta.continuity2017.type !== 'm') {
      withheld.regularEvents.push(parentRecord);
      eventMap.delete(parent);
    }
  }
  eventMap.delete(eventHash);

  // return withheld event, if any
  return {withheld};
}

// ensures that the first merge event created by a peer that has never been a
// witness (including presently) has a parent with a peer commitment for them
async function _checkPeerCommitments({
  worker, mergeEvents, eventMap, outsideBatchMap
}) {
  const {ledgerNode, consensusState: {witnesses}} = worker;

  // check for all first generation merge events from potential non-witnesses
  const firstGeneration = [];
  const peerIds = [];
  for(const eventRecord of mergeEvents) {
    const {meta: {continuity2017: {creator, generation}}} = eventRecord;
    if(generation === 1 && !witnesses.has(creator)) {
      firstGeneration.push(eventRecord);
      peerIds.push(creator);
    }
  }

  // no first generation events to check
  if(firstGeneration.length === 0) {
    return;
  }

  // check which of the first generation peers have been witnesses in the past
  const pastWitnesses = new Set(
    await _haveBeenWitnesses({ledgerNode, peerIds}));
  const nonWitnesses = new Set();
  for(const peerId of peerIds) {
    if(!pastWitnesses.has(peerId)) {
      nonWitnesses.add(peerId);
    }
  }

  // no non-witnesses to check
  if(nonWitnesses.size === 0) {
    return;
  }

  // check all first generation events from non-witnesses for a parent with
  // a peer commitment
  for(const eventRecord of firstGeneration) {
    const {
      event: {parentHash, basisBlockHeight},
      meta: {continuity2017: {creator}, eventHash}
    } = eventRecord;
    if(!nonWitnesses.has(creator)) {
      continue;
    }

    let found = false;
    for(const parent of parentHash) {
      const parentRecord = eventMap.get(parent) || outsideBatchMap.get(parent);
      if(!parentRecord) {
        // parent record must be a regular event, skip it
        continue;
      }
      const {event: {peerCommitment}, meta: {blockHeight}} = parentRecord;
      if(peerCommitment && peerCommitment[0] === creator &&
        blockHeight !== -1 && blockHeight <= basisBlockHeight) {
        // there is no need to check that the parent record is not from a
        // replayer at this point, as previous checks will have assured that
        found = true;
        break;
      }
    }

    if(!found) {
      throw new BedrockError(
        'There was no valid peer commitment found in the parent of a ' +
        'first generation non-witness merge event.',
        'DataError', {eventHash, creator});
    }
  }
}

function _setRequiredBlockHeight({eventRecord, blockHeight, eventMap}) {
  // update `meta` for merge event and related regular events
  const {event: {parentHash}, meta} = eventRecord;
  meta.continuity2017.requiredBlockHeight = blockHeight;
  for(const parent of parentHash) {
    const parentRecord = eventMap.get(parent);
    if(parentRecord && parentRecord.meta.continuity2017.type !== 'm') {
      parentRecord.meta.continuity2017.requiredBlockHeight = blockHeight;
    }
  }
}

// checks for replays of any type (forks, operations, configs)
async function _checkReplays({worker, mergeEvents, genesisHead, replayMap}) {
  /* Here, for each merge event, we need to immediately throw if the event's
  creator has already been detected as a replayer (replays includes forking
  by replaying a `treeHash` or replaying operations/configs) through the
  consensus process.

  If not, we need to determine whether any forks have occurred in the batch
  itself or if the batch will introduce any new replays based on what is
  already in the database. Either way, we cannot immediately reject forks
  until they have been detected via the consensus process.

  We must wait because merge events can be partitioned, which means we must
  handle the case where forkers have sent valid peer only one side of the fork
  or only one side at a time, which would cause a valid peer to accept one or
  both sides. If we happened to get both sides of the fork from a valid peer
  and rejected its batch only on that basis, it would be a successful
  "poisoning" of the valid peer by the forker, which must be prevented.

  To find forkers, we first check the batch for repeated `treeHash` values (or
  repeated first generation creator peer IDs) on merge events within the batch.
  Then, for all `treeHash` values from events (and first generation creator
  peer IDs) in the batch that did not result in forks, we check the database
  for forks. The parentheticals here about "first generation creator peer IDs"
  refer to handling the special case where `treeHash` is the generation hash.
  In that single case, it is legal for multiple events to descend from that
  `treeHash`.

  If the database check returns any results, it means that this batch
  introduces new fork(s). This is because:

  1. Peer events can only be added to the database through the batch process.
  2. We have guaranteed at this point that we have not requested any events we
    don't already have.
  3. Events in the batch have a valid lineage (these fork checks MUST happen
    after that to prevent an attacker from deceiving us into thinking a
    well-behaved peer forked).
  4. And, we *assume* that we will not generate any forks locally.

  Therefore, if the query for a merge event with a tree hash has any matches at
  all, it means it is for an event that is different from any in the batch yet
  it has the same tree hash, indicating a fork has occurred.

  Simultaneously with querying for `treeHash` replays, we also check for
  operation/config replays. The same list of assumptions for detecting these
  types of replays apply such that if we get any operations/config results,
  then a replay has occurred.

  All types of replays can be tracked with a single `localReplayNumber`, as
  when a block is written, if any two (or more) different `localReplayNumber`s
  are added for a given peer, it is an indication that a replay has been
  detected at that block's `blockHeight`. It will only be at that time that
  all merge events created by a peer will have `replayDetectedBlockHeight` set
  on them.

  Note that it doesn't matter if the differing replay numbers were created
  by cross category replays (e.g., one was created by a fork, one by the replay
  of an operation) because:

  1. If a non-fork replay occurred across a fork, this would be detected at
    the same time as a fork.
  2. If a non-fork replay occurred on only one branch of a fork (or if there
    was no fork at all), once consensus has been reached on the replay, all
    peers that have seen the appropriate block will know about the replay,
    regardless of any other forks or replays. */
  const replayerMap = new Map();
  // note that a single tree hash set and single sets for total operations and
  // configs can be used across all peers because they are filled with hashes
  // that were derived from information that included their creator's peer ID
  const treeHashSet = new Set();
  const treeHashesToCheck = new Set();
  const firstGenerationSet = new Set();
  const firstGenerationToCheck = new Set();
  const totalOperationSet = new Set();
  // FIXME: remove `totalConfigSet` if config change feature is removed
  //const totalConfigSet = new Set();
  const operationSummaryMap = new Map();
  for(const eventRecord of mergeEvents) {
    const {
      event,
      meta: {eventHash, continuity2017: {creator, replayDetectedBlockHeight}}
    } = eventRecord;
    // track events by `treeHash` to find potential forks
    const {treeHash, basisBlockHeight} = event;

    // ensure event wasn't created after the fork was detected (i.e., if
    // `basisBlockHeight` >= `replayDetectedBlockHeight`); note that if
    // `replayDetectedBlockHeight` is `-1`, it means the fork hasn't been
    // detected yet
    if(replayDetectedBlockHeight !== -1 &&
      basisBlockHeight >= replayDetectedBlockHeight) {
      throw new BedrockError(
        'The event supplied by the peer was created by a detected replayer.' +
        'DataError', {event, basisBlockHeight, replayDetectedBlockHeight});
    }

    // special case: event descends from the genesis event, so `treeHash`
    // will be shared legally *across* peers, but not within a single peer
    let forked = false;
    const isFirstGeneration = treeHash === genesisHead.eventHash;
    if(isFirstGeneration) {
      // add event's `creator` to check for forks within the batch and track
      // `creator` as one to check for a first generation fork via the database
      // in case one isn't found in the batch itself
      const {size} = firstGenerationSet;
      firstGenerationSet.add(creator);
      if(firstGenerationSet.size > size) {
        firstGenerationToCheck.add(creator);
      } else {
        forked = true;
      }
    } else {
      // add event's `treeHash` to `treeHashSet` to check for forks within
      // the batch and track `treeHash` as one to check for a fork via the
      // database in case one isn't found in the batch itself
      const {size} = treeHashSet;
      treeHashSet.add(treeHash);
      if(treeHashSet.size > size) {
        treeHashesToCheck.add(treeHash);
      } else {
        forked = true;
      }
    }

    // `event.treeHash` has been seen twice in the batch in events by the same
    // creator, which means a fork has been found in the batch and we don't
    // need to check the database for one; however, we cannot throw immediately
    // because another valid peer may have received the forked events in two
    // different batches and it would not have rejected them; we must wait
    // for consensus to be reached on the fork
    if(forked) {
      if(isFirstGeneration) {
        firstGenerationToCheck.delete(creator);
      } else {
        treeHashesToCheck.delete(treeHash);
      }

      // ensure replayer and replayed `treeHash` is tracked
      _updateReplayerMap({replayerMap, peerId: creator, treeHash});
    }

    // check if operations or configs have been replayed within the batch and
    // update operation summary map for checking for operations replayed
    // outside of the batch
    // FIXME: remove `configSet` if config change feature is removed
    const {operationSet/*, configSet*/} = replayMap.get(eventHash);
    let operationsByCreator = operationSummaryMap.get(creator);
    if(!operationsByCreator && operationSet.size > 0) {
      operationSummaryMap.set(creator, operationsByCreator = new Set());
    }
    for(const operationHash of operationSet) {
      const {size} = totalOperationSet;
      totalOperationSet.add(operationHash);
      operationsByCreator.add(operationHash);
      if(totalOperationSet.size > size) {
        continue;
      }
      // ensure replayer and replayed `operationHash` is tracked
      _updateReplayerMap({replayerMap, peerId: creator, operationHash});
    }
    // FIXME: remove if config change feature is removed
    /*
    for(const configEventHash of configSet) {
      const {size} = totalConfigSet;
      totalConfigSet.add(configEventHash);
      if(totalConfigSet.size > size) {
        continue;
      }
      // ensure replayer and replayed `configEventHash` is tracked
      _updateReplayerMap({replayerMap, peerId: creator, configEventHash});
    }*/
  }

  // build `operationSummaries = [{creator, operationHashes}, ...]`
  const operationSummaries = [];
  for(const [creator, operationSet] of operationSummaryMap) {
    operationSummaries.push({creator, operationHashes: [...operationSet]});
  }

  // get any forkers associated with the tree hashes and any other replayers
  // associated with operation/config replays
  const {ledgerNode} = worker;
  // FIXME: remove `configReplayers` if config change feature is removed
  const [forkers, operationReplayers/*, configReplayers*/] = await Promise.all([
    _getForkers({
      ledgerNode,
      treeHashes: [...treeHashesToCheck],
      firstGenerationPeers: [...firstGenerationToCheck],
      genesisHash: genesisHead.eventHash
    }),
    _getOperationReplayers({ledgerNode, operationSummaries})/*,
    // FIXME: create `configSummaries` above
    _getConfigReplayers({ledgerNode, configSummaries: []})*/
  ]);

  // if no replayers found, return early
  // FIXME: remove `configReplayers` if config change feature is removed
  const totalReplayers = replayerMap.size + forkers.length +
    operationReplayers.length;// + configReplayers.length;
  if(totalReplayers === 0) {
    return;
  }

  // add forkers and replayed tree hashes to `replayerMap`
  for(const forker of forkers) {
    const {peerId, treeHashes: treeHash} = forker;
    _updateReplayerMap({replayerMap, peerId, treeHash});
  }

  // add operation replayers and replayed operations to `replayerMap`
  for(const operationReplayer of operationReplayers) {
    const {peerId, operationHashes: operationHash} = operationReplayer;
    _updateReplayerMap({replayerMap, peerId, operationHash});
  }

  /*
  // add config replayers and replayed configs to `replayerMap`
  for(const configReplayer of configReplayers) {
    const {peerId, eventHashes: configEventHash} = configReplayer;
    _updateReplayerMap({replayerMap, peerId, configEventHash});
  }*/

  // get latest fork info for the given replayers and update
  const latestReplays = await _getLatestReplays(
    {ledgerNode, peerIds: [...replayerMap.keys()]});
  for(const latestReplay of latestReplays) {
    const {peerId, localReplayNumber} = latestReplay;
    const replayer = replayerMap.get(peerId);
    replayer.localReplayNumber = localReplayNumber;
  }

  // for all merge events in the batch, if its creator is a replayer and either
  // the event's `treeHash` or one of its referenced operations or configs is
  // in the replayer's respective replayed set, increment `localReplayNumber`
  // and add it to the event's meta
  for(const {event, meta} of mergeEvents) {
    const {eventHash, continuity2017: {creator}} = meta;
    const replayer = replayerMap.get(creator);
    if(!replayer) {
      // creator has not replayed anything, continue to next
      continue;
    }
    if(replayer.replayedTreeHashSet.has(event.treeHash)) {
      // event is a new fork, increment `localReplayNumber` and add it
      meta.continuity2017.localReplayNumber = ++replayer.localReplayNumber;
      continue;
    }
    // if `event` has one of the operations or configs specified in the
    // replayer map, increment and add the local replay number
    // FIXME: remove if `configSet` if config change feature is removed
    const {operationSet/*, configSet*/} = replayMap.get(eventHash);
    let found = false;
    for(const operationHash of replayer.operationSet) {
      if(operationSet.has(operationHash)) {
        found = true;
        break;
      }
    }
    // FIXME: remove if config change feature is removed
    /*
    if(!found) {
      for(const configEventHash of replayer.configSet) {
        if(configSet.has(configEventHash)) {
          found = true;
          break;
        }
      }
    }*/
    if(found) {
      meta.continuity2017.localReplayNumber = ++replayer.localReplayNumber;
    }
  }
}

function _updateReplayerMap({
  replayerMap, peerId, treeHash, operationHash, configEventHash
}) {
  // FIXME: remove `replayedConfigSet` if config change feature is removed
  let replayer = replayerMap.get(peerId);
  if(!replayer) {
    replayerMap.set(peerId, replayer = {
      peerId,
      replayedTreeHashSet: new Set(),
      replayedOperationSet: new Set(),
      replayedConfigSet: new Set(),
      localReplayNumber: 0
    });
  }

  let set;
  let members;
  if(treeHash) {
    set = replayer.replayedTreeHashSet;
    members = treeHash;
  } else if(operationHash) {
    set = replayer.replayedOperationSet;
    members = operationHash;
  } else {
    set = replayer.replayedConfigSet;
    members = configEventHash;
  }
  if(Array.isArray(members)) {
    for(const m of members) {
      set.add(m);
    }
  } else {
    set.add(members);
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

// checks the database for records that match any of the given
// `operationSummaries`; this function MUST only called using operation hashes
// from events that have not been stored yet, such that it means that if any
// records match, the creator of those events has created one or more replays;
// only the peer IDs of creators of any replays will be returned
// FIXME: move to bedrock-ledger-consensus-continuity-storage?
async function _getOperationReplayers({
  ledgerNode, operationSummaries, explain = false
}) {
  if(operationSummaries.length === 0) {
    return [];
  }

  // FIXME: make this a covered query
  const $or = [];
  for(const summary of operationSummaries) {
    const {creator, operationHashes} = summary;
    $or.push({
      'operation.creator': creator,
      'meta.operationHash': {$in: operationHashes}
    });
  }
  const {collection} = ledgerNode.storage.operations;
  const cursor = collection.aggregate([
    // find events with the given operation data
    {
      $match: {$or}
    },
    // group by creator to return just `creator`
    {
      $group: {
        _id: '$meta.continuity2017.creator',
        // aggregate all found operation hashes
        operationHashes: {$addToSet: '$meta.operationHash'}
      }
    },
    {
      $project: {
        _id: 0,
        peerId: '$_id',
        operationHashes: 1
      }
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
}

/*
// checks the database for records that match any of the given
// `configSummaries`; this function MUST only called using configs from
// events that have not been stored yet, such that it means that if any
// records match, the creator of those events has created one or more replays;
// only the peer IDs of creators of any replays will be returned
// FIXME: move to bedrock-ledger-consensus-continuity-storage?
async function _getConfigReplayers({
  ledgerNode, configSummaries, explain = false
}) {
  if(configSummaries.length === 0) {
    return [];
  }

  // FIXME: make this a covered query
  const $or = [];
  for(const summary of configSummaries) {
    const {creator, basisBlockHeight, eventHashes} = summary;
    $or.push({
      'meta.continuity2017.creator': creator,
      'event.basisBlockHeight': basisBlockHeight,
      'meta.eventHash': {$in: eventHashes}
    });
  }
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    // find events with the given operation data
    {
      $match: {$or}
    },
    // group by creator to return just `creator`
    {
      $group: {
        _id: '$meta.continuity2017.creator',
        // aggregate all found operation hashes
        eventHashes: {$addToSet: '$meta.eventHash'}
      }
    },
    {
      $project: {
        _id: 0,
        peerId: '$_id',
        eventHashes: 1
      }
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
}*/

// FIXME: move to bedrock-ledger-consensus-continuity-storage?
async function _getLatestReplays({ledgerNode, peerIds, explain = false}) {
  // FIXME: make this a covered query
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    {
      $match: {
        'meta.continuity2017.creator': {$in: peerIds},
        'meta.continuity2017.type': 'm'
      }
    },
    // sort by `localReplayNumber`, descending, to get highest one per creator
    // Note: Other fields are present in the sort to ensure the index is
    // used -- it does not affect the output because we group by creator
    {
      $sort: {
        'meta.continuity2017.creator': 1,
        'meta.continuity2017.type': 1,
        'meta.continuity2017.localReplayNumber': -1
      }
    },
    // group by creator to return just `creator`
    {
      $group: {
        _id: '$meta.continuity2017.creator',
        // can safely use `$first` here because we sorted by `localReplayNumber`
        eventHash: {$first: '$meta.continuity2017.localReplayNumber'}
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
        localReplayNumber: '$meta.continuity2017.localReplayNumber'
      }
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
}

// FIXME: move to bedrock-ledger-consensus-continuity-storage?
async function _getParentHashCommitments({
  ledgerNode, nonWitnessHashes, blockHeight, explain = false
}) {
  if(nonWitnessHashes.length === 0) {
    return [];
  }

  // FIXME: make this a covered query
  const {collection} = ledgerNode.storage.events;
  const cursor = collection.aggregate([
    {
      $match: {
        'event.parentHashCommitment': {$in: nonWitnessHashes},
        'meta.continuity2017.type': 'm',
        'meta.blockHeight': {$gt: -1, $lte: blockHeight}
      }
    },
    // order to enable selection of the earliest commitments to reach consensus
    {
      $sort: {'meta.blockHeight': 1}
    },
    // allow for multiple parent hash commitments (current implementation
    // supports just 1)
    {
      $unwind: {path: '$event.parentHashCommitment'}
    },
    // mark whether or not a particular commitment is valid; its `blockHeight`
    // must be less than the `replayDetectedBlockHeight` (or no replay
    // detected at all indicated by `-1`)
    {
      $project: {
        parentHashCommitment: '$event.parentHashCommitment',
        blockHeight: '$meta.blockHeight',
        valid: {
          $or: [{
            $eq: ['$meta.continuity2017.replayDetectedBlockHeight', -1]
          }, {
            $lt: [
              '$meta.blockHeight',
              '$meta.continuity2017.replayDetectedBlockHeight'
            ]
          }]
        }
      }
    },
    // group by committed hash and `valid` status using the earliest
    // `blockHeight`
    {
      $group: {
        _id: {
          parentHashCommitment: '$parentHashCommitment',
          valid: '$valid'
        },
        blockHeight: {$first: '$blockHeight'}
      }
    },
    // map `_id` back to `parentHashCommitment` and `valid`
    {
      $project: {
        _id: 0,
        parentHashCommitment: '$_id.parentHashCommitment',
        valid: '$_id.valid',
        blockHeight: '$blockHeight'
      }
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
}

// FIXME: move to bedrock-ledger-consensus-continuity-storage?
async function _getWitnessConversions({
  ledgerNode, nonWitnessEvents, explain = false
}) {
  if(nonWitnessEvents.length === 0) {
    return [];
  }

  // query to discover if any of the non-witness events were created by
  // a peer that became a witnesses after the block height during which
  // the event was created (`basisBlockHeight + 1`)
  const nonWitnesses = [];
  const $or = [];
  for(const eventRecord of nonWitnessEvents) {
    const {
      event: {basisBlockHeight},
      meta: {continuity2017: {creator}}
    } = eventRecord;
    $or.push({
      'meta.continuity2017.witness': creator,
      'block.blockHeight': {$gt: basisBlockHeight + 1}
    });
    nonWitnesses.push(creator);
  }

  /* Since our block records include an array of witnesses, it is possible
  for each `creator` in our `$or` array to match across multiple blocks. This
  could cause us to get results where the block height for a block is NOT
  greater than the `basisBlockHeight + 1` required for a particular `creator`
  if that creator appears in an earlier block with another matching creator.

  Therefore, we reverse sort our results by block height and select the
  greatest block height for every `creator`. Now, this will not necessarily
  result in us getting back the *lowest* block height at which a particular
  creator became a witness *after* the block height during which the related
  non-witness event was created (`basisBlockHeight + 1`). However, this is ok
  because in order for this to be a problem, it would have to be the case
  that the event reaches consensus or is needed to reach consensus prior
  to the block height we return from this function and therefore select as
  its `requiredBlockHeight`.

  Suppose there's a non-witness event, E, created by peer, P, at
  `E.basisBlockHeight + 1`, BH. Now suppose that P becomes a witness at
  `BH + H1` and later again at `BH + H3`.

  There are two cases to consider for E that would be problematic:

  1. E reached consensus prior to `BH + H3`.

  2. E is needed to create a block with block height `BH + H2` where
    `H1 < H2 < H3`.

  In both cases, our local peer would be unable to serve E to a requesting
  peer until a point after which it reaches consensus or is needed to reach
  consensus, eliminating our ability to help the requesting peer reach
  consensus.

  Assumption 1:

  In order for the database query below to select `BH + H3`, it MUST be the
  case that `BH + H3` is the block height of a known block.

  Assumption 2:

  In order to evaluate E we MUST NOT have already evaluated and accepted E.

  Consider case 1 (E reached consensus prior to `BH + H3`):

  By Assumption 1, we MUST have seen block `BH + H3`. This means we MUST
  have seen and accepted E, since it reached consensus before `BH + H3`.
  However, by Assumption 2, we MUST NOT have seen and accepted E since we are
  evaluating it. This is a contradiction so case 1 cannot happen.

  Consider case 2 (E is needed to create block `BH + H2`):

  By Assumption 1, we MUST have seen block `BH + H3`. This means we MUST
  have seen and accepted E, since it is required to create `BH + H2` and
  `H2 < H3`. However, by Assumption 2, we MUST NOT have seen and accepted E
  since we are evaluating it. This is a contradiction so case 2 cannot happen.

  In both cases, our local peer should continue to be able to help other
  peers reach consensus. */

  // FIXME: make this a covered query
  const {collection} = ledgerNode.storage.blocks;
  const cursor = collection.aggregate([
    {
      $match: {$or}
    },
    // order to enable selection of the latest block height
    {
      $sort: {'block.blockHeight': -1}
    },
    // group by witness array and latest block height
    {
      $group: {
        _id: '$meta.continuity2017.witness',
        blockHeight: {$first: '$block.blockHeight'}
      }
    },
    // unwind *after* `$group` to try and reduce the number of docs being
    // operated on -- avoiding generating a document for every witness found
    // (including those not queried for) for *every block* vs. a single
    // document for every unique combination of witnesses that includes a
    // peer we were looking for
    {
      $unwind: {path: '$_id'}
    },
    // filter out witnesses we are not interested in
    {
      $match: {_id: {$in: nonWitnesses}}
    },
    // select maximum block height for the witnesses we are interested in
    {
      $group: {
        _id: '$_id',
        blockHeight: {$max: '$blockHeight'}
      }
    },
    // map `_id` back to `witness`
    {
      $project: {
        _id: 0,
        witness: '$_id',
        blockHeight: '$blockHeight'
      }
    }
  ], {allowDiskUse: true});
  if(explain) {
    cursor.explain('executionStats');
  }
  return cursor.toArray();
}

// FIXME: move to bedrock-ledger-consensus-continuity-storage
// returns all peer IDs from `peerIds` that have been witnesses
async function _haveBeenWitnesses({ledgerNode, peerIds, explain = false} = {}) {
  // FIXME: make this a covered query
  const query = {
    'meta.continuity2017.witness': {$in: [peerIds]}
  };
  const {collection} = ledgerNode.storage.blocks;
  const projection = {_id: 0, 'meta.continuity2017.witness': 1};
  const cursor = await collection.find(query, {projection}).limit(
    peerIds.length);
  if(explain) {
    return cursor.explain('executionStats');
  }
  const records = await cursor.toArray();
  const witnesses = new Set();
  for(const {meta: {continuity2017: {witness}}} of records) {
    for(const w of witness) {
      witnesses.add(w);
    }
  }
  const results = [];
  for(const peerId of peerIds) {
    if(witnesses.has(peerId)) {
      results.push(peerId);
    }
  }
  return results;
}
