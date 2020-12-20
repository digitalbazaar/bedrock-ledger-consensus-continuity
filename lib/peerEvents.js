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

api.addBatch = async ({worker, blockHeight, events, ledgerNode, needed}) => {
  const ledgerNodeId = ledgerNode.id;
  let mergeEventsReceived = 0;
  const {eventMap} = await _validateEvents({
    blockHeight, events, ledgerNode, needed
  });

  for(const [eventHash, {event, meta, rawEvent, _temp}] of eventMap) {
    const {valid} = _temp;
    if(!valid) {
      // FIXME: do we want to reject the whole batch here? note that what
      // we have that is valid can be accepted due to new _validateGraph
      // algorithm that ensures we depth-first validate
      continue;
    }

    if(meta.continuity2017.type === 'm') {
      mergeEventsReceived++;
    }

    // FIXME: this "cache warming" may be removed and instead the gossip
    // service(s) may rely on their own in-memory LRU cache, but it does
    // mean that it will take longer for this event to become available
    // to be gossipped; i.e., it won't be available until the event writer
    // has flushed -- so that's a consideration here on getting gossip out
    // onto the wire faster

    // FIXME: this hits redis once for every event which is slow; either
    // we should avoid this cache warming entirely or do it all at once;
    // note that not making this call causes gossip/consensus to fail
    // so it is currently critical and this should be changed so gossip
    // and consensus will continue to function if it fails

    // place the unaltered rawEvent into the cache for future gossip
    await _cache.events.setEventGossip({
      event: rawEvent,
      eventHash,
      ledgerNodeId,
      meta
    });

    // add the event to be written in the next batch
    await worker.peerEventWriter.add({event, meta});
  }

  return {mergeEventsReceived};
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
  const jobDefinitions = [];
  for(const event of events) {
    // event must have a valid `basisBlockHeight` that is not ahead of our
    // current blockHeight, otherwise it gets dropped; note that the peer
    // should not send us these events if it is following the gossip protocol
    // but we have to account for such violations
    const {basisBlockHeight} = event;
    if(!(Number.isInteger(basisBlockHeight) &&
      basisBlockHeight <= blockHeight)) {
      // FIXME: keep track of the fact that the peer sent events it should
      // not have sent; do we want to throw out the whole batch like we do
      // with other unrequested events? (do we even want to do that?)
      continue;
    }
    // push a job to handle basic event validation for this event
    jobDefinitions.push({
      data: {event, ledgerNodeId},
      // max time it should take to validate an event
      // FIXME: we need to ensure that if this spills over into the next work
      // session that it cannot cause corruption
      opts: {timeout: 5000}
    });
  }

  // job.finished() returns a promise that resolves when the job completes
  const jobs = await jobQueue.addBulk(jobDefinitions);
  const promises = [];
  for(const job of jobs) {
    promises.push(job.finished());
  }
  const results = await Promise.all(promises);

  // gather all of the validated events; at this point operations and
  // configurations in events have been validated based on ledger validators,
  // but DAG merge history has not yet been validated
  const neededSet = new Set(needed);
  const eventMap = new Map();
  const eventHashes = [];
  let index = 0;
  for(const {event, meta} of results) {
    const {eventHash} = meta;
    // if any event in the batch was not requested, throw out entire batch
    if(!neededSet.has(eventHash)) {
      throw new BedrockError(
        'The event supplied by the peer was not requested.',
        'DataError', {event, eventHash, neededSet});
    }
    eventHashes.push(eventHash);
    const _temp = {valid: false, processed: false};
    eventMap.set(eventHash, {
      event,
      meta,
      rawEvent: events[index++],
      _temp
    });
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
  await _validateGraph({eventHashes, eventMap, ledgerNode});

  return {eventHashes, eventMap};
}

// called by a bedrock-job, job contains `data` payload
async function _validateEvent({data: {event, ledgerNodeId}}) {
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
// identify merge events, then validate their parents. ensure:
// merge events are validated
// merge event parents that are not merge events are validated
// parents referenced outside the batch already exist and have been validated
async function _validateGraph({eventHashes, eventMap, ledgerNode}) {
  // get all parent merge events that exist outside of the batch that
  // will be needed to validate those inside the batch; previous checks
  // ensure that all of the events will be found -- these are fetched all
  // at once to optimize for the common case where the batch will be valid
  const outsideBatchHashes = [];
  const mergeEvents = [];
  for(const eventHash of eventHashes) {
    const eventRecord = eventMap.get(eventHash);
    const {_temp, event, meta} = eventRecord;
    // only need parents of merge events
    if(meta.continuity2017.type !== 'm') {
      continue;
    }
    // we will only process merge events and their parents in this function,
    // any "dangling" non-merge events will be marked neither valid
    // nor processed
    mergeEvents.push(eventRecord);
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

  // build map of parent records outside of the batch
  const outsideBatchMap = new Map();
  const outsideBatchEvents = await _events.getEvents(
    {eventHash: outsideBatchHashes, ledgerNode});
  for(const parentRecord of outsideBatchEvents) {
    outsideBatchMap.set(parentRecord.meta.eventHash, parentRecord);
  }

  // for storing a lazily-loaded genesisCreator
  const genesisCreator = {id: null};
  // ensure parents of events are validated before their children
  let next = mergeEvents;
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const eventRecord of current) {
      const {_temp, event} = eventRecord;

      // defer if any merge parents haven't been validated yet
      let defer = false;
      for(const parentHash of event.parentHash) {
        const parentRecord = eventMap.get(parentHash);
        if(parentRecord && parentRecord.meta.continuity2017.type === 'm' &&
          !parentRecord._temp.processed) {
          defer = true;
          // FIXME: consider rejecting batch instead of deferring and making
          // ordered gossip a requirement of peers
          break;
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
            break;
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
            {ledgerNode, eventRecord, parentRecord, genesisCreator});
        } else if(parentType === 'm') {
          // validate non-tree parent merge event
          await _validateNonTreeParentMergeEvent(
            {eventRecord, parentRecord, parentCreatorSet});
        }

        // apply generic per-type validation rules
        if(parentType === 'm') {
          await _validateParentMergeEvent(
            {ledgerNode, eventRecord, parentRecord});
          localAncestorGeneration = Math.max(
            localAncestorGeneration, parentLocalAncestorGeneration);
        } else if(!outsideBatch) {
          // must be a regular/config event...
          // non-merge event from outside the batch must have already been
          // validated, as this algorithm does not allow events into the
          // database that haven't been properly validated, so we only process
          // those not outside the batch here
          await _validateParentNonMergeEvent(
            {ledgerNode, eventRecord, parentRecord});
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
}

async function _validateTreeParent({
  ledgerNode, eventRecord, parentRecord, genesisCreator
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
    // lazy load genesis creator
    if(!genesisCreator.id) {
      genesisCreator = {id: await _getGenesisCreator({ledgerNode})};
    }
    if(parentCreator !== genesisCreator.id) {
      throw new BedrockError(
        'First generation merge events must descend directly from the ' +
        'genesis merge event.', 'DataError', {
          parentRecord,
          eventRecord,
          genesisCreator: genesisCreator.id,
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
  /*ledgerNode, eventRecord, parentRecord*/
}) {
  // any future parent merge event validation rules go here...
}

async function _validateParentNonMergeEvent({
  ledgerNode, eventRecord, parentRecord
}) {
  // parent regular and configuration events (types c || r)

  // FIXME: the comment below is old -- newer rules MUST prevent
  // deferred events from being processed at all and we MUST NOT
  // allow non-merge events to be validated/admitted without also
  // validating the merge event in which they descend

  // FIXME: this is an old comment that should be tweaked/removed:
  // some regular or configuration events may have been found outside
  // this batch, in the event pipeline. It is possible that a regular
  // event gets recorded without its corresponding merge event if the
  // gossip session times out before all deferred events are processed.
  // In this case, the merge event would include regular events with
  // multiple basisBlockHeight values. During gossip processing, some
  // of the events were allowed to pass, while those with greater
  // basisBlockHeight values were deferred along with the merge event.
  // It is then possible that the gossip session times out before
  // all the deferred events are processed. In that case the remaining
  // deferred events will be discarded and then reacquired during
  // the next gossip session.

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

async function _getGenesisCreator({ledgerNode}) {
  const {eventHash: genesisHeadHash} = await _history.getGenesisHead(
    {ledgerNode});
  const [{meta: {continuity2017: {creator: genesisCreator}}}] =
    await _events.getEvents({eventHash: genesisHeadHash, ledgerNode});
  return genesisCreator;
}
