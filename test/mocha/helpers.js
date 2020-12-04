/*
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
const hasher = brLedgerNode.consensus._hasher;
const jsigs = require('jsonld-signatures');
const {util: {uuid}} = bedrock;

const ledgerHistory = {
  alpha: require('./history-alpha'),
  beta: require('./history-beta'),
  gamma: require('./history-gamma'),
  delta: require('./history-delta'),
  epsilon: require('./history-epsilon'),
};

const api = {};
module.exports = api;

api.peersReverse = {};

api.average = arr => Math.round(arr.reduce((p, c) => p + c, 0) / arr.length);

// test hashing function
api.testHasher = brLedgerNode.consensus._hasher;

api.addEvent = async ({
  count = 1, eventTemplate, ledgerNode, opTemplate
} = {}) => {
  const events = {};
  const {creatorId} = ledgerNode;
  let operations;
  for(let i = 0; i < count; ++i) {
    const testEvent = bedrock.util.clone(eventTemplate);
    testEvent.basisBlockHeight = 1;
    const operation = bedrock.util.clone(opTemplate);
    const testRecordId = `https://example.com/event/${uuid()}`;
    operation.creator = ledgerNode.creatorId;
    if(operation.type === 'CreateWebLedgerRecord') {
      operation.record.id = testRecordId;
    }
    if(operation.type === 'UpdateWebLedgerRecord') {
      operation.recordPatch.target = testRecordId;
    }

    const head = await ledgerNode.consensus._history.getHead({
      creatorId, ledgerNode
    });
    const {eventHash: headHash} = head;
    testEvent.parentHash = [headHash];
    testEvent.treeHash = headHash;

    testEvent.operationHash = [await hasher(operation)];

    const eventHash = await hasher(testEvent);
    operations = [{
      meta: {
        basisBlockHeight: 1,
        operationHash: testEvent.operationHash,
        recordId: ledgerNode.storage.driver.hash(testRecordId)
      },
      operation
    }];
    await ledgerNode.consensus.operations.write({
      eventHash, ledgerNode, operations
    });

    const result = await ledgerNode.consensus._events.add({
      event: testEvent, eventHash, ledgerNode
    });

    result.operations = operations;
    events[result.meta.eventHash] = result;
  }

  return events;
};

api.addEventAndMerge = async ({
  consensusApi, count = 1, eventTemplate, ledgerNode, opTemplate
} = {}) => {
  if(!(consensusApi && eventTemplate && ledgerNode)) {
    throw new TypeError(
      '`consensusApi`, `eventTemplate`, and `ledgerNode` are required.');
  }
  const events = {};

  events.regular = await api.addEvent({
    count, eventTemplate, ledgerNode, opTemplate
  });
  events.regularHashes = Object.keys(events.regular);

  events.merge = await consensusApi._worker.merge({
    creatorId: ledgerNode.creatorId, ledgerNode
  });
  events.mergeHash = events.merge.meta.eventHash;
  events.allHashes = [events.mergeHash, ...events.regularHashes];

  return events;
};

api.addEventMultiNode = async ({
  consensusApi, eventTemplate, nodes, opTemplate
} = {}) => {
  const rVal = {
    mergeHash: [],
    regularHash: []
  };
  for(const name of Object.keys(nodes)) {
    const ledgerNode = nodes[name];
    rVal[name] = await api.addEventAndMerge({
      consensusApi, eventTemplate, ledgerNode, opTemplate
    });
  }
  Object.keys(nodes).forEach(k => {
    rVal.regularHash.push(Object.keys(rVal[k].regular)[0]);
    rVal.mergeHash.push(rVal[k].merge.meta.eventHash);
  });
  return rVal;
};

// this helper is for test that execute the consensus worker
api.addOperation = async ({count = 1, ledgerNode, opTemplate} = {}) => {
  const operations = {};
  for(let i = 0; i < count; ++i) {
    const operation = bedrock.util.clone(opTemplate);
    // _peerId added for convenience in test framework
    operation.creator = ledgerNode._peerId;
    operation.record.id = `https://example.com/event/${uuid()}`;
    operation.record.creator = ledgerNode.id;
    const result = await ledgerNode.operations.add({operation, ledgerNode});
    operations[result.meta.operationHash] = operation;
  }
  return operations;
};

api.addOperations = async ({count, nodes, opTemplate} = {}) => {
  const results = [];
  for(const ledgerNode of nodes) {
    results.push(await api.addOperation({count, ledgerNode, opTemplate}));
  }
  return results;
};

// returns a different data structure than `api.addOperations`
api.addOperations2 = async ({count, nodes, opTemplate}) => {
  const results = {};
  for(const key in nodes) {
    const ledgerNode = nodes[key];
    const result = await api.addOperation({count, ledgerNode, opTemplate});
    results[key] = result;
  }
  return results;
};

// add a merge event and regular event as if it came in through gossip
// NOTE: the events are rooted with the genesis merge event
api.addRemoteEvents = async ({
  consensusApi, count = 1, ledgerNode, mockData
} = {}) => {
  const creatorId = mockData.exampleIdentity;
  const results = [];
  for(let i = 0; i < count; ++i) {
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.operation[0].record.id =
      `https://example.com/event/${uuid()}`;
    const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    // NOTE: using the local branch head for treeHash of the remote merge event
    const head = await consensusApi._history.getHead({
      // unknown creator will yield genesis merge event
      creatorId,
      ledgerNode
    });

    // in this example the merge event and the regular event
    // have a common ancestor which is the genesis merge event
    testMergeEvent.treeHash = head.eventHash;
    testMergeEvent.parentHash = [head.eventHash];
    testRegularEvent.treeHash = head.eventHash;
    testRegularEvent.parentHash = [head.eventHash];

    testMergeEvent.parentHash.push(await api.testHasher(testRegularEvent));

    // FIXME: this helper is not currently being used, jsigs API call
    // will need to be updated
    const signed = await jsigs.sign(
      testMergeEvent, {
        algorithm: 'Ed25519Signature2018',
        privateKeyBase58: keyPair.privateKey,
        creator: mockData.authorizedSignerUrl
      });

    // add regular event
    const regularEvent = await ledgerNode.consensus._events.add({
      continuity2017: {peer: true},
      event: testRegularEvent,
      ledgerNode
    });

    // add merge event
    const mergeEvent = await ledgerNode.consensus._events.add({
      continuity2017: {peer: true},
      event: signed,
      ledgerNode
    });

    results.push({
      merge: mergeEvent.meta.eventHash,
      regular: regularEvent.meta.eventHash
    });
  }

  if(results.length === 1) {
    return results[0];
  }
  return results;
};

api.buildHistory = async ({consensusApi, historyId, mockData, nodes} = {}) => {
  const eventTemplate = mockData.events.alpha;
  const opTemplate = mockData.operations.alpha;
  const results = await ledgerHistory[historyId]({
    api, consensusApi, eventTemplate, nodes, opTemplate
  });
  const copyMergeHashes = {};
  const copyMergeHashesIndex = {};
  Object.keys(results).forEach(key => {
    if(key.startsWith('cp')) {
      copyMergeHashes[key] = results[key].meta.eventHash;
      copyMergeHashesIndex[results[key].meta.eventHash] = key;
    }
  });
  const regularEvent = results.regularEvent;
  return {copyMergeHashes, copyMergeHashesIndex, regularEvent};
};

// from may be a single node or an array of nodes
api.copyAndMerge = async ({
  consensusApi, from, nodes, to, useSnapshot = false
} = {}) => {
  const copyFrom = [].concat(from);
  for(const f of copyFrom) {
    await api.copyEvents({from: nodes[f], to: nodes[to], useSnapshot});
  }
  return consensusApi._worker.merge(
    {creatorId: nodes[to].creatorId, ledgerNode: nodes[to]});
};

const snapshot = {};
api.copyEvents = async ({from, to, useSnapshot = false}) => {
  // events
  const collection = from.storage.events.collection;
  let events;
  if(useSnapshot && snapshot[collection.collectionName]) {
    events = snapshot[collection.collectionName];
  } else {
    // FIXME: use a more efficient query, the commented aggregate function
    // is evidently missing some events.
    const projection = {'meta.eventHash': 1};
    const results = await collection.find({
      'meta.consensus': false
    }, {projection}).sort({$natural: 1}).toArray();
    events = await from.storage.events.getMany({
      eventHashes: results.map(r => r.meta.eventHash)
    }).toArray();
  }
  // diff
  const eventHashes = events.map(e => e.meta.eventHash);
  const _diff = await to.storage.events.difference(eventHashes);
  if(_diff.length === 0) {
    // throw new BedrockError('Nothing to do.', 'AbortError');
    return [];
  }
  const diffSet = new Set(_diff);
  const diff = events.filter(e => diffSet.has(e.meta.eventHash));
  // add
  for(const e of diff) {
    try {
      await _addTestEvent({event: e.event, ledgerNode: to});
    } catch(err) {
      // ignore dup errors
      if(!(err && err.name === 'DuplicateError')) {
        throw err;
      }
    }
  }
  // write
  await to.eventWriter.write();
};

api.createEvent = async (
  {eventTemplate, eventNum, consensus = true, hash = true} = {}) => {
  const events = [];
  for(let i = 0; i < eventNum; ++i) {
    const event = bedrock.util.clone(eventTemplate);
    event.id = `https://example.com/events/${uuid()}`;
    const meta = {};
    if(consensus) {
      meta.consensus = true;
      meta.consensusDate = Date.now();
    }
    if(!hash) {
      events.push({event, meta});
      continue;
    }
    // FIXME: make parallel
    meta.eventHash = await api.testHasher(event);
    events.push({event, meta});
  }
  return events;
};

api.createEventBasic = ({eventTemplate} = {}) => {
  const event = bedrock.util.clone(eventTemplate);
  event.operation[0].record.id = 'https://example.com/events/' + uuid();
  return event;
};

api.flushCache = () => cache.client.flushall();

api.nBlocks = async ({
  consensusApi, nodes, operationOnWorkCycle = 'all', opTemplate,
  targetBlockHeight
}) => {
  const recordIds = {};
  const targetBlockHashMap = {};
  let workCycle = 0;
  while(Object.keys(targetBlockHashMap).length !== Object.keys(nodes).length) {
    const count = 1;
    workCycle++;
    let addOperation = true;
    if(operationOnWorkCycle === 'first' && workCycle > 1) {
      addOperation = false;
    }
    // add operations if requested
    if(addOperation) {
      const operations = await api.addOperations2({count, nodes, opTemplate});
      // record the IDs for the records that were just added
      for(const n in nodes) {
        if(!recordIds[n]) {
          recordIds[n] = [];
        }
        for(const opHash of Object.keys(operations[n])) {
          recordIds[n].push(operations[n][opHash].record.id);
        }
      }
    }
    // run work cycle
    // `nodes` is an object that needs to be converted to an array for helper
    await api.runWorkerCycle(
      {consensusApi, nodes: Object.values(nodes), series: false});
    // report
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const result = await ledgerNode.storage.blocks.getLatestSummary();
      const {block} = result.eventBlock;
      if(block.blockHeight >= targetBlockHeight) {
        const result = await ledgerNode.storage.blocks.getByHeight(
          targetBlockHeight);
        targetBlockHashMap[key] = result.meta.blockHash;
      }
    }
  }
  return {recordIds, targetBlockHashMap};
};

// collections may be a string or array
api.removeCollections = async function(collections) {
  const collectionNames = [].concat(collections);
  await database.openCollections(collectionNames);
  for(const collectionName of collectionNames) {
    await database.collections[collectionName].deleteMany({});
  }
};

api.prepareDatabase = async function() {
  await api.removeCollections([
    'identity', 'eventLog', 'ledger', 'ledgerNode', 'continuity2017_key',
    'continuity2017_manifest', 'continuity2017_vote', 'continuity2017_voter'
  ]);
};

api.runWorkerCycle = async (
  {consensusApi, nodes, series = false, targetCyclesPerNode = 1}) => {
  const promises = [];
  for(const ledgerNode of nodes) {
    const promise = _cycleNode(
      {consensusApi, ledgerNode, targetCycles: targetCyclesPerNode});
    if(series) {
      await promise;
    } else {
      promises.push(promise);
    }
  }
  await Promise.all(promises);
};

async function _cycleNode({consensusApi, ledgerNode, targetCycles = 1} = {}) {
  if(ledgerNode.stop) {
    return;
  }

  try {
    await consensusApi._worker._run({ledgerNode, targetCycles});
  } catch(err) {
    // if a config change is detected, do not run worker on that node again
    if(err && err.name === 'LedgerConfigurationChangeError') {
      ledgerNode.stop = true;
      return;
    }
    throw err;
  }
}

/*
 * execute the worker cycle until there are no non-consensus
 * events of type `WebLedgerOperationEvent` or `WebLedgerConfigurationEvent`
 * and the blockHeight on all nodes are the same. It is expected that
 * there will be various numbers of non-consensus events of type
 * `ContinuityMergeEvent` on a settled network.
 */
api.settleNetwork = async ({consensusApi, nodes, series = false} = {}) => {
  while(true) {
    await api.runWorkerCycle({consensusApi, nodes, series});

    // all nodes should have zero non-consensus regular events
    let promises = [];
    for(const ledgerNode of nodes) {
      promises.push(ledgerNode.storage.events.getCount({
        consensus: false, type: 'WebLedgerOperationEvent'
      }));
    }
    if((await Promise.all(promises)).some(c => c > 0)) {
      continue;
    }

    // all nodes should have zero non-consensus configuration events
    promises = [];
    for(const ledgerNode of nodes) {
      promises.push(ledgerNode.storage.events.getCount({
        consensus: false, type: 'WebLedgerConfigurationEvent'
      }));
    }
    if((await Promise.all(promises)).some(c => c > 0)) {
      continue;
    }

    // all nodes should have the same latest blockHeight
    promises = [];
    for(const ledgerNode of nodes) {
      promises.push(ledgerNode.storage.blocks.getLatestSummary());
    }
    const summaries = await Promise.all(promises);
    const blockHeights = summaries.map(s => s.eventBlock.block.blockHeight);
    if(blockHeights.every(b => b === blockHeights[0])) {
      break;
    }
  }
};

api.snapshotEvents = async ({ledgerNode}) => {
  const collection = ledgerNode.storage.events.collection;
  // FIXME: use a more efficient query, the commented aggregate function
  // is evidently missing some events.
  const nonConsensusRecords = await collection.find({
    'meta.consensus': false
  }).sort({$natural: 1}).toArray();
  const eventRecords = await ledgerNode.storage.events.getMany({
    eventHashes: nonConsensusRecords.map(r => r.meta.eventHash)
  }).toArray();
  const mergeRecords = eventRecords.map(r => {
    if(r.event.type !== 'WebLedgerOperationEvent') {
      delete r.event.operation;
    }
    return r;
  });
  // make snapshot
  snapshot[collection.collectionName] = mergeRecords;
  return mergeRecords;
};

api.use = async plugin => {
  return brLedgerNode.use(plugin);
};

async function _addTestEvent({event, ledgerNode}) {
  const ledgerNodeId = ledgerNode.id;
  const {_cache, _peerEvents} = ledgerNode.consensus;
  const eventMap = new Map();
  const {event: processedEvent, meta} =
    await _peerEvents.createPeerEventRecord({event, eventMap, ledgerNode});
  await _cache.events.setEventGossip(
    {event, eventHash: meta.eventHash, ledgerNodeId, meta});
  await _cache.events.addPeerEvent(
    {event: processedEvent, ledgerNodeId, meta});
}
