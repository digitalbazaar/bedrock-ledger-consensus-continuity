/*
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const {callbackify, promisify} = require('util');
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

const openCollections = promisify(database.openCollections);

const api = {};
module.exports = api;

api.peersReverse = {};

api.average = arr => Math.round(arr.reduce((p, c) => p + c, 0) / arr.length);

// test hashing function
api.testHasher = brLedgerNode.consensus._hasher;

api.addEvent = async ({count = 1, eventTemplate, ledgerNode, opTemplate}) => {
  const events = {};
  const {creatorId} = ledgerNode;
  let operations;
  for(let i = 0; i < count; ++i) {
    const testEvent = bedrock.util.clone(eventTemplate);
    testEvent.basisBlockHeight = 1;
    const operation = bedrock.util.clone(opTemplate);
    const testRecordId = `https://example.com/event/${uuid()}`;
    if(operation.type === 'CreateWebLedgerRecord') {
      operation.record.id = testRecordId;
    }
    if(operation.type === 'UpdateWebLedgerRecord') {
      operation.recordPatch.target = testRecordId;
    }

    const head = await ledgerNode.consensus._events.getHead({
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
}) => {
  if(!(consensusApi && eventTemplate && ledgerNode)) {
    throw new TypeError(
      '`consensusApi`, `eventTemplate`, and `ledgerNode` are required.');
  }
  const events = {};
  const merge = consensusApi._events.merge;

  events.regular = await api.addEvent({
    count, eventTemplate, ledgerNode, opTemplate
  });
  events.regularHashes = Object.keys(events.regular);

  events.merge = await merge({
    creatorId: ledgerNode.creatorId, ledgerNode
  });
  events.mergeHash = events.merge.meta.eventHash;
  events.allHashes = [events.mergeHash, ...events.regularHashes];

  return events;
};

api.addEventMultiNode = async ({
  consensusApi, eventTemplate, nodes, opTemplate
}) => {
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
api.addOperation = ({count = 1, ledgerNode, opTemplate}, callback) => {
  const operations = {};
  async.timesSeries(count, (i, callback) => {
    const operation = bedrock.util.clone(opTemplate);
    // _peerId added for convenience in test framework
    operation.creator = ledgerNode._peerId;
    operation.record.id = `https://example.com/event/${uuid()}`;
    operation.record.creator = ledgerNode.id;
    ledgerNode.operations.add(
      {operation, ledgerNode}, (err, result) => {
        if(err) {
          return callback(err);
        }
        operations[result.meta.operationHash] = operation;
        callback();
      });
  }, err => callback(err, operations));
};

api.addOperations = ({count, nodes, opTemplate}, callback) => {
  async.mapSeries(nodes, (ledgerNode, callback) => api.addOperation(
    {count, ledgerNode, opTemplate}, callback), callback);
};

// returns a different data structure than `api.addOpartions`
api.addOperations2 = ({count, nodes, opTemplate}, callback) => {
  const results = {};
  async.eachOf(nodes, (ledgerNode, key, callback) =>
    api.addOperation({count, ledgerNode, opTemplate}, (err, result) => {
      if(err) {
        return callback(err);
      }
      results[key] = result;
      callback();
    }),
  err => {
    if(err) {
      return callback(err);
    }
    callback(null, results);
  });
};

// add a merge event and regular event as if it came in through gossip
// NOTE: the events are rooted with the genesis merge event
api.addRemoteEvents = ({
  consensusApi, count = 1, ledgerNode, mockData
}, callback) => {
  const creatorId = mockData.exampleIdentity;
  async.timesSeries(count, (i, callback) => {
    const nodes = [].concat(ledgerNode);
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.operation[0].record.id =
      `https://example.com/event/${uuid()}`;
    const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    // NOTE: using the local branch head for treeHash of the remote merge event
    const {getHead} = consensusApi._events;
    async.auto({
      head: callback => getHead({
        // unknown creator will yield genesis merge event
        creatorId,
        ledgerNode
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        // in this example the merge event and the regular event
        // have a common ancestor which is the genesis merge event
        testMergeEvent.treeHash = result.eventHash;
        testMergeEvent.parentHash = [result.eventHash];
        testRegularEvent.treeHash = result.eventHash;
        testRegularEvent.parentHash = [result.eventHash];
        callback(null, result);
      }),
      regularEventHash: ['head', (results, callback) =>
        api.testHasher(testRegularEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          testMergeEvent.parentHash.push(result);
          callback(null, result);
        })],
      // FIXME: this helper is not currently being used, jsigs API call
      // will need to be updated
      sign: ['regularEventHash', (results, callback) => jsigs.sign(
        testMergeEvent, {
          algorithm: 'Ed25519Signature2018',
          privateKeyBase58: keyPair.privateKey,
          creator: mockData.authorizedSignerUrl
        }, callback)],
      addRegular: ['head', (results, callback) => async.map(
        nodes, (node, callback) => node.consensus._events.add({
          continuity2017: {peer: true},
          event: testRegularEvent, ledgerNode,
        }, callback),
        callback)],
      addMerge: ['sign', 'addRegular', (results, callback) => async.map(
        nodes, (node, callback) => node.consensus._events.add({
          continuity2017: {peer: true},
          event: results.sign, ledgerNode,
        }, callback), callback)],
    }, (err, results) => {
      if(err) {
        return callback(err);
      }
      const hashes = {
        merge: results.addMerge[0].meta.eventHash,
        regular: results.addRegular[0].meta.eventHash
      };
      callback(null, hashes);
    });
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    if(results.length === 1) {
      return callback(null, results[0]);
    }
    callback(null, results);
  });
};

api.buildHistory = ({consensusApi, historyId, mockData, nodes}, callback) => {
  const eventTemplate = mockData.events.alpha;
  const opTemplate = mockData.operations.alpha;
  async.auto(
    ledgerHistory[historyId]({
      api, consensusApi, eventTemplate, nodes, opTemplate
    }), (err, results) => {
      if(err) {
        return callback(err);
      }
      const copyMergeHashes = {};
      const copyMergeHashesIndex = {};
      Object.keys(results).forEach(key => {
        if(key.startsWith('cp')) {
          copyMergeHashes[key] = results[key].meta.eventHash;
          copyMergeHashesIndex[results[key].meta.eventHash] = key;
        }
      });
      const regularEvent = results.regularEvent;
      callback(null, {copyMergeHashes, copyMergeHashesIndex, regularEvent});
    });
};

// from may be a single node or an array of nodes
api.copyAndMerge = (
  {consensusApi, from, nodes, to, useSnapshot = false}, callback) => {
  const copyFrom = [].concat(from);
  const merge = consensusApi._events.merge;
  async.auto({
    copy: callback => async.eachSeries(copyFrom, (f, callback) =>
      api.copyEvents(
        {from: nodes[f], to: nodes[to], useSnapshot}, callback), callback),
    merge: ['copy', (results, callback) =>
      merge({creatorId: nodes[to].creatorId, ledgerNode: nodes[to]}, callback)]
  }, (err, results) => {
    err ? callback(err) : callback(null, results.merge);
  });
};

const snapshot = {};
api.copyEvents = ({from, to, useSnapshot = false}, callback) => {
  async.auto({
    events: callback => {
      const collection = from.storage.events.collection;
      if(useSnapshot && snapshot[collection.s.name]) {
        return callback(null, snapshot[collection.s.name]);
      }
      // FIXME: use a more efficient query, the commented aggregate function
      // is evidently missing some events.
      collection.find({
        'meta.consensus': false
      }, {'meta.eventHash': 1}).sort({$natural: 1}).toArray(
        (err, results) => {
          if(err) {
            return callback(err);
          }
          from.storage.events.getMany({
            eventHashes: results.map(r => r.meta.eventHash)
          }).toArray(callback);
        });
    },
    diff: ['events', (results, callback) => {
      const eventHashes = results.events.map(e => e.meta.eventHash);
      to.storage.events.difference(eventHashes, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.length === 0) {
          // return callback(new BedrockError('Nothing to do.', 'AbortError'));
          return callback(null, []);
        }
        const diffSet = new Set(result);
        const events = results.events
          .filter(e => diffSet.has(e.meta.eventHash));
        return callback(null, events);
      });
    }],
    add: ['diff', (results, callback) => {
      const events = results.diff;
      async.auto({
        addEvents: callback => async.eachSeries(
          events, (e, callback) => to.consensus._events._addTestEvent({
            event: e.event, ledgerNode: to,
          }, err => {
            // ignore dup errors
            if(err && err.name === 'DuplicateError') {
              return callback();
            }
            callback(err);
          }), callback),
        write: ['addEvents', (results, callback) => {
          to.eventWriter.write().then(() => callback(), callback);
        }]
      }, callback);
    }]
  }, callback);
};

api.createEvent = (
  {eventTemplate, eventNum, consensus = true, hash = true}, callback) => {
  const events = [];
  async.timesLimit(eventNum, 100, (i, callback) => {
    const event = bedrock.util.clone(eventTemplate);
    event.id = `https://example.com/events/${uuid()}`;
    const meta = {};
    if(consensus) {
      meta.consensus = true;
      meta.consensusDate = Date.now();
    }
    if(!hash) {
      events.push({event, meta});
      return callback();
    }
    api.testHasher(event, (err, result) => {
      meta.eventHash = result;
      events.push({event, meta});
      callback();
    });
  }, err => callback(err, events));
};

api.createEventBasic = ({eventTemplate}) => {
  const event = bedrock.util.clone(eventTemplate);
  event.operation[0].record.id = 'https://example.com/events/' + uuid();
  return event;
};

api.flushCache = () => cache.client.flushall();

api.nBlocks = ({
  consensusApi, nodes, operationOnWorkCycle = 'all', opTemplate,
  targetBlockHeight
}, callback) => {
  const recordIds = {};
  const targetBlockHashMap = {};
  let workCycle = 0;
  async.until(() => {
    return Object.keys(targetBlockHashMap).length ===
      Object.keys(nodes).length;
  }, callback => {
    const count = 1;
    workCycle++;
    let addOperation = true;
    if(operationOnWorkCycle === 'first' && workCycle > 1) {
      addOperation = false;
    }
    async.auto({
      operations: callback => {
        if(!addOperation) {
          return callback();
        }
        api.addOperations2({count, nodes, opTemplate}, callback);
      },
      workCycle: ['operations', (results, callback) => {
        // record the IDs for the records that were just added
        if(addOperation) {
          for(const n of Object.keys(nodes)) {
            if(!recordIds[n]) {
              recordIds[n] = [];
            }
            for(const opHash of Object.keys(results.operations[n])) {
              recordIds[n].push(results.operations[n][opHash].record.id);
            }
          }
        }
        // in this test `nodes` is an object that needs to be converted to
        // an array for the helper
        api.runWorkerCycle(
          {consensusApi, nodes: _.values(nodes), series: false}, callback);
      }],
      report: ['workCycle', (results, callback) => async.forEachOfSeries(
        nodes, (ledgerNode, i, callback) => {
          ledgerNode.storage.blocks.getLatestSummary((err, result) => {
            if(err) {
              return callback(err);
            }
            const {block} = result.eventBlock;
            if(block.blockHeight >= targetBlockHeight) {
              return ledgerNode.storage.blocks.getByHeight(
                targetBlockHeight, (err, result) => {
                  if(err) {
                    return callback(err);
                  }
                  targetBlockHashMap[i] = result.meta.blockHash;
                  callback();
                });
            }
            callback();
          });
        }, callback)]
    }, err => {
      if(err) {
        return callback(err);
      }
      callback();
    });
  }, err => {
    if(err) {
      return callback(err);
    }
    callback(null, {recordIds, targetBlockHashMap});
  });
};

// collections may be a string or array
api.removeCollections = async function(collections) {
  const collectionNames = [].concat(collections);
  await openCollections(collectionNames);
  for(const collectionName of collectionNames) {
    if(!database.collections[collectionName]) {
      return;
    }
    await database.collections[collectionName].remove({});
  }
};

api.prepareDatabase = async function() {
  await api.removeCollections([
    'identity', 'eventLog', 'ledger', 'ledgerNode', 'continuity2017_key',
    'continuity2017_manifest', 'continuity2017_vote', 'continuity2017_voter'
  ]);
};

api.runWorkerCycle = ({consensusApi, nodes, series = false}, callback) => {
  const func = series ? async.eachSeries : async.each;
  func(nodes.filter(n => !n.stop), (ledgerNode, callback) =>
    consensusApi._worker._run(ledgerNode, err => {
      // if a config change is detected, do not run worker on that node again
      if(err && err.name === 'LedgerConfigurationChangeError') {
        ledgerNode.stop = true;
        return callback();
      }
      callback(err);
    }), callback);
};

/*
 * execute the worker cycle until there are no non-consensus
 * events of type `WebLedgerOperationEvent` or `WebLedgerConfigurationEvent`
 * and the blockHeight on all nodes are the same. It is expected that
 * there will be various numbers of non-consensus events of type
 * `ContinuityMergeEvent` on a settled network.
 */
api.settleNetwork = ({consensusApi, nodes, series = false}, callback) => {
  async.doWhilst(callback => {
    async.auto({
      workCycle: callback => api.runWorkerCycle(
        {consensusApi, nodes, series}, callback),
      operationEvents: ['workCycle', (results, callback) => {
        async.map(nodes, (ledgerNode, callback) => {
          ledgerNode.storage.events.getCount({
            consensus: false, type: 'WebLedgerOperationEvent'
          }, callback);
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          // all nodes should have zero non-consensus regular events
          callback(null, result.every(c => c === 0));
        });
      }],
      configEvents: ['operationEvents', (results, callback) => {
        async.map(nodes, (ledgerNode, callback) => {
          ledgerNode.storage.events.getCount({
            consensus: false, type: 'WebLedgerConfigurationEvent'
          }, callback);
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          // all nodes should have zero non-consensus configuration events
          callback(null, result.every(c => c === 0));
        });
      }],
      // there are normal cases where the number of merge events on each node
      // will not be equal. If there are no outstanding operation/configuration
      // events, some merge events from other nodes may be left unmerged
      // which results in those merge events not being gossiped across the
      // entire network since they do not lend to acheiving consensus.
      /*
      mergeEvents: ['configEvents', (results, callback) => {
        async.map(nodes, (ledgerNode, callback) => {
          ledgerNode.storage.events.getCount({
            consensus: false, type: 'ContinuityMergeEvent'
          }, callback);
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          // all nodes should have an equal number of non-consensus merge events
          callback(null, result.every(c => c === result[0]));
        });
      }],
      */
      blocks: ['configEvents', (results, callback) => {
        async.map(nodes, (ledgerNode, callback) => {
          ledgerNode.storage.blocks.getLatestSummary(callback);
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          const blockHeights = result.map(s => s.eventBlock.block.blockHeight);
          // all nodes should have the same latest blockHeight
          callback(null, blockHeights.every(b => b === blockHeights[0]));
        });
      }]
    }, callback);
  }, results => {
    return !(results.operationEvents && results.configEvents && results.blocks);
  }, callback);
};

api.snapshotEvents = ({ledgerNode}, callback) => {
  const collection = ledgerNode.storage.events.collection;
  // FIXME: use a more efficient query, the commented aggregate function
  // is evidently missing some events.
  collection.find({
    'meta.consensus': false
  }).sort({$natural: 1}).toArray((err, result) => {
    if(err) {
      return callback(err);
    }
    ledgerNode.storage.events.getMany({
      eventHashes: result.map(r => r.meta.eventHash)
    }).toArray((err, results) => {
      if(err) {
        return err;
      }
      results = results.map(r => {
        if(r.event.type !== 'WebLedgerOperationEvent') {
          delete r.event.operation;
        }
        return r;
      });
      // make snapshot
      snapshot[collection.s.name] = results;
      callback(null, results);
    });
  });
};

// FIXME: remove and use brLedgerNode directly in tests
api.use = (plugin, callback) => {
  let p;
  try {
    p = brLedgerNode.use(plugin);
  } catch(e) {
    return callback(e);
  }
  callback(null, p);
};
