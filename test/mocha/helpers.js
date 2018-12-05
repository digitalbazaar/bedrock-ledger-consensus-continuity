/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
const hasher = brLedgerNode.consensus._hasher;
const jsigs = require('jsonld-signatures')();
const jsonld = bedrock.jsonld;
const uuid = require('uuid/v4');
// const util = require('util');
// const BedrockError = bedrock.util.BedrockError;

jsigs.use('jsonld', jsonld);

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

api.addEvent = (
  {count = 1, eventTemplate, ledgerNode, opTemplate}, callback) => {
  const events = {};
  const {creatorId} = ledgerNode;
  let operations;
  async.timesSeries(count, (i, callback) => {
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
    async.auto({
      head: callback => ledgerNode.consensus._events.getHead(
        {creatorId, ledgerNode}, (err, result) => {
          if(err) {
            return callback(err);
          }
          const {eventHash: headHash} = result;
          testEvent.parentHash = [headHash];
          testEvent.treeHash = headHash;
          callback();
        }),
      operationHash: callback => hasher(operation, (err, opHash) => {
        if(err) {
          return callback(err);
        }
        testEvent.operationHash = [opHash];
        callback(null, opHash);
      }),
      eventHash: ['head', 'operationHash', (results, callback) => hasher(
        testEvent, callback)],
      operation: ['eventHash', (results, callback) => {
        const {eventHash, operationHash} = results;
        operations = [{
          meta: {
            basisBlockHeight: 1,
            operationHash,
            recordId: ledgerNode.storage.driver.hash(testRecordId),
          },
          operation,
        }];
        ledgerNode.consensus.operations.write(
          {eventHash, ledgerNode, operations}, callback);
      }],
      event: ['operation', (results, callback) => {
        const {eventHash} = results;
        ledgerNode.consensus._events.add(
          {event: testEvent, eventHash, ledgerNode}, (err, result) => {
            if(err) {
              return callback(err);
            }
            result.operations = operations;
            events[result.meta.eventHash] = result;
            callback();
          });
      }]
    }, callback);
  }, err => callback(err, events));
};

api.addEventAndMerge = ({
  consensusApi, count = 1, eventTemplate, ledgerNode, opTemplate
}, callback) => {
  if(!(consensusApi && eventTemplate && ledgerNode)) {
    throw new TypeError(
      '`consensusApi`, `eventTemplate`, and `ledgerNode` are required.');
  }
  const events = {};
  const merge = consensusApi._events.merge;
  async.auto({
    addEvent: callback => api.addEvent(
      {count, eventTemplate, ledgerNode, opTemplate}, (err, result) => {
        if(err) {
          return callback(err);
        }
        events.regular = result;
        events.regularHashes = Object.keys(result);
        callback();
      }),
    merge: ['addEvent', (results, callback) => merge(
      {creatorId: ledgerNode.creatorId, ledgerNode}, (err, result) => {
        if(err) {
          return callback(err);
        }
        events.merge = result;
        events.mergeHash = result.meta.eventHash;
        events.allHashes = [events.mergeHash, ...events.regularHashes];
        callback();
      })]
  }, err => callback(err, events));
};

api.addEventMultiNode = (
  {consensusApi, eventTemplate, nodes, opTemplate}, callback) => {
  const rVal = {
    mergeHash: [],
    regularHash: []
  };
  async.eachOfSeries(nodes, (ledgerNode, i, callback) => {
    api.addEventAndMerge({
      consensusApi, eventTemplate, ledgerNode, opTemplate
    }, (err, result) => {
      if(err) {
        return callback(err);
      }
      rVal[i] = result;
      callback();
    });
  },
  err => {
    if(err) {
      return callback(err);
    }
    Object.keys(nodes).forEach(k => {
      rVal.regularHash.push(Object.keys(rVal[k].regular)[0]);
      rVal.mergeHash.push(rVal[k].merge.meta.eventHash);
    });
    callback(null, rVal);
  });
};

// this helper is for test that execute the consensus worker
api.addOperation = ({count = 1, ledgerNode, opTemplate}, callback) => {
  const operations = {};
  async.timesSeries(count, (i, callback) => {
    const operation = bedrock.util.clone(opTemplate);
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
      }, {'meta.eventHash': 1}).sort({'$natural': 1}).toArray(
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
      const needed = events.map(r => r.meta.eventHash);
      async.auto({
        addEvents: callback => async.eachSeries(
          events, (e, callback) => to.consensus._events.add({
            continuity2017: {peer: true}, event: e.event, ledgerNode: to,
            needed
          }, err => {
            // ignore dup errors
            if(err && err.name === 'DuplicateError') {
              return callback();
            }
            callback(err);
          }), callback),
        write: ['addEvents', (results, callback) =>
          to.eventWriter.start(callback)]
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

api.createIdentity = function(userName) {
  const newIdentity = {
    id: 'did:' + uuid(),
    type: 'Identity',
    sysSlug: userName,
    label: userName,
    email: userName + '@bedrock.dev',
    sysPassword: 'password',
    sysPublic: ['label', 'url', 'description'],
    sysResourceRole: [],
    url: 'https://example.com',
    description: userName,
    sysStatus: 'active'
  };
  return newIdentity;
};

api.flushCache = callback => cache.client.flushall(callback);

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
api.removeCollections = function(collections, callback) {
  const collectionNames = [].concat(collections);
  database.openCollections(collectionNames, () => {
    async.each(collectionNames, function(collectionName, callback) {
      if(!database.collections[collectionName]) {
        return callback();
      }
      database.collections[collectionName].remove({}, callback);
    }, function(err) {
      callback(err);
    });
  });
};

api.prepareDatabase = function(mockData, callback) {
  async.series([
    callback => {
      api.removeCollections([
        'identity', 'eventLog', 'ledger', 'ledgerNode', 'continuity2017_key',
        'continuity2017_manifest', 'continuity2017_vote', 'continuity2017_voter'
      ], callback);
    },
    callback => {
      insertTestData(mockData, callback);
    }
  ], callback);
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
  }).sort({'$natural': 1}).toArray((err, result) => {
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

api.use = (plugin, callback) => {
  let p;
  try {
    p = brLedgerNode.use(plugin);
  } catch(e) {
    return callback(e);
  }
  callback(null, p);
};

// Insert identities and public keys used for testing into database
function insertTestData(mockData, callback) {
  async.forEachOf(mockData.identities, (identity, key, callback) => {
    brIdentity.insert(null, identity.identity, callback);
  }, err => {
    if(err) {
      if(!database.isDuplicateError(err)) {
        // duplicate error means test data is already loaded
        return callback(err);
      }
    }
    callback();
  }, callback);
}
