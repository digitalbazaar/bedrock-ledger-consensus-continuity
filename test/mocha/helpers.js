/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
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

api.addEvent = ({count = 1, eventTemplate, ledgerNode}, callback) => {
  const events = {};
  async.timesSeries(count, (i, callback) => {
    const event = bedrock.util.clone(eventTemplate);
    event.operation[0].record.id = `https://example.com/event/${uuid()}`;
    ledgerNode.consensus._events.add({event, ledgerNode}, (err, result) => {
      if(err) {
        return callback(err);
      }
      events[result.meta.eventHash] = result;
      callback();
    });
  }, err => callback(err, events));
};

api.addEventAndMerge = (
  {consensusApi, count = 1, eventTemplate, ledgerNode}, callback) => {
  if(!(consensusApi && eventTemplate && ledgerNode)) {
    throw new TypeError(
      '`consensusApi`, `eventTemplate`, and `ledgerNode` are required.');
  }
  const events = {};
  const merge = consensusApi._worker._events.merge;
  async.auto({
    addEvent: callback => api.addEvent(
      {count, eventTemplate, ledgerNode}, (err, result) => {
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
  {consensusApi, eventTemplate, nodes}, callback) => {
  const rVal = {
    mergeHash: [],
    regularHash: []
  };
  async.eachOf(nodes, (n, i, callback) =>
    api.addEventAndMerge({
      consensusApi, creatorId: n.creatorId, eventTemplate, ledgerNode: n
    }, (err, result) => {
      if(err) {
        return callback(err);
      }
      rVal[i] = result;
      callback();
    }),
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
    const getHead = consensusApi._worker._events._getLocalBranchHead;
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
  async.auto(
    ledgerHistory[historyId](api, consensusApi, eventTemplate, nodes),
    (err, results) => {
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
  const merge = consensusApi._worker._events.merge;
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
        'meta.consensus': {$exists: false}
      }).sort({'$natural': 1}).toArray(callback);
    },
    diff: ['events', (results, callback) => {
      const eventHashes = results.events.map(e => e.eventHash);
      to.storage.events.difference(eventHashes, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.length === 0) {
          // return callback(new BedrockError('Nothing to do.', 'AbortError'));
          return callback();
        }
        const diffSet = new Set(result);
        const events = results.events.filter(e => diffSet.has(e.eventHash));
        return callback(null, events);
      });
    }],
    add: ['diff', (results, callback) => {
      const events = results.diff;
      async.auto({
        addEvents: callback => async.eachSeries(
          events, (e, callback) => to.consensus._events.add({
            continuity2017: {peer: true}, event: e.event, ledgerNode: to
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

api.snapshotEvents = ({ledgerNode}, callback) => {
  const collection = ledgerNode.storage.events.collection;
  // FIXME: use a more efficient query, the commented aggregate function
  // is evidently missing some events.
  collection.find({
    'meta.consensus': {$exists: false}
  }).sort({'$natural': 1}).toArray((err, result) => {
    if(err) {
      return callback(err);
    }
    // make snapshot
    snapshot[collection.s.name] = result;
    callback(null, result);
  });
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
