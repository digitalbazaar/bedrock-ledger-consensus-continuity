/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const database = require('bedrock-mongodb');
const forge = require('node-forge');
const jsigs = require('jsonld-signatures')();
const jsonld = bedrock.jsonld;
const uuid = require('uuid/v4');
const util = require('util');

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
    const testEvent = bedrock.util.clone(eventTemplate);
    testEvent.input[0].id = `https://example.com/event/${uuid()}`;
    ledgerNode.events.add(testEvent, (err, result) => {
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
  const getRecentHistory = consensusApi._worker._events.getRecentHistory;
  const mergeBranches = consensusApi._worker._events.mergeBranches;
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
    history: ['addEvent', (results, callback) =>
      getRecentHistory({ledgerNode}, callback)],
    merge: ['history', (results, callback) => mergeBranches(
      {history: results.history, ledgerNode}, (err, result) => {
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

api.addEventMultiNode = ({consensusApi, eventTemplate, nodes}, callback) => {
  const rVal = {
    mergeHash: [],
    regularHash: []
  };
  async.eachOf(nodes, (n, i, callback) =>
    api.addEventAndMerge(
      {consensusApi, eventTemplate, ledgerNode: n}, (err, result) => {
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
  const creator = mockData.exampleIdentity;
  async.timesSeries(count, (i, callback) => {
    const nodes = [].concat(ledgerNode);
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.input[0].id = `https://example.com/event/${uuid()}`;
    const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    // NOTE: using the local branch head for treeHash of the remote merge event
    const getHead = consensusApi._worker._events._getLocalBranchHead;
    async.auto({
      head: callback => getHead({
        eventsCollection: nodes[0].storage.events.collection,
        // unknown creator will yield genesis merge event
        creator
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        // in this example the merge event and the regular event
        // have a common ancestor which is the genesis merge event
        testMergeEvent.treeHash = result;
        testRegularEvent.treeHash = result;
        testRegularEvent.parentHash = [result];
        callback(null, result);
      }),
      regularEventHash: ['head', (results, callback) =>
        api.testHasher(testRegularEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          testMergeEvent.parentHash = [result, results.head];
          callback(null, result);
        })],
      sign: ['regularEventHash', (results, callback) => jsigs.sign(
        testMergeEvent, {
          algorithm: 'LinkedDataSignature2015',
          privateKeyPem: keyPair.privateKey,
          creator: mockData.authorizedSignerUrl
        }, callback)],
      addRegular: ['head', (results, callback) => async.map(
        nodes, (node, callback) => node.events.add(
          testRegularEvent, {continuity2017: {peer: true}}, callback),
        callback)],
      addMerge: ['sign', 'addRegular', (results, callback) => async.map(
        nodes, (node, callback) => node.events.add(
          results.sign, {continuity2017: {peer: true}}, callback), callback)],
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
  {consensusApi, from, to, useSnapshot = false}, callback) => {
  const copyFrom = [].concat(from);
  const getRecentHistory = consensusApi._worker._events.getRecentHistory;
  const mergeBranches = consensusApi._worker._events.mergeBranches;
  async.auto({
    copy: callback => async.each(copyFrom, (f, callback) =>
      api.copyEvents({from: f, to, useSnapshot}, callback), callback),
    history: ['copy', (results, callback) =>
      getRecentHistory({ledgerNode: to}, callback)],
    merge: ['history', (results, callback) =>
      mergeBranches({history: results.history, ledgerNode: to}, callback)]
  }, (err, results) => err ? callback(err) : callback(null, results.merge));
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
      // collection.aggregate([
      //   {$match: {eventHash}},
      //   {
      //     $graphLookup: {
      //       from: collection.s.name,
      //       startWith: '$eventHash',
      //       connectFromField: "event.parentHash",
      //       connectToField: "eventHash",
      //       as: "_parents",
      //       restrictSearchWithMatch: {
      //         eventHash: {$ne: treeHash},
      //         'meta.consensus': {$exists: false}
      //       }
      //     },
      //   },
      //   {$unwind: '$_parents'},
      //   {$replaceRoot: {newRoot: '$_parents'}},
      //   // the order of events is unpredictable without this sort, and we
      //   // must ensure that events are added in chronological order
      //   {$sort: {'meta.created': 1}}
      // ], callback);
    },
    add: ['events', (results, callback) => {
      async.eachSeries(results.events, (e, callback) => {
        to.events.add(e.event, {continuity2017: {peer: true}}, err => {
          // ignore dup errors
          if(err && err.name === 'DuplicateError') {
            return callback();
          }
          if(err) {
            console.log('ERRR', err);
            console.log('------------EVENT', util.inspect(e.event));
          }
          callback();
        });
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
  event.input[0].id = 'https://example.com/events/' + uuid();
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

api.deterministicCompareTallies = ({a, b}) => {
  const lengthA = a.set.length;
  const lengthB = b.set.length;
  if(lengthA !== lengthB) {
    return lengthA < lengthB;
  }
  const setA = a.set.map(e => e.meta.continuity2017.creator)
    .map(c => api.peersReverse[c])
    .sort((a, b) => a.localeCompare(b));
  const setB = b.set.map(e => e.meta.continuity2017.creator)
    .map(c => api.peersReverse[c])
    .sort((a, b) => a.localeCompare(b));
  let rVal;
  for(let i = 0; i < lengthA; ++i) {
    rVal = setA[i].localeCompare(setB[i]);
    if(rVal !== 0) {
      break;
    }
  }
  return !!rVal;
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

/**
 * Get the SubjectPublicKeyInfo base64url-encoded fingerprint for the
 * given public key PEM.
 *
 * @param publicKeyPem the public key PEM to get the fingerprint for.
 *
 * @return the public key's fingerprint.
 */
api.getPublicKeyFingerprint = publicKeyPem => {
  const publicKey = forge.pki.publicKeyFromPem(publicKeyPem);
  const fingerprint = forge.pki.getPublicKeyFingerprint(
    publicKey, {
      md: forge.md.sha256.create(),
      type: 'SubjectPublicKeyInfo'
    });
  // base64url encode
  return forge.util.encode64(fingerprint.getBytes())
    .replace(/\+/g, '-')
    .replace(/\//g, '_')
    .replace(/=/g, '');
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
