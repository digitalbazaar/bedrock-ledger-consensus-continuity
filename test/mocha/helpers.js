/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
/* jshint node: true */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const database = require('bedrock-mongodb');
const jsigs = require('jsonld-signatures')();
const jsonld = bedrock.jsonld;
const uuid = require('uuid/v4');

jsigs.use('jsonld', jsonld);

const api = {};
module.exports = api;

api.average = arr => Math.round(arr.reduce((p, c) => p + c, 0) / arr.length);

// test hashing function
api.testHasher = brLedgerNode.consensus._hasher;

// add a merge event and regular event as if it came in through gossip
// NOTE: the events are rooted with the genesis merge event
api.addRemoteEvents = ({consensusApi, ledgerNode, mockData}, callback) => {
  const nodes = [].concat(ledgerNode);
  const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
  testRegularEvent.input[0].id = `https://example.com/event/${uuid()}`;
  const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
  // use a valid keypair from mocks
  const keyPair = mockData.groups.authorized;
  // NOTE: using the loal branch head for treeHash of the remote merge event
  const getHead = consensusApi._worker._events._getLocalBranchHead;
  async.auto({
    head: callback => getHead({
      eventsCollection: nodes[0].storage.events.collection,
      // unknown creator will yield genesis merge event
      creator: 'http://example.com/unknownCreator'
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
        testRegularEvent, {continuity2017: {peer: true}}, callback), callback)],
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
        'identity', 'eventLog', 'ledger', 'ledgerNode',
        'continuity2017_manifest', 'continuity2017_vote', 'continuity2017_voter'
      ], callback);
    },
    callback => {
      insertTestData(mockData, callback);
    }
  ], callback);
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
