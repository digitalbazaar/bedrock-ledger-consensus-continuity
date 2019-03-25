/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const hasher = brLedgerNode.consensus._hasher;
const helpers = require('./helpers');
const jsigs = require('jsonld-signatures')();
const mockData = require('./mock.data');
let request = require('request');
request = request.defaults({json: true, strictSSL: false});
const {jsonld, util: {uuid}} = bedrock;

jsigs.use('jsonld', jsonld);

describe.skip('Consensus Agent - Add Event API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  beforeEach(done => {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        helpers.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNode = result;
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._voters.get(ledgerNode.id, (err, result) => {
          voterId = result.id;
          callback();
        });
      }]
    }, done);
  });
  it('should add a regular remote event', done => {
    const testUrl = voterId + '/events';
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.operation[0].record.id =
      `https://example.com/event/${uuid()}`;
    const {getHead} = consensusApi._events;
    async.auto({
      head: callback => getHead({
        ledgerNodeId: ledgerNode.id,
        eventsCollection: ledgerNode.storage.events.collection,
        creatorId: voterId
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        // the ancestor is the genesis merge event
        testRegularEvent.treeHash = result;
        testRegularEvent.parentHash = [result];
        callback(null, result);
      }),
      regularEventHash: ['head', (results, callback) =>
        hasher(testRegularEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          callback(null, result);
        })],
      addRegular: ['regularEventHash', (results, callback) => request.post({
        url: testUrl,
        json: {eventHash: results.regularEventHash, event: testRegularEvent}
      }, (err, res) => {
        assertNoError(err);
        res.statusCode.should.equal(201);
        should.exist(res.headers.location);
        callback();
      })]
    }, done);
  });
  it('should add a remote merge event', done => {
    const testUrl = voterId + '/events';
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.operation[0].record.id =
      `https://example.com/event/${uuid()}`;
    const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    const {getHead} = consensusApi._events;
    async.auto({
      head: callback => getHead({
        ledgerNodeId: ledgerNode.id,
        eventsCollection: ledgerNode.storage.events.collection,
        creatorId: voterId
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
        hasher(testRegularEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          testMergeEvent.parentHash = [result, results.head];
          callback(null, result);
        })],
      sign: ['regularEventHash', (results, callback) =>
        jsigs.sign(testMergeEvent, {
          algorithm: 'Ed25519Signature2018',
          privateKeyBase58: keyPair.privateKey,
          creator: mockData.authorizedSignerUrl
        }, callback)],
      eventHash: ['sign', (results, callback) =>
        hasher(results.sign, callback)],
      addRegular: ['head', (results, callback) =>
        ledgerNode.consensus._events.add(
          testRegularEvent, ledgerNode,
          {continuity2017: {peer: true}}, callback)],
      addEvent: ['addRegular', 'eventHash', (results, callback) =>
        request.post({
          url: testUrl,
          json: {eventHash: results.eventHash, event: results.sign}
        }, (err, res) => {
          assertNoError(err);
          res.statusCode.should.equal(201);
          should.exist(res.headers.location);
          callback();
        })]
    }, done);
  });
});
