/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const hasher = brLedgerNode.consensus._hasher;
const helpers = require('./helpers');
const jsigs = require('jsonld-signatures')();
const jsonld = bedrock.jsonld;
const mockData = require('./mock.data');
let request = require('request');
request = request.defaults({json: true, strictSSL: false});
const uuid = require('uuid/v4');

jsigs.use('jsonld', jsonld);

describe('Consensus Agent - Add Event API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  beforeEach(done => {
    const configEvent = mockData.events.config;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedgerNode.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {configEvent}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNode = result;
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          voterId = result.id;
          callback();
        });
      }]
    }, done);
  });
  it('should add a regular remote event', done => {
    const testUrl = voterId + '/events';
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.input[0].id = `https://example.com/event/${uuid()}`;
    const getHead = consensusApi._worker._events._getLocalBranchHead;
    async.auto({
      head: callback => getHead(
        ledgerNode.storage.events.collection, (err, result) => {
          if(err) {
            return callback(err);
          }
          // the ancestor is the genesis merge event
          testRegularEvent.treeHash = result;
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
    testRegularEvent.input[0].id = `https://example.com/event/${uuid()}`;
    const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    const getHead = consensusApi._worker._events._getLocalBranchHead;
    async.auto({
      head: callback => getHead(
        ledgerNode.storage.events.collection, (err, result) => {
          if(err) {
            return callback(err);
          }
          // in this example the merge event and the regular event
          // have a common ancestor which is the genesis merge event
          testMergeEvent.treeHash = result;
          testRegularEvent.treeHash = result;
          callback(null, result);
        }),
      regularEventHash: ['head', (results, callback) =>
        hasher(testRegularEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          testMergeEvent.parentHash = [result];
          callback(null, result);
        })],
      sign: ['regularEventHash', (results, callback) =>
        jsigs.sign(testMergeEvent, {
          algorithm: 'LinkedDataSignature2015',
          privateKeyPem: keyPair.privateKey,
          creator: mockData.authorizedSignerUrl
        }, callback)],
      eventHash: ['sign', (results, callback) =>
        hasher(results.sign, callback)],
      addRegular: ['head', (results, callback) => ledgerNode.events.add(
        testRegularEvent, {continuity2017: {peer: true}}, callback)],
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
