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
  it('should add a remote merge event', done => {
    const testUrl = voterId + '/events';
    const testEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    async.auto({
      sign: callback => jsigs.sign(testEvent, {
        algorithm: 'LinkedDataSignature2015',
        privateKeyPem: keyPair.privateKey,
        creator: mockData.authorizedSignerUrl
      }, callback),
      eventHash: ['sign', (results, callback) =>
        hasher(results.sign, callback)],
      addEvent: ['eventHash', (results, callback) =>
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
  it('should add a regular remote event', done => {
    const testUrl = voterId + '/events';
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.input[0].id = uuid();
    const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    testRegularEvent.treeHash =
      'ni:///sha-256;1rj73NTf8Nx3fhGrwHo7elDCF7dfdUqPoK2tzpf-NNN';
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    async.auto({
      regularEventHash: callback => hasher(testRegularEvent, callback),
      build: ['regularEventHash', (results, callback) => {
        testMergeEvent.parentHash = [results.regularEventHash];
        callback(null, testMergeEvent);
      }],
      sign: ['build', (results, callback) => jsigs.sign(results.build, {
        algorithm: 'LinkedDataSignature2015',
        privateKeyPem: keyPair.privateKey,
        creator: mockData.authorizedSignerUrl
      }, callback)],
      mergeEventHash: ['sign', (results, callback) =>
        hasher(results.sign, callback)],
      addMerge: ['mergeEventHash', (results, callback) => request.post({
        url: testUrl,
        json: {eventHash: results.mergeEventHash, event: results.sign}
      }, (err, res) => {
        assertNoError(err);
        res.statusCode.should.equal(201);
        should.exist(res.headers.location);
        callback();
      })],
      addRegular: ['addMerge', (results, callback) => request.post({
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
});
