/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
let request = require('request');
request = request.defaults({json: true, strictSSL: false});
const uuid = require('uuid/v4');

describe('Consensus Agent - Get Event API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  let eventHash;
  let testEventId;
  beforeEach(done => {
    const configEvent = mockData.events.config;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedger.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedger.add(
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
      }],
      addEvent: ['ledgerNode', (results, callback) => ledgerNode.events.add(
        testEvent, (err, result) => {
          eventHash = result.meta.eventHash;
          callback();
        })]
    }, done);
  });
  it('should get a manifest', done => {
    async.auto({
      createManifest: callback => consensusApi._worker._election
        ._createEventManifest(ledgerNode, 1, callback),
      get: ['createManifest', (results, callback) => {
        const testUrl = voterId + '/manifests?id=' + results.createManifest.id;
        request.get(testUrl, (err, res) => {
          should.not.exist(err);
          res.statusCode.should.equal(200);
          const result = res.body;
          result.should.be.an('object');
          should.exist(result.id);
          should.exist(result.blockHeight);
          result.blockHeight.should.equal(1);
          should.exist(result.item);
          result.item.should.be.an('array');
          result.item.should.have.length(1);
          result.item.should.have.members([eventHash]);
          callback();
        });
      }]
    }, done);
  });
});
