/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
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

describe.skip('Consensus Agent - Get Event API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  let eventHash;
  let testEventId;
  beforeEach(done => {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
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
        null, {ledgerConfiguration}, (err, result) => {
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
      addEvent: ['ledgerNode', (results, callback) =>
        ledgerNode.consensus._events.add(
          testEvent, ledgerNode, (err, result) => {
          eventHash = result.meta.eventHash;
          callback();
        })]
    }, done);
  });
  it('should get an event', done => {
    const testUrl = voterId + '/events?id=' + eventHash;
    async.auto({
      get: callback => request.get(testUrl, (err, res) => {
        should.not.exist(err);
        res.statusCode.should.equal(200);
        const result = res.body;
        should.exist(result.operation);
        result.operation.should.be.an('array');
        result.operation.should.have.length(1);
        should.exist(result.operation[0].record);
        result.operation[0].record.id.should.equal(testEventId);
        callback();
      })
    }, done);
  });
});
