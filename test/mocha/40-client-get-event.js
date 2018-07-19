/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

describe.skip('Consensus Client - getEvent API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let peerId;
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
        helpers.use('Continuity2017', (err, result) => {
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
          peerId = result.id;
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
    async.auto({
      get: callback => consensusApi._worker._client.getEvent(
        {eventHash, peerId}, (err, result) => {
          should.not.exist(err);
          should.exist(result);
          result.should.be.an('object');
          result.operation.should.be.an('array');
          result.operation.should.have.length(1);
          result.operation[0].should.be.an('object');
          result.operation[0].record.id.should.equal(testEventId);
          callback();
        })
    }, done);
  });
  it('returns an error on an unknown event', done => {
    async.auto({
      get: callback => consensusApi._worker._client.getEvent(
        {eventHash: uuid(), peerId}, (err, result) => {
          should.exist(err);
          should.not.exist(result);
          err.details.httpStatusCode.should.equal(404);
          callback();
        })
    }, done);
  });
});
