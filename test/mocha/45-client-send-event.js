/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
/* globals should, assertNoError */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

describe('Consensus Client - sendEvent API', () => {
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
      }]
    }, done);
  });
  // FIXME: are events supposed to be signed?
  it('should send an event', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    const testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      send: callback => consensusApi._worker._client.sendEvent(
        testEvent, voterId, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.a('string');
          callback();
        })
    }, done);
  });
  it('returns an error when a peer is unreachable.', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    const testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      get: callback => consensusApi._worker._client.sendEvent(
        testEvent, {id: 'https://' + uuid() + '.com'}, (err, result) => {
          should.exist(err);
          err.name.should.equal('NetworkError');
          should.not.exist(result);
          callback();
        })
    }, done);
  });
  it('returns an error', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    // wipe out the event data to create failure
    testEvent.input = [];
    async.auto({
      get: callback => consensusApi._worker._client.sendEvent(
        testEvent, voterId, (err, result) => {
          should.exist(err);
          err.name.should.equal('NetworkError');
          err.details.error.type.should.equal('ValidationError');
          should.not.exist(result);
          callback();
        })
    }, done);
  });
});
