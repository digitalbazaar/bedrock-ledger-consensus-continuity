/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const {callbackify} = require('util');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const {util: {uuid}} = bedrock;

describe.skip('Consensus Client - sendEvent API', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });

  let consensusApi;
  let ledgerNode;
  let peerId;
  beforeEach(done => {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      clean: callback =>
        callbackify(helpers.removeCollections)(
          ['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        callbackify(helpers.use)('Continuity2017', (err, result) => {
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
          peerId = result.id;
          callback();
        });
      }]
    }, done);
  });
  it('should send an event', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    const testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
    const {getHead} = ledgerNode.consensus._events;
    async.auto({
      head: callback => getHead({
        creatorId: peerId,
        ledgerNode
      }, (err, result) => {
        testEvent.parentHash = [result];
        testEvent.treeHash = result;
        callback();
      }),
      hash: ['head', (results, callback) =>
        helpers.testHasher(testEvent, callback)],
      send: ['hash', (results, callback) =>
        consensusApi._client.sendEvent(
          {eventHash: results.hash, event: testEvent, peerId},
          (err, result) => {
            assertNoError(err);
            should.exist(result);
            result.should.be.a('string');
            callback();
          })]
    }, done);
  });
  it('returns an error when a peer is unreachable.', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    const testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
    async.auto({
      hash: callback => brLedgerNode.consensus._hasher(testEvent, callback),
      send: ['hash', (results, callback) =>
        consensusApi._client.sendEvent({
          eventHash: results.hash,
          event: testEvent,
          peerId: 'https://' + uuid() + '.com'
        }, (err, result) => {
          should.exist(err);
          err.name.should.equal('NetworkError');
          should.not.exist(result);
          callback();
        })]
    }, done);
  });
  it('returns an error', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    // wipe out the event data to create failure
    testEvent.operation = [];
    async.auto({
      hash: callback => brLedgerNode.consensus._hasher(testEvent, callback),
      send: ['hash', (results, callback) =>
        consensusApi._client.sendEvent({
          eventHash: results.hash,
          event: testEvent,
          peerId
        }, (err, result) => {
          should.exist(err);
          err.name.should.equal('NetworkError');
          err.details.error.type.should.equal('ValidationError');
          should.not.exist(result);
          callback();
        })]
    }, done);
  });
});
