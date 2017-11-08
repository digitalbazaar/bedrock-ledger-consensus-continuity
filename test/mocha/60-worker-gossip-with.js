/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

describe('Worker - _gossipWith', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let ledgerNodeBeta;
  let peerId;
  let eventHash;
  let testEventId;
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
          peerId = result.id;
          callback();
        });
      }],
      genesisBlock: ['ledgerNode', (results, callback) =>
        ledgerNode.blocks.getGenesis((err, result) => {
          if(err) {
            return callback(err);
          }
          callback(null, result.genesisBlock.block);
        })],
      nodeBeta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNodeBeta = result;
          callback(null, result);
        })]
    }, done);
  });
  /*
    gossip wih ledgerNode from ledgerNodeBeta, there is no merge event on
    ledgerNode beyond the genesis merge event, so the gossip should complete
    without an error.  There is also nothing to be sent.
  */
  it('completes without an error when nothing to be received or sent', done => {
    async.auto({
      gossipWith: callback => consensusApi._worker._gossipWith(
        {ledgerNode: ledgerNodeBeta, peerId}, err => {
          should.not.exist(err);
          callback();
        })
    }, done);
  });
  /*
    gossip wih ledgerNode from ledgerNodeBeta. There is a regular event and a
    merge event on ledgerNode to be gossiped.  There is nothing to be sent from
    ledgerNodeBeta.
  */
  it('properly gossips one regular event and one merge event', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      mergeBranches: ['addEvent', (results, callback) =>
        mergeBranches(ledgerNode, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            should.not.exist(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // the events from ledgerNode should now be present on ledgerNodeBeta
        ledgerNodeBeta.storage.events.exists([
          results.addEvent.meta.eventHash,
          results.mergeBranches.meta.eventHash
        ], (err, result) => {
          assertNoError(err);
          result.should.be.true;
          callback();
        });
      }]
    }, done);
  });
  /*
    gossip wih ledgerNode from ledgerNodeBeta. There is a regular event and a
    merge event on ledgerNode to be gossiped. There is a regular event and a
    merge event from a fictitious node as well. There is nothing to be sent from
    ledgerNodeBeta.
  */
  it('properly gossips two regular events and two merge events', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode, mockData}, callback),
      mergeBranches: ['addEvent', 'remoteEvents', (results, callback) =>
        mergeBranches(ledgerNode, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            should.not.exist(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // the events from ledgerNode should now be present on ledgerNodeBeta
        ledgerNodeBeta.storage.events.exists([
          results.remoteEvents.merge,
          results.remoteEvents.regular,
          results.addEvent.meta.eventHash,
          results.mergeBranches.meta.eventHash
        ], (err, result) => {
          assertNoError(err);
          result.should.be.true;
          callback();
        });
      }]
    }, done);
  });
  /*
    gossip with ledgerNode from ledgerNodeBeta. There are no new events on
    ledgerNode, but ledgerNodeBeta has one regular event and one merge event
    to be push gossipped.
  */
  it('properly push gossips a regular event and a merge event', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      addEvent: callback => ledgerNodeBeta.events.add(testEvent, callback),
      mergeBranches: ['addEvent', (results, callback) =>
        mergeBranches(ledgerNodeBeta, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            should.not.exist(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // the events from ledgerNodeBeta should now be present on ledgerNode
        ledgerNode.storage.events.exists([
          results.addEvent.meta.eventHash,
          results.mergeBranches.meta.eventHash
        ], (err, result) => {
          assertNoError(err);
          result.should.be.true;
          callback();
        });
      }]
    }, done);
  });
  /*
    gossip wih ledgerNode from ledgerNodeBeta. There are no new events on
    ledgerNode, but there is a regular event and a merge event from ledgerNode
    as well as a regular event and merged event from a fictitious node on
    ledgerNode to be gossiped to ledgerNodeBeta.
  */
  it('properly push gossips two regular events and two merge events', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      addEvent: callback => ledgerNodeBeta.events.add(testEvent, callback),
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode: ledgerNodeBeta, mockData}, callback),
      mergeBranches: ['addEvent', 'remoteEvents', (results, callback) =>
        mergeBranches(ledgerNodeBeta, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            should.not.exist(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // the events from ledgerNode should now be present on ledgerNodeBeta
        ledgerNode.storage.events.exists([
          results.remoteEvents.merge,
          results.remoteEvents.regular,
          results.addEvent.meta.eventHash,
          results.mergeBranches.meta.eventHash
        ], (err, result) => {
          assertNoError(err);
          result.should.be.true;
          callback();
        });
      }]
    }, done);
  });
  /*
    ledgerNode and ledgerNodeBeta each have unique local regular events.
    The also have the same set of regular event and merge event communicated
    to them by a fictitious node. ledgerNode and ledgeNodeBeta have eached
    merged the events from the fictitious node into their respective histories.
  */
  it('properly gossips in both directions', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    const testEventBeta = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    const testEventBetaId = 'https://example.com/events/' + uuid();
    testEventBeta.input[0].id = testEventBetaId;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      addEventBeta: callback => ledgerNodeBeta.events.add(
        testEventBeta, callback),
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode: [ledgerNode, ledgerNodeBeta], mockData},
        callback),
      mergeBranches: ['addEvent', 'remoteEvents', (results, callback) =>
        mergeBranches(ledgerNode, callback)],
      mergeBranchesBeta: ['addEventBeta', 'remoteEvents', (results, callback) =>
        mergeBranches(ledgerNodeBeta, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            should.not.exist(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // ledgerNode and ledgerNode beta should have the same events
        async.auto({
          ledgerNode: callback => ledgerNode.storage.events.exists([
            results.remoteEvents.merge,
            results.remoteEvents.regular,
            results.addEvent.meta.eventHash,
            results.mergeBranches.meta.eventHash,
            results.addEventBeta.meta.eventHash,
            results.mergeBranchesBeta.meta.eventHash
          ], (err, result) => {
            assertNoError(err);
            result.should.be.true;
            callback();
          }),
          ledgerNodeBeta: ['ledgerNode', (resultsInner, callback) =>
            ledgerNodeBeta.storage.events.exists([
              results.remoteEvents.merge,
              results.remoteEvents.regular,
              results.addEvent.meta.eventHash,
              results.mergeBranches.meta.eventHash,
              results.addEventBeta.meta.eventHash,
              results.mergeBranchesBeta.meta.eventHash
            ], (err, result) => {
              assertNoError(err);
              result.should.be.true;
              callback();
            })]
        }, callback);
      }]
    }, done);
  });
});
