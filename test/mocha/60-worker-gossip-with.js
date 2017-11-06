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
  let voterId;
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
          voterId = result.id;
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
        {ledgerNode: ledgerNodeBeta, peerId: voterId}, err => {
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
          {ledgerNode: ledgerNodeBeta, peerId: voterId}, err => {
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
          {ledgerNode: ledgerNodeBeta, peerId: voterId}, err => {
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
});
