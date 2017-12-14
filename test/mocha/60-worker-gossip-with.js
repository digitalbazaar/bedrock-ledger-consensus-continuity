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

describe.only('Worker - _gossipWith', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let eventHash;
  let getRecentHistory;
  let ledgerNode;
  let ledgerNodeBeta;
  let ledgerNodeGamma;
  let ledgerNodeDelta;
  let mergeBranches;
  let peerId;
  let peerBetaId;
  let peerDeltaId;
  let peerGammaId;
  let testEventId;
  beforeEach(function(done) {
    this.timeout(120000);
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
          getRecentHistory = consensusApi._worker._events.getRecentHistory;
          mergeBranches = consensusApi._worker._events.mergeBranches;
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
      getPeer: ['consensusPlugin', 'ledgerNode', (results, callback) => {
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
        })],
      getPeerBeta: ['consensusPlugin', 'nodeBeta', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNodeBeta.id, (err, result) => {
          peerBetaId = result.id;
          callback();
        });
      }],
      nodeGamma: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNodeGamma = result;
          callback(null, result);
        })],
      getPeerGamma: ['consensusPlugin', 'nodeGamma', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNodeGamma.id, (err, result) => {
          peerGammaId = result.id;
          callback();
        });
      }],
      nodeDelta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNodeDelta = result;
          callback(null, result);
        })],
      getPeerDelta: ['consensusPlugin', 'nodeDelta', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNodeDelta.id, (err, result) => {
          peerDeltaId = result.id;
          callback();
        });
      }],
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
          assertNoError(err);
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
    const eventTemplate = mockData.events.alpha;
    async.auto({
      addEvent: callback => helpers.addEventAndMerge(
        {consensusApi, ledgerNode, eventTemplate}, callback),
      gossipWith: ['addEvent', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            assertNoError(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // the events from ledgerNode should now be present on ledgerNodeBeta
        ledgerNodeBeta.storage.events.exists([
          Object.keys(results.addEvent.regular)[0],
          results.addEvent.merge.meta.eventHash
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
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode, mockData}, callback),
      history: ['addEvent', 'remoteEvents', (results, callback) =>
        getRecentHistory({ledgerNode}, callback)],
      mergeBranches: ['history', (results, callback) =>
        mergeBranches({history: results.history, ledgerNode}, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            assertNoError(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // the events from ledgerNode should now be present on ledgerNodeBeta
        ledgerNodeBeta.storage.events.exists([
          // results.remoteEvents.merge,
          // results.remoteEvents.regular,
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
      history: ['addEvent', (results, callback) =>
        getRecentHistory({ledgerNode: ledgerNodeBeta}, callback)],
      mergeBranches: ['history', (results, callback) => mergeBranches(
        {history: results.history, ledgerNode: ledgerNodeBeta}, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            assertNoError(err);
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
      history: ['addEvent', 'remoteEvents', (results, callback) =>
        getRecentHistory({ledgerNode: ledgerNodeBeta}, callback)],
      mergeBranches: ['history', (results, callback) => mergeBranches(
        {history: results.history, ledgerNode: ledgerNodeBeta}, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            assertNoError(err);
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
    const testNodes = [ledgerNode, ledgerNodeBeta];
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      addEventBeta: callback => ledgerNodeBeta.events.add(
        testEventBeta, callback),
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode: testNodes, mockData}, callback),
      history1: ['addEvent', 'remoteEvents', (results, callback) =>
        getRecentHistory({ledgerNode}, callback)],
      mergeBranches: ['history1', (results, callback) =>
        mergeBranches({history: results.history1, ledgerNode}, callback)],
      history2: ['addEventBeta', 'remoteEvents', (results, callback) =>
        getRecentHistory({ledgerNode: ledgerNodeBeta}, callback)],
      mergeBranchesBeta: ['history2', (results, callback) => mergeBranches(
        {history: results.history2, ledgerNode: ledgerNodeBeta}, callback)],
      gossipWith: ['mergeBranches', 'mergeBranchesBeta', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            assertNoError(err);
            callback();
          })],
      test: ['gossipWith', (results, callback) => {
        // ledgerNode and ledgerNode beta should have the same events
        async.eachSeries(testNodes, (node, callback) =>
          node.storage.events.exists([
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
          }), callback);
      }]
    }, done);
  });
  /*
    beta gossips with alpha, gamma gossips with alpha, beta gossips with gamma.
    Afterwards, all nodes have the same events.
  */
  it('properly gossips among three nodes', done => {
    const eventTemplate = mockData.events.alpha;
    const nodes = {
      alpha: ledgerNode,
      beta: ledgerNodeBeta,
      gamma: ledgerNodeGamma
    };
    async.auto({
      addEvent: callback => helpers.addEventMultiNode(
        {consensusApi, eventTemplate, nodes}, callback),
      gossipWith: ['addEvent', (results, callback) => async.series([
        // beta to alpha
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId}, err => {
            assertNoError(err);
            callback();
          }),
        // gamma to alpha
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeGamma, peerId}, err => {
            assertNoError(err);
            callback();
          }),
        // beta to gamma
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: ledgerNodeBeta, peerId: peerGammaId}, err => {
            assertNoError(err);
            callback();
          }),
      ], callback)],
      test: ['gossipWith', (results, callback) => {
        // all nodes should have the same events
        async.eachSeries(nodes, (node, callback) =>
          node.storage.events.exists([
            ...results.addEvent.mergeHash,
            ...results.addEvent.regularHash,
          ], (err, result) => {
            assertNoError(err);
            result.should.be.true;
            callback();
          }), callback);
      }]
    }, done);
  });
});
