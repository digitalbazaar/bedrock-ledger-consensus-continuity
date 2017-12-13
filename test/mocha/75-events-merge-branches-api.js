/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const util = require('util');
const uuid = require('uuid/v4');

let consensusApi;

describe('events.mergeBranches API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let getRecentHistory;
  let mergeBranches;
  let genesisMergeHash;
  let eventHash;
  let testEventId;
  const nodes = {};
  const peers = {};
  beforeEach(done => {
    const configEvent = mockData.events.config;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
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
          mergeBranches = consensusApi._worker._events.mergeBranchesNEW;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {configEvent}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.alpha = result;
          callback(null, result);
        })],
      genesisMerge: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          eventsCollection: nodes.alpha.storage.events.collection
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMergeHash = result;
          callback();
        });
      }],
      genesisBlock: ['ledgerNode', (results, callback) =>
        nodes.alpha.blocks.getGenesis((err, result) => {
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
          nodes.beta = result;
          callback(null, result);
        })],
      nodeGamma: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.gamma = result;
          callback(null, result);
        })],
      nodeDelta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.delta = result;
          callback(null, result);
        })],
      // NOTE: if nodeEpsilon is enabled, be sure to add to `creator` deps
      // nodeEpsilon: ['genesisBlock', (results, callback) => brLedgerNode.add(
      //   null, {genesisBlock: results.genesisBlock}, (err, result) => {
      //     if(err) {
      //       return callback(err);
      //     }
      //     nodes.epsilon = result;
      //     callback(null, result);
      //   })],
      creator: ['nodeBeta', 'nodeGamma', 'nodeDelta', (results, callback) =>
        async.eachOf(nodes, (n, i, callback) =>
          consensusApi._worker._voters.get(n.id, (err, result) => {
            if(err) {
              return callback(err);
            }
            peers[i] = result.id;
            callback();
          }), callback)]
    }, done);
  });

  it.only('collects one local event', done => {
    const eventTemplate = mockData.events.alpha;
    const ledgerNode = nodes.alpha;
    async.auto({
      addEvent: callback => helpers.addEvent(
        {ledgerNode, eventTemplate}, callback),
      history: ['addEvent', (results, callback) =>
        getRecentHistory({ledgerNode}, callback)],
      mergeBranches: ['history', (results, callback) => {
        mergeBranches({ledgerNode, history: results.history}, (err, result) => {
          assertNoError(err);
          const eventHash = Object.keys(results.addEvent)[0];
          should.exist(result.event);
          const event = result.event;
          should.exist(event.type);
          event.type.should.be.an('array');
          event.type.should.have.length(2);
          event.type.should.have.same.members(
            ['WebLedgerEvent', 'ContinuityMergeEvent']);
          should.exist(event.treeHash);
          event.treeHash.should.equal(genesisMergeHash);
          should.exist(event.parentHash);
          const parentHash = event.parentHash;
          parentHash.should.be.an('array');
          parentHash.should.have.length(2);
          parentHash.should.have.same.members([eventHash, event.treeHash]);
          should.exist(result.meta);
          const meta = result.meta;
          should.exist(meta.continuity2017);
          should.exist(meta.continuity2017.creator);
          const eventCreator = meta.continuity2017.creator;
          eventCreator.should.be.a('string');
          eventCreator.should.equal(peers.alpha);
          should.exist(meta.eventHash);
          meta.eventHash.should.be.a('string');
          should.exist(meta.created);
          meta.created.should.be.a('number');
          should.exist(meta.updated);
          meta.updated.should.be.a('number');
          callback();
        });
      }]
    }, done);
  });
  it('returns NotFoundError if no events since last merge', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEvent.input[0].id = `https://example.com/event/${uuid()}`;
    const ledgerNode = nodes.alpha;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      mergeBranches1: ['addEvent', (results, callback) =>
        mergeBranches({ledgerNode}, callback)],
      mergeBranches2: ['mergeBranches1', (results, callback) => {
        mergeBranches({ledgerNode}, (err, result) => {
          should.exist(err);
          should.not.exist(result);
          err.name.should.equal('NotFoundError');
          callback();
        });
      }]
    }, done);
  });
  it('collects five local events', done => {
    const eventTemplate = mockData.events.alpha;
    const ledgerNode = nodes.alpha;
    async.auto({
      addEvent: callback => helpers.addEvent(
        {eventTemplate, count: 5, ledgerNode}, callback),
      mergeBranches: ['addEvent', (results, callback) => {
        mergeBranches({ledgerNode}, (err, result) => {
          assertNoError(err);
          should.exist(result.event);
          const event = result.event;
          event.treeHash.should.equal(genesisMergeHash);
          should.exist(event.parentHash);
          const parentHash = event.parentHash;
          parentHash.should.be.an('array');
          parentHash.should.have.length(6);
          const regularEventHash = Object.keys(results.addEvent);
          parentHash.should.have.same.members(
            [event.treeHash, ...regularEventHash]);
          callback();
        });
      }]
    }, done);
  });
  it('collects one remote merge event', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, ledgerNode: nodes.beta, eventTemplate}, callback),
      mergeBranches: ['eventBeta', (results, callback) => {
        helpers.copyAndMerge(
          {consensusApi, from: nodes.beta, to: nodes.alpha}, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            should.exist(event.treeHash);
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(2);
            const betaMergeHash = results.eventBeta.merge.meta.eventHash;
            parentHash.should.have.same.members(
              [betaMergeHash, event.treeHash]);
            callback();
          });
      }]
    }, done);
  });
  // two chained merge events from beta are copied to alpha and merged, only
  // the second beta merge event hash should be included in parentHash
  it('collects one remote merge events', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      betaEvent1: callback => helpers.addEventAndMerge(
        {consensusApi, ledgerNode: nodes.beta, eventTemplate}, callback),
      betaEvent2: ['betaEvent1', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, ledgerNode: nodes.beta, eventTemplate}, callback)],
      mergeBranches: ['betaEvent2', (results, callback) => {
        helpers.copyAndMerge(
          {consensusApi, from: nodes.beta, to: nodes.alpha}, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            const secondBetaMergeHash = results.betaEvent2.merge.meta.eventHash;
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(2);
            parentHash.should.have.same.members(
              [event.treeHash, secondBetaMergeHash]);
            callback();
          });
      }]
    }, done);
  });
  it('collects one remote merge events and eight local events', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      alphaEvent: callback => helpers.addEvent(
        {eventTemplate, count: 8, ledgerNode: nodes.alpha}, callback),
      betaEvent1: callback => helpers.addEventAndMerge(
        {consensusApi, ledgerNode: nodes.beta, eventTemplate}, callback),
      betaEvent2: ['betaEvent1', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, ledgerNode: nodes.beta, eventTemplate}, callback)],
      mergeBranches: ['alphaEvent', 'betaEvent2', (results, callback) => {
        helpers.copyAndMerge(
          {consensusApi, from: nodes.beta, to: nodes.alpha}, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            const secondBetaMergeHash = results.betaEvent2.merge.meta.eventHash;
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(10);
            const alphaHashes = Object.keys(results.alphaEvent);
            parentHash.should.have.same.members(
              [event.treeHash, secondBetaMergeHash, ...alphaHashes]);
            callback();
          });
      }]
    }, done);
  });
  it('Second merge event has the proper treeHash', done => {
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      event1: callback => {
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode}, (err, result) => {
            assertNoError(err);
            const mergeEvent = result.merge;
            mergeEvent.event.treeHash.should.equal(genesisMergeHash);
            callback(null, result);
          });
      },
      event2: ['event1', (results, callback) => {
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode}, (err, result) => {
            assertNoError(err);
            const mergeEvent = result.merge;
            mergeEvent.event.treeHash.should.equal(
              results.event1.merge.meta.eventHash);
            callback();
          });
      }]
    }, done);
  });
  // beta -> gamma -> alpha
  it('alpha properly merges events from beta and gamma', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta}, callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.beta, to: nodes.gamma}, callback)],
      eventAlpha: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.gamma, to: nodes.alpha}, (err, result) => {
          assertNoError(err);
          const gammaMergeHash = results.eventGamma.meta.eventHash;
          const parentHash = result.event.parentHash;
          parentHash.should.have.length(2);
          parentHash.should.have.same.members(
            [genesisMergeHash, gammaMergeHash]);
          callback();
        }
      )]
    }, done);
  });
  it('alpha properly merges events from beta and gamma II', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta}, callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.beta, to: nodes.gamma}, callback)],
      eventBeta2: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.gamma, to: nodes.beta}, callback)],
      eventAlpha: ['eventBeta2', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.beta, to: nodes.alpha}, (err, result) => {
          assertNoError(err);
          const betaMergeHash2 = results.eventBeta2.meta.eventHash;
          const parentHash = result.event.parentHash;
          parentHash.should.have.length(2);
          parentHash.should.have.same.members(
            [genesisMergeHash, betaMergeHash2]);
          callback();
        }
      )]
    }, done);
  });
  it('alpha properly merges events from beta and gamma III', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta}, callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.beta, to: nodes.gamma}, callback)],
      // beta has only merge event from gamma
      eventBeta2: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.gamma, to: nodes.beta}, callback)],
      // add new regular event to gamma
      eventGamma2: ['eventGamma', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode: nodes.gamma}, callback)],
      eventAlpha: ['eventBeta2', 'eventGamma2', (results, callback) =>
        helpers.copyAndMerge(
          {consensusApi, from: [nodes.beta, nodes.gamma], to: nodes.alpha},
          (err, result) => {
            assertNoError(err);
            const betaMergeHash2 = results.eventBeta2.meta.eventHash;
            const gammaMergeHash2 = results.eventGamma2.merge.meta.eventHash;
            const parentHash = result.event.parentHash;
            parentHash.should.have.length(3);
            parentHash.should.have.same.members(
              [genesisMergeHash, betaMergeHash2, gammaMergeHash2]);
            callback();
          }
        )]
    }, done);
  });
  // same as III and adds a regular event to alpha before final merge there
  it('alpha properly merges events from beta and gamma IV', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta}, callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.beta, to: nodes.gamma}, callback)],
      // beta has only merge event from gamma
      eventBeta2: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: nodes.gamma, to: nodes.beta}, callback)],
      // add new regular event to gamma
      eventGamma2: ['eventGamma', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode: nodes.gamma}, callback)],
      eventAlpha: callback => helpers.addEvent(
        {eventTemplate, count: 3, ledgerNode: nodes.alpha}, callback),
      eventAlpha2: ['eventAlpha', 'eventBeta2', 'eventGamma2',
        (results, callback) => helpers.copyAndMerge(
          {consensusApi, from: [nodes.beta, nodes.gamma], to: nodes.alpha},
          (err, result) => {
            assertNoError(err);
            const alphaHashes = Object.keys(results.eventAlpha);
            const betaMergeHash2 = results.eventBeta2.meta.eventHash;
            const gammaMergeHash2 = results.eventGamma2.merge.meta.eventHash;
            const parentHash = result.event.parentHash;
            parentHash.should.have.length(6);
            parentHash.should.have.same.members(
              [genesisMergeHash, betaMergeHash2, gammaMergeHash2,
                ...alphaHashes]);
            callback();
          }
        )]
    }, done);
  });
});
