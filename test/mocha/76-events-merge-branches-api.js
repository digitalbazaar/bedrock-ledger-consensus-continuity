/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
// const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
// const util = require('util');
// const uuid = require('uuid/v4');

let consensusApi;

describe('events.mergeBranches API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let merge;
  let genesisMergeHash;
  let EventWriter;
  const nodes = {};
  const peers = {};
  beforeEach(function(done) {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      flush: helpers.flushCache,
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedgerNode.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          merge = consensusApi._worker._events.merge;
          EventWriter = consensusApi._worker.EventWriter;
          callback();
        }),
      ledgerNode: ['clean', 'flush', (results, callback) => brLedgerNode.add(
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.alpha = result;
          callback(null, result);
        })],
      creatorId: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        const {id: ledgerNodeId} = nodes.alpha;
        consensusApi._worker._voters.get({ledgerNodeId}, (err, result) => {
          callback(null, result.id);
        });
      }],
      genesisMerge: ['creatorId', (results, callback) => {
        const ledgerNode = nodes.alpha;
        const {creatorId} = results;
        consensusApi._worker._events._getLocalBranchHead(
          {creatorId, ledgerNode}, (err, result) => {
            if(err) {
              return callback(err);
            }
            genesisMergeHash = result.eventHash;
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
        async.eachOf(nodes, (ledgerNode, i, callback) => {
          const {id: ledgerNodeId} = ledgerNode;
          // attach eventWriter to the node
          ledgerNode.eventWriter = new EventWriter(
            {immediate: true, ledgerNode});
          consensusApi._worker._voters.get({ledgerNodeId}, (err, result) => {
            if(err) {
              return callback(err);
            }
            ledgerNode.creatorId = result.id;
            peers[i] = result.id;
            callback();
          });
        }, callback)]
    }, done);
  });

  it('collects one local event', done => {
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    const ledgerNode = nodes.alpha;
    const {creatorId} = ledgerNode;
    async.auto({
      addEvent: callback => helpers.addEvent(
        {ledgerNode, eventTemplate, opTemplate}, callback),
      mergeBranches: ['addEvent', (results, callback) => {
        merge({creatorId, ledgerNode}, (err, result) => {
          assertNoError(err);
          const eventHash = Object.keys(results.addEvent)[0];
          should.exist(result.event);
          const event = result.event;
          should.exist(event.type);
          event.type.should.equal('ContinuityMergeEvent');
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
  it('returns `null` if no events since last merge', done => {
    const eventTemplate = mockData.events.alpha;
    const ledgerNode = nodes.alpha;
    const opTemplate = mockData.operations.alpha;
    const {creatorId} = ledgerNode;
    async.auto({
      addEvent: callback => helpers.addEvent(
        {ledgerNode, eventTemplate, opTemplate}, callback),
      mergeBranches1: ['addEvent', (results, callback) =>
        merge({creatorId, ledgerNode}, callback)],
      mergeBranches2: ['mergeBranches1', (results, callback) => merge(
        {creatorId, ledgerNode}, (err, result) => {
          assertNoError();
          should.equal(result, undefined);
          callback();
        })]
    }, done);
  });
  it('collects five local events', done => {
    const eventTemplate = mockData.events.alpha;
    const ledgerNode = nodes.alpha;
    const opTemplate = mockData.operations.alpha;
    const {creatorId} = ledgerNode;
    async.auto({
      addEvent: callback => helpers.addEvent(
        {eventTemplate, count: 5, ledgerNode, opTemplate}, callback),
      mergeBranches: ['addEvent', (results, callback) => {
        merge({creatorId, ledgerNode}, (err, result) => {
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      mergeBranches: ['eventBeta', (results, callback) => {
        helpers.copyAndMerge(
          {consensusApi, from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      betaEvent1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      betaEvent2: ['betaEvent1', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode: nodes.beta, opTemplate},
          callback)],
      mergeBranches: ['betaEvent2', (results, callback) => {
        helpers.copyAndMerge(
          {consensusApi, from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      alphaEvent: callback => helpers.addEvent(
        {eventTemplate, count: 8, ledgerNode: nodes.alpha, opTemplate},
        callback),
      betaEvent1: callback => helpers.addEventAndMerge(
        {consensusApi, ledgerNode: nodes.beta, eventTemplate, opTemplate},
        callback),
      betaEvent2: ['betaEvent1', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, ledgerNode: nodes.beta, eventTemplate, opTemplate},
          callback)],
      mergeBranches: ['alphaEvent', 'betaEvent2', (results, callback) => {
        helpers.copyAndMerge(
          {consensusApi, from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => {
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode, opTemplate},
          (err, result) => {
            assertNoError(err);
            const mergeEvent = result.merge;
            mergeEvent.event.treeHash.should.equal(genesisMergeHash);
            callback(null, result);
          });
      },
      event2: ['event1', (results, callback) => {
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode, opTemplate},
          (err, result) => {
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'beta', nodes, to: 'gamma'}, callback)],
      eventAlpha: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'gamma', nodes, to: 'alpha'}, (err, result) => {
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'beta', nodes, to: 'gamma'}, callback)],
      eventBeta2: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'gamma', nodes, to: 'beta'}, callback)],
      eventAlpha: ['eventBeta2', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'beta', nodes, to: 'gamma'}, callback)],
      // beta has only merge event from gamma
      eventBeta2: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'gamma', nodes, to: 'beta'}, callback)],
      // add new regular event to gamma
      eventGamma2: ['eventGamma', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode: nodes.gamma, opTemplate},
          callback)],
      eventAlpha: ['eventBeta2', 'eventGamma2', (results, callback) =>
        helpers.copyAndMerge(
          {consensusApi, from: ['beta', 'gamma'], nodes, to: 'alpha'},
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      eventBeta: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'beta', nodes, to: 'gamma'}, callback)],
      // beta has only merge event from gamma
      eventBeta2: ['eventGamma', (results, callback) => helpers.copyAndMerge(
        {consensusApi, from: 'gamma', nodes, to: 'beta'}, callback)],
      // add new regular event to gamma
      eventGamma2: ['eventGamma', (results, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode: nodes.gamma, opTemplate},
          callback)],
      eventAlpha: callback => helpers.addEvent(
        {eventTemplate, count: 3, ledgerNode: nodes.alpha, opTemplate},
        callback),
      eventAlpha2: ['eventAlpha', 'eventBeta2', 'eventGamma2',
        (results, callback) => helpers.copyAndMerge(
          {consensusApi, from: ['beta', 'gamma'], nodes, to: 'alpha'},
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
