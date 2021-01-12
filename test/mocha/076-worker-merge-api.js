/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const {callbackify} = require('util');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

describe('events.mergeBranches API', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });
  let genesisMergeHash;
  let Worker;
  const nodes = {};
  const peers = {};
  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    await helpers.flushCache();
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const consensusPlugin = await helpers.use('Continuity2017');
    consensusApi = consensusPlugin.api;
    Worker = consensusApi._worker.Worker;
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    const {genesisBlock: _genesisBlock} = await nodes.alpha.blocks.getGenesis();
    const genesisBlock = _genesisBlock.block;
    nodes.beta = await brLedgerNode.add(null, {genesisBlock});
    nodes.gamma = await brLedgerNode.add(null, {genesisBlock});
    nodes.delta = await brLedgerNode.add(null, {genesisBlock});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      // attach worker to the node to emulate a work session used by `helpers`
      ledgerNode.worker = new Worker({session: {ledgerNode}});
      await ledgerNode.worker.init();
      const {id: ledgerNodeId} = ledgerNode;
      const voter = await consensusApi._peers.get({ledgerNodeId});
      ledgerNode.peerId = voter.id;
      peers[key] = voter.id;
    }
    // NOTE: if nodeEpsilon is enabled, be sure to add to `creator` deps
    // nodeEpsilon: ['genesisBlock', (results, callback) => brLedgerNode.add(
    //   null, {genesisBlock: results.genesisBlock}, (err, result) => {
    //     if(err) {
    //       return callback(err);
    //     }
    //     nodes.epsilon = result;
    //     callback(null, result);
    //   })],
    genesisMergeHash = nodes.alpha.worker.head.eventHash;
  });

  it('collects one local event', async () => {
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    const ledgerNode = nodes.alpha;
    const addedEvents = await helpers.addEvent(
      {ledgerNode, eventTemplate, opTemplate});
    const result = await ledgerNode.worker.merge(
      {witnesses: [], nonEmptyThreshold: 0, emptyThreshold: 1});
    const eventHash = Object.keys(addedEvents)[0];
    should.exist(result);
    should.exist(result.record);
    const {record} = result;
    should.exist(record.event);
    const event = record.event;
    should.exist(event.type);
    event.type.should.equal('ContinuityMergeEvent');
    should.exist(event.treeHash);
    event.treeHash.should.equal(genesisMergeHash);
    should.exist(event.parentHash);
    const parentHash = event.parentHash;
    parentHash.should.be.an('array');
    parentHash.should.have.length(2);
    parentHash.should.have.same.members([eventHash, event.treeHash]);
    should.exist(record.meta);
    const meta = record.meta;
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
  });
  it('returns `null` record if no events since last merge', async () => {
    const eventTemplate = mockData.events.alpha;
    const ledgerNode = nodes.alpha;
    const opTemplate = mockData.operations.alpha;
    await helpers.addEvent({ledgerNode, eventTemplate, opTemplate});
    await ledgerNode.worker.merge(
      {witnesses: [], nonEmptyThreshold: 0, emptyThreshold: 1});
    const result = await ledgerNode.worker.merge(
      {witnesses: [], nonEmptyThreshold: 0, emptyThreshold: 1});
    should.exist(result);
    should.equal(result.merged, false);
    should.equal(result.record, null);
  });
  it('collects five local events', async () => {
    const eventTemplate = mockData.events.alpha;
    const ledgerNode = nodes.alpha;
    const opTemplate = mockData.operations.alpha;
    const addedEvents = await helpers.addEvent(
      {eventTemplate, count: 5, ledgerNode, opTemplate});
    const result = await ledgerNode.worker.merge(
      {witnesses: [], nonEmptyThreshold: 0, emptyThreshold: 1});
    should.exist(result.record);
    const {record} = result;
    should.exist(record.event);
    const event = record.event;
    event.treeHash.should.equal(genesisMergeHash);
    should.exist(event.parentHash);
    const parentHash = event.parentHash;
    parentHash.should.be.an('array');
    parentHash.should.have.length(6);
    const regularEventHash = Object.keys(addedEvents);
    parentHash.should.have.same.members(
      [event.treeHash, ...regularEventHash]);
  });
  it('collects one remote merge event', done => {
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      eventBeta: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      mergeBranches: ['eventBeta', (results, callback) => {
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
      betaEvent1: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      betaEvent2: ['betaEvent1', (results, callback) =>
        callbackify(helpers.addEventAndMerge)(
          {eventTemplate, ledgerNode: nodes.beta, opTemplate},
          callback)],
      mergeBranches: ['betaEvent2', (results, callback) => {
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
      alphaEvent: callback => callbackify(helpers.addEvent)(
        {eventTemplate, count: 8, ledgerNode: nodes.alpha, opTemplate},
        callback),
      betaEvent1: callback => callbackify(helpers.addEventAndMerge)(
        {ledgerNode: nodes.beta, eventTemplate, opTemplate},
        callback),
      betaEvent2: ['betaEvent1', (results, callback) =>
        callbackify(helpers.addEventAndMerge)(
          {ledgerNode: nodes.beta, eventTemplate, opTemplate},
          callback)],
      mergeBranches: ['alphaEvent', 'betaEvent2', (results, callback) => {
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
        callbackify(helpers.addEventAndMerge)(
          {eventTemplate, ledgerNode, opTemplate},
          (err, result) => {
            assertNoError(err);
            const mergeEvent = result.merge;
            mergeEvent.event.treeHash.should.equal(genesisMergeHash);
            callback(null, result);
          });
      },
      event2: ['event1', (results, callback) => {
        callbackify(helpers.addEventAndMerge)(
          {eventTemplate, ledgerNode, opTemplate},
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
      eventBeta: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'gamma'}, callback)],
      eventAlpha: ['eventGamma', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'gamma', nodes, to: 'alpha'}, (err, result) => {
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
      eventBeta: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'gamma'}, callback)],
      eventBeta2: ['eventGamma', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'gamma', nodes, to: 'beta'}, callback)],
      eventAlpha: ['eventBeta2', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'alpha'}, (err, result) => {
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
      eventBeta: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'gamma'}, callback)],
      // beta has only merge event from gamma
      eventBeta2: ['eventGamma', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'gamma', nodes, to: 'beta'}, callback)],
      // add new regular event to gamma
      eventGamma2: ['eventGamma', (results, callback) =>
        callbackify(helpers.addEventAndMerge)(
          {eventTemplate, ledgerNode: nodes.gamma, opTemplate},
          callback)],
      eventAlpha: ['eventBeta2', 'eventGamma2', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: ['beta', 'gamma'], nodes, to: 'alpha'},
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
      eventBeta: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode: nodes.beta, opTemplate},
        callback),
      eventGamma: ['eventBeta', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'beta', nodes, to: 'gamma'}, callback)],
      // beta has only merge event from gamma
      eventBeta2: ['eventGamma', (results, callback) =>
        callbackify(helpers.copyAndMerge)(
          {from: 'gamma', nodes, to: 'beta'}, callback)],
      // add new regular event to gamma
      eventGamma2: ['eventGamma', (results, callback) =>
        callbackify(helpers.addEventAndMerge)(
          {eventTemplate, ledgerNode: nodes.gamma, opTemplate},
          callback)],
      eventAlpha: callback => callbackify(helpers.addEvent)(
        {eventTemplate, count: 3, ledgerNode: nodes.alpha, opTemplate},
        callback),
      eventAlpha2: ['eventAlpha', 'eventBeta2', 'eventGamma2',
        (results, callback) => callbackify(helpers.copyAndMerge)(
          {from: ['beta', 'gamma'], nodes, to: 'alpha'},
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
