/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const {callbackify} = require('util');

const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

/* eslint-disable no-unused-vars */
describe('Consensus API find', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });
  let genesisBlock;
  let EventWriter;
  const nodes = {};
  const peers = {};
  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    await helpers.flushCache();
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const plugin = await helpers.use('Continuity2017');
    consensusApi = plugin.api;
    EventWriter = consensusApi._worker.EventWriter;
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    const {id: ledgerNodeId} = nodes.alpha;
    const _voter = await consensusApi._peers.get({ledgerNodeId});
    const {genesisBlock: _genesisBlock} = await nodes.alpha.blocks.getGenesis();
    genesisBlock = _genesisBlock.block;
    nodes.beta = await brLedgerNode.add(null, {genesisBlock});
    nodes.gamma = await brLedgerNode.add(null, {genesisBlock});
    nodes.delta = await brLedgerNode.add(null, {genesisBlock});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      ledgerNode.eventWriter = new EventWriter({ledgerNode});
      const {id: ledgerNodeId} = ledgerNode;
      const voter = await consensusApi._peers.get({ledgerNodeId});
      peers[key] = voter.id;
      ledgerNode.creatorId = voter.id;
      helpers.peersReverse[voter.id] = key;
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
  });
  it('single node consensus', done => {
    // the genesisMerge already has consensus
    const findConsensus = callbackify(consensusApi._consensus.find);
    const getRecentHistory = callbackify(consensusApi._events.getRecentHistory);
    const ledgerNode = nodes.alpha;
    const {creatorId} = ledgerNode;
    const witnesses = [{id: peers.alpha}];
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      history: ['event1', (results, callback) => getRecentHistory(
        {creatorId, ledgerNode}, callback)],
      consensus: ['history', (results, callback) => {
        findConsensus({
          witnesses, ledgerNode, history: results.history
        }, (err, result) => {
          assertNoError(err);
          result.consensusProofHash.should.have.length(1);
          result.consensusProofHash[0].should.equal(
            results.event1.mergeHash);
          result.eventHash.should.have.length(2);
          result.eventHash.should.have.same.members([
            Object.keys(results.event1.regular)[0],
            results.event1.merge.meta.eventHash
          ]);
          callback();
        });
      }]
    }, done);
  });
  it('properly does not reach consensus with four witnesses', done => {
    const findConsensus = callbackify(consensusApi._consensus.find);
    const getRecentHistory = callbackify(consensusApi._events.getRecentHistory);
    const ledgerNode = nodes.alpha;
    const {creatorId} = ledgerNode;
    const witnesses = [
      {id: peers.alpha}, {id: peers.beta},
      {id: peers.gamma}, {id: peers.delta}];
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      history: ['event1', (results, callback) => getRecentHistory(
        {creatorId, ledgerNode}, callback)],
      consensus: ['history', (results, callback) => {
        findConsensus({
          witnesses, ledgerNode, history: results.history
        }, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.consensus.should.equal(false);
          callback();
        });
      }]
    }, done);
  });
  // disabled from here down until these can be updated to work with `first`
  // mode instead of old `firstWithConsensusProof`
  it.skip('ledger history alpha', function(done) {
    this.timeout(120000);
    const findConsensus = callbackify(consensusApi._consensus.find);
    const getRecentHistory = callbackify(consensusApi._events.getRecentHistory);
    const witnesses = _.values(peers).map(p => ({id: p}));
    async.auto({
      build: callback => callbackify(helpers.buildHistory)(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        const {copyMergeHashes, copyMergeHashesIndex, regularEvent} =
          results.build;
        async.each(nodes, (ledgerNode, callback) => async.auto({
          history: callback => {
            const {creatorId} = ledgerNode;
            getRecentHistory({creatorId, ledgerNode}, callback);
          },
          consensus: ['history', (results, callback) => {
            findConsensus({
              witnesses, ledgerNode, history: results.history
            }, (err, result) => {
              assertNoError(err);
              should.exist(result);
              should.exist(result.eventHash);
              result.eventHash.should.be.an('array');
              result.eventHash.should.have.length(8);
              result.eventHash.should.have.same.members([
                ...regularEvent.regularHash,
                ...regularEvent.mergeHash
              ]);
              should.exist(result.consensusProofHash);
              result.consensusProofHash.should.be.an('array');
              result.consensusProofHash.should.have.length(10);
              result.consensusProofHash.should.have.same.members([
                copyMergeHashes.cpa,
                copyMergeHashes.cpb,
                copyMergeHashes.cp1,
                copyMergeHashes.cp2,
                copyMergeHashes.cp3,
                copyMergeHashes.cp4,
                copyMergeHashes.cp5,
                copyMergeHashes.cp6,
                copyMergeHashes.cp7,
                copyMergeHashes.cp8
              ]);
              // createReport({
              //   copyMergeHashes,
              //   copyMergeHashesIndex,
              //   consensusProofHash: result.consensusProofHash,
              // });
              callback();
            });
          }]
        }, callback), callback);
      }]
    }, done);
  });
  it.skip('ledger history beta', function(done) {
    this.timeout(120000);
    const findConsensus = callbackify(consensusApi._consensus.find);
    const getRecentHistory = callbackify(consensusApi._events.getRecentHistory);
    const witnesses = _.values(peers).map(p => ({id: p}));
    async.auto({
      build: callback => callbackify(helpers.buildHistory)(
        {consensusApi, historyId: 'beta', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        const {copyMergeHashes, copyMergeHashesIndex, regularEvent} =
          results.build;
        async.each(nodes, (ledgerNode, callback) => async.auto({
          history: callback => {
            const {creatorId} = ledgerNode;
            getRecentHistory({creatorId, ledgerNode}, callback);
          },
          consensus: ['history', (results, callback) => {
            findConsensus({
              witnesses, ledgerNode, history: results.history
            }, (err, result) => {
              assertNoError(err);
              result.eventHash.should.have.length(8);
              result.eventHash.should.have.same.members([
                ...regularEvent.regularHash,
                ...regularEvent.mergeHash
              ]);
              result.consensusProofHash.should.have.length(10);
              result.consensusProofHash.should.have.same.members([
                copyMergeHashes.cpa,
                copyMergeHashes.cpb,
                copyMergeHashes.cp1,
                copyMergeHashes.cp2,
                copyMergeHashes.cp3,
                copyMergeHashes.cp4,
                copyMergeHashes.cp5,
                copyMergeHashes.cp6,
                copyMergeHashes.cp7,
                copyMergeHashes.cp8
              ]);
              // createReport({
              //   copyMergeHashes,
              //   copyMergeHashesIndex,
              //   consensusProofHash: result.consensusProofHash,
              // });
              callback();
            });
          }]
        }, callback), callback);
      }]
    }, done);
  });
  it.skip('ledger history gamma', function(done) {
    this.timeout(120000);
    const findConsensus = callbackify(consensusApi._consensus.find);
    const getRecentHistory = callbackify(consensusApi._events.getRecentHistory);
    const witnesses = _.values(peers).map(p => ({id: p}));
    async.auto({
      build: callback => callbackify(helpers.buildHistory)(
        {consensusApi, historyId: 'gamma', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        const {copyMergeHashes, copyMergeHashesIndex, regularEvent} =
          results.build;
        async.each(nodes, (ledgerNode, callback) => async.auto({
          history: callback => {
            const {creatorId} = ledgerNode;
            getRecentHistory({creatorId, ledgerNode}, callback);
          },
          consensus: ['history', (results, callback) => {
            findConsensus({
              witnesses, ledgerNode, history: results.history
            }, (err, result) => {
              assertNoError(err);
              result.eventHash.should.have.length(8);
              result.eventHash.should.have.same.members([
                ...regularEvent.regularHash,
                ...regularEvent.mergeHash
              ]);
              result.consensusProofHash.should.have.length(10);
              result.consensusProofHash.should.have.same.members([
                copyMergeHashes.cpa,
                copyMergeHashes.cpb,
                copyMergeHashes.cp1,
                copyMergeHashes.cp2,
                copyMergeHashes.cp3,
                copyMergeHashes.cp4,
                copyMergeHashes.cp5,
                copyMergeHashes.cp6,
                copyMergeHashes.cp7,
                copyMergeHashes.cp8
              ]);
              // createReport({
              //   copyMergeHashes,
              //   copyMergeHashesIndex,
              //   consensusProofHash: result.consensusProofHash,
              // });
              callback();
            });
          }]
        }, callback), callback);
      }]
    }, done);
  });
  it.skip('ledger history delta', function(done) {
    this.timeout(120000);
    const findConsensus = callbackify(consensusApi._consensus.find);
    const getRecentHistory = callbackify(consensusApi._events.getRecentHistory);
    // all peers except epsilon are witnesses
    const witnesses = _.values(peers)
      .filter(p => p !== peers.epsilon)
      .map(p => ({id: p}));
    async.auto({
      // add node epsilon for this test and remove it afterwards
      nodeEpsilon: callback => async.auto({
        ledgerNode: callback => brLedgerNode.add(
          null, {genesisBlock}, (err, result) => {
            if(err) {
              return callback(err);
            }
            nodes.epsilon = result;
            callback();
          }),
        creator: ['ledgerNode', (results, callback) => {
          const ledgerNode = nodes.epsilon;
          const {id: ledgerNodeId} = ledgerNode;
          // attach eventWriter to the node
          ledgerNode.eventWriter = new EventWriter({ledgerNode});
          consensusApi._peers.get({ledgerNodeId}, (err, result) => {
            if(err) {
              return callback(err);
            }
            ledgerNode.creatorId = result.id;
            callback();
          });
        }]
      }, callback),
      build: ['nodeEpsilon', (results, callback) =>
        callbackify(helpers.buildHistory)(
          {consensusApi, historyId: 'delta', mockData, nodes}, callback)],
      testAll: ['build', (results, callback) => {
        const {copyMergeHashes, copyMergeHashesIndex, regularEvent} =
          results.build;
        async.each(nodes, (ledgerNode, callback) => async.auto({
          history: callback => {
            const {creatorId} = ledgerNode;
            getRecentHistory({creatorId, ledgerNode}, callback);
          },
          consensus: ['history', (results, callback) => {
            findConsensus({
              witnesses, ledgerNode, history: results.history
            }, (err, result) => {
              assertNoError(err);
              should.exist(result);
              should.exist(result.eventHash);
              result.eventHash.should.be.an('array');
              result.eventHash.should.have.length(8);
              result.eventHash.should.have.same.members([
                regularEvent.alpha.regularHashes[0],
                regularEvent.beta.regularHashes[0],
                regularEvent.gamma.regularHashes[0],
                regularEvent.delta.regularHashes[0],
                regularEvent.alpha.mergeHash,
                regularEvent.beta.mergeHash,
                regularEvent.gamma.mergeHash,
                regularEvent.delta.mergeHash
                // exclude epsilon (non-elector)
              ]);
              result.consensusProofHash.should.have.length(10);
              result.consensusProofHash.should.have.same.members([
                copyMergeHashes.cpa,
                copyMergeHashes.cpb,
                copyMergeHashes.cp1,
                copyMergeHashes.cp2,
                copyMergeHashes.cp3,
                copyMergeHashes.cp4,
                copyMergeHashes.cp5,
                copyMergeHashes.cp6,
                copyMergeHashes.cp7,
                copyMergeHashes.cp8
              ]);
              // createReport({
              //   copyMergeHashes,
              //   copyMergeHashesIndex,
              //   consensusProofHash: result.consensusProofHash,
              // });
              callback();
            });
          }]
        }, callback), callback);
      }],
      cleanup: ['testAll', (results, callback) => {
        delete nodes.epsilon;
        callback();
      }]
    }, done);
  });
  it.skip('add regular event with no merge before findConsensus', done => {
    const findConsensus = callbackify(consensusApi._consensus.find);
    const getRecentHistory = callbackify(consensusApi._events.getRecentHistory);
    const ledgerNode = nodes.alpha;
    const witnesses = _.values(peers).map(p => ({id: p}));
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      build: callback => callbackify(helpers.buildHistory)(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      history: ['build', (results, callback) => {
        const {creatorId} = ledgerNode;
        getRecentHistory({creatorId, ledgerNode}, callback);
      }],
      event: ['history', (results, callback) => callbackify(helpers.addEvent)(
        {ledgerNode, eventTemplate, opTemplate}, callback)],
      consensus: ['event', (results, callback) => {
        findConsensus({
          witnesses, ledgerNode, history: results.history
        }, (err, result) => {
          const {copyMergeHashes, copyMergeHashesIndex, regularEvent} =
            results.build;
          assertNoError(err);
          result.eventHash.should.have.length(8);
          result.eventHash.should.have.same.members([
            ...regularEvent.regularHash,
            ...regularEvent.mergeHash
          ]);
          result.consensusProofHash.should.have.length(10);
          result.consensusProofHash.should.have.same.members([
            copyMergeHashes.cpa,
            copyMergeHashes.cpb,
            copyMergeHashes.cp1,
            copyMergeHashes.cp2,
            copyMergeHashes.cp3,
            copyMergeHashes.cp4,
            copyMergeHashes.cp5,
            copyMergeHashes.cp6,
            copyMergeHashes.cp7,
            copyMergeHashes.cp8
          ]);
          // createReport({
          //   copyMergeHashes,
          //   copyMergeHashesIndex,
          //   consensusProofHash: result.consensusProofHash,
          // });
          callback();
        });
      }]
    }, done);
  });
});

function createReport(
  {copyMergeHashes, copyMergeHashesIndex, consensusProofHash}) {
  console.log(
    'CONSENSUS REPORT',
    consensusProofHash.map(c => copyMergeHashesIndex[c]));
}
