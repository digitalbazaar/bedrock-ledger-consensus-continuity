/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
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
describe('Election API findConsensus', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let genesisBlock;
  let EventWriter;
  const nodes = {};
  const peers = {};
  beforeEach(function(done) {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      flush: callbackify(helpers.flushCache),
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        helpers.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
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
        consensusApi._voters.get({ledgerNodeId}, (err, result) => {
          callback(null, result.id);
        });
      }],
      genesisBlock: ['ledgerNode', (results, callback) =>
        nodes.alpha.blocks.getGenesis((err, result) => {
          if(err) {
            return callback(err);
          }
          genesisBlock = result.genesisBlock.block;
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
          ledgerNode.eventWriter = new EventWriter({ledgerNode});
          consensusApi._voters.get({ledgerNodeId}, (err, result) => {
            if(err) {
              return callback(err);
            }
            peers[i] = result.id;
            ledgerNode.creatorId = result.id;
            helpers.peersReverse[result.id] = i;
            callback();
          });
        }, callback)]
    }, done);
  });
  it('single node consensus', done => {
    // the genesisMerge already has consensus
    const findConsensus = consensusApi._election.findConsensus;
    const getRecentHistory = consensusApi._events.getRecentHistory;
    const ledgerNode = nodes.alpha;
    const {creatorId} = ledgerNode;
    const electors = [{id: peers.alpha}];
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      history: ['event1', (results, callback) => getRecentHistory(
        {creatorId, ledgerNode}, callback)],
      consensus: ['history', (results, callback) => {
        findConsensus({
          electors, ledgerNode, history: results.history,
          mode: 'first'
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
  it('properly does not reach consensus with four electors', done => {
    const findConsensus = consensusApi._election.findConsensus;
    const getRecentHistory = consensusApi._events.getRecentHistory;
    const ledgerNode = nodes.alpha;
    const {creatorId} = ledgerNode;
    const electors = [
      {id: peers.alpha}, {id: peers.beta},
      {id: peers.gamma}, {id: peers.delta}];
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      history: ['event1', (results, callback) => getRecentHistory(
        {creatorId, ledgerNode}, callback)],
      consensus: ['history', (results, callback) => {
        findConsensus({
          electors, ledgerNode, history: results.history,
          mode: 'first'
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
  // mode instead of `firstWithConsensusProof`
  it.skip('ledger history alpha', function(done) {
    this.timeout(120000);
    const findConsensus = consensusApi._election.findConsensus;
    const getRecentHistory = consensusApi._events.getRecentHistory;
    const electors = _.values(peers).map(p => ({id: p}));
    async.auto({
      build: callback => helpers.buildHistory(
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
              electors, ledgerNode, history: results.history,
              mode: 'firstWithConsensusProof'
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
              // proofReport({
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
    const findConsensus = consensusApi._election.findConsensus;
    const getRecentHistory = consensusApi._events.getRecentHistory;
    const electors = _.values(peers).map(p => ({id: p}));
    async.auto({
      build: callback => helpers.buildHistory(
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
              electors, ledgerNode, history: results.history,
              mode: 'firstWithConsensusProof'
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
              // proofReport({
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
    const findConsensus = consensusApi._election.findConsensus;
    const getRecentHistory = consensusApi._events.getRecentHistory;
    const electors = _.values(peers).map(p => ({id: p}));
    async.auto({
      build: callback => helpers.buildHistory(
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
              electors, ledgerNode, history: results.history,
              mode: 'firstWithConsensusProof'
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
              // proofReport({
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
    const findConsensus = consensusApi._election.findConsensus;
    const getRecentHistory = consensusApi._events.getRecentHistory;
    // all peers except epsilon are electors
    const electors = _.values(peers)
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
          consensusApi._voters.get({ledgerNodeId}, (err, result) => {
            if(err) {
              return callback(err);
            }
            ledgerNode.creatorId = result.id;
            callback();
          });
        }]
      }, callback),
      build: ['nodeEpsilon', (results, callback) => helpers.buildHistory(
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
              electors, ledgerNode, history: results.history,
              mode: 'firstWithConsensusProof'
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
              // proofReport({
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
    const findConsensus = consensusApi._election.findConsensus;
    const getRecentHistory = consensusApi._events.getRecentHistory;
    const ledgerNode = nodes.alpha;
    const electors = _.values(peers).map(p => ({id: p}));
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      history: ['build', (results, callback) => {
        const {creatorId} = ledgerNode;
        getRecentHistory({creatorId, ledgerNode}, callback);
      }],
      event: ['history', (results, callback) => helpers.addEvent(
        {ledgerNode, eventTemplate, opTemplate}, callback)],
      consensus: ['event', (results, callback) => {
        findConsensus({
          electors, ledgerNode, history: results.history,
          mode: 'firstWithConsensusProof'
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
          // proofReport({
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

function proofReport(
  {copyMergeHashes, copyMergeHashesIndex, consensusProofHash}) {
  console.log(
    'REPORTED PROOF',
    consensusProofHash.map(c => copyMergeHashesIndex[c]));
}
