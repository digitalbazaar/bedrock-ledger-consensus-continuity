/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const uuid = require('uuid/v4');
const util = require('util');
const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

describe.only('Election API _findMergeEventProof', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let _findMergeEventProof;
  let _getElectorBranches;
  let genesisBlock;
  let genesisMerge;
  let getRecentHistory;
  let eventHash;
  let testEventId;
  const nodes = {};
  const peers = {};
  beforeEach(function(done) {
    this.timeout(120000);
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
          _getElectorBranches =
            consensusApi._worker._election._getElectorBranches;
          _findMergeEventProof =
            consensusApi._worker._election._findMergeEventProof;
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
          genesisMerge = result;
          callback();
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
        async.eachOf(nodes, (n, i, callback) =>
          consensusApi._worker._voters.get(n.id, (err, result) => {
            if(err) {
              return callback(err);
            }
            peers[i] = result.id;
            helpers.peersReverse[result.id] = i;
            callback();
          }), callback)]
    }, done);
  });
  it('ledger history alpha', done => {
    const report = {};
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory(
            {ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: branches,
              electors
            });
            // try {
            //   report[i] = proofReport({
            //     proof,
            //     copyMergeHashes: build.copyMergeHashes,
            //     copyMergeHashesIndex: build.copyMergeHashesIndex});
            // } catch(e) {
            //   report[i] = 'NO PROOF';
            // }
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(4);
            allXs.should.have.same.members(build.regularEvent.mergeHash);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(4);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
              build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
            ]);
            callback();
          }]
        }, callback), callback);
      }]
    }, err => {
      // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
      done(err);
    });
  });
  it('ledger history beta', done => {
    const report = {};
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'beta', mockData, nodes}, callback),
      testAlpha: ['build', (results, callback) => {
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory(
            {ledgerNode, excludeLocalRegularEvents: true}, callback),
          branches: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            callback(null, branches);
          }],
          proof: ['branches', (results, callback) => {
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: results.branches,
              electors
            });
            // try {
            //   report[i] = proofReport({
            //     proof,
            //     copyMergeHashes: build.copyMergeHashes,
            //     copyMergeHashesIndex: build.copyMergeHashesIndex});
            // } catch(e) {
            //   report[i] = 'NO PROOF';
            // }
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(4);
            allXs.should.have.same.members(build.regularEvent.mergeHash);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(4);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
              build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
            ]);
            callback();
          }]
        }, callback), callback);
      }]
    }, err => {
      // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
      done(err);
    });
  }); // end test 2
  it('ledger history gamma', function(done) {
    this.timeout(120000);
    const report = {};
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'gamma', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory(
            {ledgerNode, excludeLocalRegularEvents: true}, callback),
          branches: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            callback(null, branches);
          }],
          proof: ['branches', (results, callback) => {
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: results.branches,
              electors
            });
            // try {
            //   report[i] = proofReport({
            //     proof,
            //     copyMergeHashes: build.copyMergeHashes,
            //     copyMergeHashesIndex: build.copyMergeHashesIndex});
            // } catch(e) {
            //   report[i] = 'NO PROOF';
            // }
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(4);
            allXs.should.have.same.members(build.regularEvent.mergeHash);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(4);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
              build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
            ]);
            callback();
          }]
        }, callback), callback);
      }]
    }, err => {
      // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
      done(err);
    });
  });
  // involves 4 elector nodes and one non-elector
  it('ledger history delta produces same as alpha result', function(done) {
    this.timeout(120000);
    const report = {};
    // add node epsilon for this test and remove it afterwards
    async.auto({
      nodeEpsilon: callback => brLedgerNode.add(
        null, {genesisBlock}, (err, result) => {
          if(err) {
            return done(err);
          }
          nodes.epsilon = result;
          callback();
        }),
      build: ['nodeEpsilon', (results, callback) => helpers.buildHistory(
        {consensusApi, historyId: 'delta', mockData, nodes}, callback)],
      testAll: ['build', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory(
            {ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: branches,
              electors
            });
            // try {
            //   report[i] = proofReport({
            //     proof,
            //     copyMergeHashes: build.copyMergeHashes,
            //     copyMergeHashesIndex: build.copyMergeHashesIndex});
            // } catch(e) {
            //   report[i] = 'NO PROOF';
            // }
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(4);
            const mergeHashes = [
              build.regularEvent.alpha.mergeHash,
              build.regularEvent.beta.mergeHash,
              build.regularEvent.gamma.mergeHash,
              build.regularEvent.delta.mergeHash
              // exclude epsilon (non-elector)
            ];
            allXs.should.have.same.members(mergeHashes);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(4);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
              build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
            ]);
            callback();
          }]
        }, callback), callback);
      }],
      cleanup: ['testAll', (results, callback) => {
        delete nodes.epsilon;
        callback();
      }]
    }, err => {
      // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
      done(err);
    });
  });
  // FIXME: enable test
  it('ledger history epsilon', done => {
    const report = {};
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'epsilon', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory(
            {ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: branches,
              electors
            });
            // try {
            //   report[i] = proofReport({
            //     proof,
            //     copyMergeHashes: build.copyMergeHashes,
            //     copyMergeHashesIndex: build.copyMergeHashesIndex});
            // } catch(e) {
            //   report[i] = 'NO PROOF';
            // }
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(4);
            allXs.should.have.same.members(build.regularEvent.mergeHash);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(4);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
              build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
            ]);
            callback();
          }]
        }, callback), callback);
      }]
    }, err => {
      // console.log('REPORT', JSON.stringify(report, null, 2));
      done(err);
    });
  });
  // add regular event on alpha before running findMergeEventProof on alpha
  it('add regular local event before getting proof', done => {
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      event: ['build', (results, callback) => helpers.addEvent(
        {ledgerNode, eventTemplate}, callback)],
      testAll: ['event', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers).map(p => ({id: p}));
        async.auto({
          history: callback => getRecentHistory(
            {ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: branches,
              electors
            });
            // proofReport({
            //   proof,
            //   copyMergeHashes: build.copyMergeHashes,
            //   copyMergeHashesIndex: build.copyMergeHashesIndex});
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(4);
            allXs.should.have.same.members(build.regularEvent.mergeHash);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(4);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
              build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
            ]);
            callback();
          }]
        }, callback);
      }]
    }, done);
  });
});

function proofReport({proof, copyMergeHashes, copyMergeHashesIndex}) {
  const allXs = proof.consensus.map(p => p.x.eventHash);
  const allYs = proof.consensus.map(p => p.y.eventHash);
  const yCandidates = proof.yCandidates.map(c => c.eventHash);
  // console.log('COPYHASHES', JSON.stringify(copyMergeHashes, null, 2));
  console.log('XXXXXXXXX', allXs);
  console.log('YYYYYYYYY', allYs);
  console.log('YCANDIDATE', yCandidates);
  const xIndex = allXs.map(x => copyMergeHashesIndex[x]);
  const yIndex = allYs.map(y => copyMergeHashesIndex[y]);
  const yCandidateIndex = yCandidates.map(c => copyMergeHashesIndex[c]);
  console.log('REPORTED Xs', xIndex);
  console.log('REPORTED Ys', yIndex);
  console.log('REPORTED yCandidates', yCandidateIndex);
  return {xIndex, yIndex, yCandidateIndex};
}
