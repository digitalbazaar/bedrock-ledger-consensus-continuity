/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
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
  let genesisMerge;
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
  it('ledger history alpha', done => {
    const getRecentHistory = consensusApi._worker._events.getRecentHistory;
    const _getElectorBranches =
      consensusApi._worker._election._getElectorBranches;
    const _findMergeEventProof =
      consensusApi._worker._election._findMergeEventProof;
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers);
        async.each(nodes, (ledgerNode, callback) => async.auto({
          history: callback => getRecentHistory({ledgerNode}, callback),
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
            allXs.should.have.length(2);
            allXs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6
            ]);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(2);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp13, build.copyMergeHashes.cp14
            ]);
            callback();
          }]
        }, callback), callback);
      }]
    }, done);
  });
  it('ledger history beta', done => {
    const getRecentHistory = consensusApi._worker._events.getRecentHistory;
    const _getElectorBranches =
      consensusApi._worker._election._getElectorBranches;
    const _findMergeEventProof =
      consensusApi._worker._election._findMergeEventProof;
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'beta', mockData, nodes}, callback),
      testAlpha: ['build', (results, callback) => {
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers);
        async.each(nodes, (ledgerNode, callback) => async.auto({
          history: callback =>
            getRecentHistory({ledgerNode}, callback),
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
            // proofReport({
            //   proof,
            //   copyMergeHashes: build.copyMergeHashes,
            //   copyMergeHashesIndex: build.copyMergeHashesIndex});
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(1);
            allXs.should.have.same.members([build.copyMergeHashes.cp6]);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(1);
            allYs.should.have.same.members([build.copyMergeHashes.cp14]);
            callback();
          }]
        }, callback), callback);
      }]
    }, done);
  }); // end test 2
  it('ledger history gamma', done => {
    const getRecentHistory = consensusApi._worker._events.getRecentHistory;
    const _getElectorBranches =
      consensusApi._worker._election._getElectorBranches;
    const _findMergeEventProof =
      consensusApi._worker._election._findMergeEventProof;
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'gamma', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        const build = results.build;
        // all peers are electors
        const electors = _.values(peers);
        async.each(nodes, (ledgerNode, callback) => async.auto({
          history: callback =>
            getRecentHistory({ledgerNode}, callback),
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
            // proofReport({
            //   proof,
            //   copyMergeHashes: build.copyMergeHashes,
            //   copyMergeHashesIndex: build.copyMergeHashesIndex});
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(2);
            allXs.should.have.same.members([
              build.copyMergeHashes.cp5, build.copyMergeHashes.cp6
            ]);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(2);
            allYs.should.have.same.members([
              build.copyMergeHashes.cp9, build.copyMergeHashes.cp10
            ]);
            callback();
          }]
        }, callback), callback);
      }]
    }, done);
  });
});

function _mergeOn({nodes}, callback) {
  // FIXME: get mergeBranches by some other reference
  const mergeBranches = consensusApi._worker._events.mergeBranches;
  const events = {};
  async.eachOf(nodes, (n, i, callback) =>
    mergeBranches({ledgerNode: n}, (err, result) => {
      events[i] = result;
      callback(err);
    }), err => callback(err, events));
}

function proofReport({proof, copyMergeHashes, copyMergeHashesIndex}) {
  const allXs = proof.consensus.map(p => p.x.eventHash);
  const allYs = proof.consensus.map(p => p.y.eventHash);
  const yCandidates = proof.yCandidates.map(c => c.eventHash);
  console.log('COPYHASHES', JSON.stringify(copyMergeHashes, null, 2));
  console.log('XXXXXXXXX', allXs);
  console.log('YYYYYYYYY', allYs);
  console.log('YCANDIDATE', yCandidates);
  console.log('REPORTED Xs');
  for(const x in copyMergeHashes) {
    if(allXs.includes(copyMergeHashes[x])) {
      console.log(x);
    }
  }
  console.log('REPORTED Ys');
  for(const y in copyMergeHashes) {
    if(allYs.includes(copyMergeHashes[y])) {
      console.log(y);
    }
  }
  console.log(
    'REPORTED yCandidates',
    yCandidates.map(c => copyMergeHashesIndex[c]));
}
