/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const {callbackify} = require('util');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

/* eslint-disable no-unused-vars */
describe('Election API _findMergeEventProof', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let _findMergeEventProof;
  let _getTails;
  let genesisBlock;
  let getRecentHistory;
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
          getRecentHistory = consensusApi._events.getRecentHistory;
          _getTails = consensusApi._election._getTails;
          _findMergeEventProof = consensusApi._election._findMergeEventProof;
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
        consensusApi._voters.get(
          {ledgerNodeId: nodes.alpha.id}, (err, result) => {
            callback(null, result.id);
          });
      }],
      // genesisMerge: ['creatorId', (results, callback) => {
      //   consensusApi._events.getHead({
      //     creatorId: results.creatorId,
      //     ledgerNode: nodes.alpha
      //   }, (err, result) => {
      //     if(err) {
      //       return callback(err);
      //     }
      //     // genesisMerge = result;
      //     callback();
      //   });
      // }],
      genesisBlock: ['ledgerNode', (results, callback) =>
        nodes.alpha.blocks.getGenesis((err, result) => {
          if(err) {
            return callback(err);
          }
          genesisBlock = result.genesisBlock.block;
          callback(null, genesisBlock);
        })],
      nodeBeta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.beta = result;
          callback(null, result);
        })],
      nodeGamma: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.gamma = result;
          callback(null, result);
        })],
      nodeDelta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock}, (err, result) => {
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

  it('ledger history alpha', done => {
    const report = {};
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        // all peers are witnesses
        const witnesses = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory({
            creatorId: nodes[i].creatorId,
            ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const {tails, witnessTails} = _getTails({
              history: results.history,
              witnesses
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails,
              witnessTails,
              witnesses
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
            allYs.should.have.same.members(build.regularEvent.mergeHash);
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
        // all peers are witnesses
        const witnesses = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory({
            creatorId: nodes[i].creatorId,
            ledgerNode, excludeLocalRegularEvents: true}, callback),
          branches: ['history', (results, callback) => {
            const {tails, witnessTails} = _getTails({
              history: results.history,
              witnesses
            });
            callback(null, {tails, witnessTails});
          }],
          proof: ['branches', (results, callback) => {
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: results.branches.tails,
              witnessTails: results.branches.witnessTails,
              witnesses
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
            allYs.should.have.same.members(build.regularEvent.mergeHash);
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
        // all peers are witnesses
        const witnesses = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory({
            creatorId: nodes[i].creatorId,
            ledgerNode, excludeLocalRegularEvents: true}, callback),
          branches: ['history', (results, callback) => {
            const {tails, witnessTails} = _getTails({
              history: results.history,
              witnesses
            });
            callback(null, {tails, witnessTails});
          }],
          proof: ['branches', (results, callback) => {
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails: results.branches.tails,
              witnessTails: results.branches.witnessTails,
              witnesses
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
            allYs.should.have.same.members(build.regularEvent.mergeHash);
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
    // all peers except epsilon are witnesses
    const witnesses = _.values(peers)
      .filter(p => p !== peers.epsilon)
      .map(p => ({id: p}));
    // add node epsilon for this test and remove it afterwards
    async.auto({
      nodeEpsilon: callback => async.auto({
        add: callback => brLedgerNode.add(
          null, {genesisBlock}, (err, result) => {
            if(err) {
              return callback(err);
            }
            nodes.epsilon = result;
            nodes.epsilon.eventWriter = new EventWriter(
              {ledgerNode: nodes.epsilon});
            callback();
          }),
        creator: ['add', (results, callback) =>
          consensusApi._voters.get(
            {ledgerNodeId: nodes.epsilon.id}, (err, result) => {
              if(err) {
                return callback(err);
              }
              peers.epsilon = result.id;
              nodes.epsilon.creatorId = result.id;
              helpers.peersReverse[result.id] = 'epsilon';
              callback();
            })]
      }, callback),
      build: ['nodeEpsilon', (results, callback) => helpers.buildHistory(
        {consensusApi, historyId: 'delta', mockData, nodes}, callback)],
      testAll: ['build', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory({
            creatorId: nodes[i].creatorId,
            ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const {tails, witnessTails} = _getTails({
              history: results.history,
              witnesses
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails,
              witnessTails,
              witnesses
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
            allYs.should.have.same.members(mergeHashes);
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
    // all peers except epsilon are witnesses
    const witnesses = _.values(peers)
      .filter(p => p !== peers.epsilon)
      .map(p => ({id: p}));
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'epsilon', mockData, nodes}, callback),
      testAll: ['build', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
          history: callback => getRecentHistory({
            creatorId: nodes[i].creatorId,
            ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const {tails, witnessTails} = _getTails({
              history: results.history,
              witnesses
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails,
              witnessTails,
              witnesses
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
            allYs.should.have.same.members(build.regularEvent.mergeHash);
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
    const opTemplate = mockData.operations.alpha;
    async.auto({
      build: callback => helpers.buildHistory(
        {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
      event: ['build', (results, callback) => helpers.addEvent(
        {ledgerNode, eventTemplate, opTemplate}, callback)],
      testAll: ['event', (results, callback) => {
        // NOTE: for ledger history alpha, all nodes should have the same view
        const build = results.build;
        // all peers are witnesses
        const witnesses = _.values(peers).map(p => ({id: p}));
        async.auto({
          history: callback => getRecentHistory({
            creatorId: nodes.alpha.creatorId,
            ledgerNode, excludeLocalRegularEvents: true}, callback),
          proof: ['history', (results, callback) => {
            const {tails, witnessTails} = _getTails({
              history: results.history,
              witnesses
            });
            const proof = _findMergeEventProof({
              ledgerNode,
              history: results.history,
              tails,
              witnessTails,
              witnesses
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
            allYs.should.have.same.members(build.regularEvent.mergeHash);
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
