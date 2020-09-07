/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

/* eslint-disable no-unused-vars */
describe('Election API _findMergeEventProof', () => {
  before(async function() {
    await helpers.prepareDatabase(mockData);
  });
  let _findMergeEventProof;
  let _getElectorBranches;
  let genesisBlock;
  let getRecentHistory;
  let EventWriter;
  const nodes = {};
  const peers = {};
  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    await helpers.flushCache();
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const plugin = helpers.use('Continuity2017');
    consensusApi = plugin.api;
    // this enables tests to call on this method
    getRecentHistory = consensusApi._events.getRecentHistory;
    // this method is also called on in tests
    _getElectorBranches = consensusApi._election._getElectorBranches;
    // used by tests
    _findMergeEventProof = consensusApi._election._findMergeEventProof;
    EventWriter = consensusApi._worker.EventWriter;
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    consensusApi._voters.get({ledgerNodeId: nodes.alpha.id});
    const {genesisBlock: _genesisBlock} = await nodes.alpha.blocks.getGenesis();
    genesisBlock = _genesisBlock.block;
    nodes.beta = await brLedgerNode.add(null, {genesisBlock});
    nodes.gamma = await brLedgerNode.add(null, {genesisBlock});
    nodes.delta = await brLedgerNode.add(null, {genesisBlock});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      ledgerNode.eventWriter = new EventWriter({ledgerNode});
      const {id: ledgerNodeId} = ledgerNode;
      const voter = await consensusApi._voters.get({ledgerNodeId});
      peers[key] = voter.id;
      ledgerNode.creatorId = voter.id;
      helpers.peersReverse[voter.id] = key;
    }
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

  describe('"first" mode', () => {
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
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
            proof: ['history', (results, callback) => {
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'first'
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
          // all peers are electors
          const electors = _.values(peers).map(p => ({id: p}));
          async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
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
                electors,
                mode: 'first'
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
          // all peers are electors
          const electors = _.values(peers).map(p => ({id: p}));
          async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
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
                electors,
                mode: 'first'
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
      // all peers except epsilon are electors
      const electors = _.values(peers)
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
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'first'
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
      // all peers except epsilon are electors
      const electors = _.values(peers)
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
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'first'
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
          // all peers are electors
          const electors = _.values(peers).map(p => ({id: p}));
          async.auto({
            history: callback => getRecentHistory({
              creatorId: nodes.alpha.creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
            proof: ['history', (results, callback) => {
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'first'
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

  describe('"firstWithConsensusProof" mode', () => {
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
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
            proof: ['history', (results, callback) => {
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'firstWithConsensusProof'
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
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
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
                electors,
                mode: 'firstWithConsensusProof'
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
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
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
                electors,
                mode: 'firstWithConsensusProof'
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
          // all peers are electors
          const electors = _.values(peers).map(p => ({id: p}));
          async.eachOfSeries(nodes, (ledgerNode, i, callback) => async.auto({
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
            proof: ['history', (results, callback) => {
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'firstWithConsensusProof'
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
    // FIXME: this test was passing before upgrade
    // it is now failing becauses proof.consensus is empty for alpha etc.
    it.skip('ledger history epsilon', done => {
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
            history: callback => getRecentHistory({
              creatorId: nodes[i].creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
            proof: ['history', (results, callback) => {
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'firstWithConsensusProof'
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
      const opTemplate = mockData.operations.alpha;
      async.auto({
        build: callback => helpers.buildHistory(
          {consensusApi, historyId: 'alpha', mockData, nodes}, callback),
        event: ['build', (results, callback) => helpers.addEvent(
          {ledgerNode, eventTemplate, opTemplate}, callback)],
        testAll: ['event', (results, callback) => {
          // NOTE: for ledger history alpha, all nodes should have the same view
          const build = results.build;
          // all peers are electors
          const electors = _.values(peers).map(p => ({id: p}));
          async.auto({
            history: callback => getRecentHistory({
              creatorId: nodes.alpha.creatorId,
              ledgerNode, excludeLocalRegularEvents: true}, callback),
            proof: ['history', (results, callback) => {
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const proof = _findMergeEventProof({
                ledgerNode,
                history: results.history,
                tails: branches,
                electors,
                mode: 'firstWithConsensusProof'
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
