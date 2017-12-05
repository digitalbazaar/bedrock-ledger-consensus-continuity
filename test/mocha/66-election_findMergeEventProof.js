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
  it.only('Test 1', done => {
    console.log('PEERS', peers);
    const getRecentHistory = consensusApi._worker._events.getRecentHistory;
    const _getElectorBranches =
      consensusApi._worker._election._getElectorBranches;
    const _findMergeEventProof =
      consensusApi._worker._election._findMergeEventProof;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      // add a regular event and merge on every node
      regularEvent: callback => async.each(nodes, (n, callback) =>
        helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode: n}, callback), callback),
      // step 3
      cp1: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      // step 4
      cp2: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // step 5
      cp3: ['cp1', 'cp2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 6
      cp4: ['cp1', 'cp2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 7
      cp5: ['cp3', 'cp4', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      // step 8
      cp6: ['cp3', 'cp4', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      // step 9
      cp7: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      // step 10
      cp8: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // step 11
      cp9: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 12
      cp10: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 13
      cp11: ['cp9', 'cp10', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 14
      cp12: ['cp9', 'cp10', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 15
      cp13: ['cp11', 'cp12', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      // step 16
      cp14: ['cp11', 'cp12', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      // step 17
      cp15: ['cp13', 'cp14', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      // step 18
      cp16: ['cp13', 'cp14', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // step 19
      cp17: ['cp15', 'cp16', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 20
      cp18: ['cp15', 'cp16', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 21
      cp19: ['cp17', 'cp18', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 22
      cp20: ['cp17', 'cp18', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 23
      cp21: ['cp19', 'cp20', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      // step 24
      cp22: ['cp19', 'cp20', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      testAlpha: ['cp21', 'cp22', (results, callback) => {
        // all peers are electors
        const electors = _.values(peers);
        const ledgerNode = nodes.alpha;
        console.log('ELECTORS', electors);
        async.auto({
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
              tails: results.branches,
              electors
            });
            console.log('PROOF', util.inspect(proof));
            callback();
          }]
        }, callback);
      }]
    }, done);
  });
  it('Test 2', done => {
    const getRecentHistory = consensusApi._worker._events.getRecentHistory;
    const _getElectorBranches =
      consensusApi._worker._election._getElectorBranches;
    const _findMergeEventProof =
      consensusApi._worker._election._findMergeEventProof;
    async.auto({
      regularEvent: callback => async.each(nodes, (n, callback) => {
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEventId = 'https://example.com/events/' + uuid();
        testEvent.input[0].id = testEventId;
        n.events.add(testEvent, callback);
      }, callback),
      merge1: ['regularEvent', (results, callback) =>
        _mergeOn({nodes}, callback)],
      // step 3
      cp1: ['merge1', (results, callback) => helpers.copyAndMerge({
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      // step 4
      cp2: ['merge1', (results, callback) => helpers.copyAndMerge({
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // step 5
      cp3: ['cp1', 'cp2', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 6
      cp4: ['cp1', 'cp2', (results, callback) => helpers.copyAndMerge({
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 7
      cp5: ['cp3', 'cp4', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      // step 8
      cp6: ['cp3', 'cp4', (results, callback) => helpers.copyAndMerge({
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      // step 9
      cp7: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      // step 10
      cp8: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // step 11
      cp9: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 12
      cp10: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 13
      cp11: ['cp9', 'cp10', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 14
      cp12: ['cp9', 'cp10', (results, callback) => helpers.copyAndMerge({
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // cp beta to delta, merge
      cp13: ['cp11', 'cp12', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.delta
      }, callback)],
      // cp beta and delta to alpha, merge
      cp14: ['cp13', (results, callback) => helpers.copyAndMerge({
        from: [nodes.beta, nodes.delta],
        to: nodes.alpha
      }, callback)],
      // cp alpha to beta, merge
      cp15: ['cp14', (results, callback) => helpers.copyAndMerge({
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      // cp beta to alpha, merge
      cp16: ['cp15', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      // cp beta to delta, merge
      cp17: ['cp15', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.delta
      }, callback)],
      // cp delta to gamma, merge
      cp18: ['cp17', (results, callback) => helpers.copyAndMerge({
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      proof: ['cp18', (results, callback) => {
        const proofs = {};
        // all peers are electors
        const electors = _.values(peers);
        console.log('ELECTORS', electors);
        async.eachOf(nodes, (ledgerNode, i, callback) => {
          async.auto({
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
                tails: results.branches,
                electors
              });
              proofs[i] = proof;
              callback();
            }]
          }, callback);
        }, err => callback(err, proofs));
      }],
      test: ['proof', (results, callback) => {
        console.log('PROOF', util.inspect(results.proof));
        callback();
      }]
    }, done);
  }); // end test 2
  it('Test 3', done => {
    const getRecentHistory = consensusApi._worker._events.getRecentHistory;
    const _getElectorBranches =
      consensusApi._worker._election._getElectorBranches;
    const _findMergeEventProof =
      consensusApi._worker._election._findMergeEventProof;
    async.auto({
      regularEvent: callback => async.each(nodes, (n, callback) => {
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEventId = 'https://example.com/events/' + uuid();
        testEvent.input[0].id = testEventId;
        n.events.add(testEvent, callback);
      }, callback),
      merge1: ['regularEvent', (results, callback) =>
        _mergeOn({nodes}, callback)],
      // step 3
      cp1: ['merge1', (results, callback) => helpers.copyAndMerge({
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      // step 4
      cp2: ['merge1', (results, callback) => helpers.copyAndMerge({
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // step 5
      cp3: ['cp1', 'cp2', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      // step 6
      cp4: ['cp1', 'cp2', (results, callback) => helpers.copyAndMerge({
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      // step 7
      cp5: ['cp3', 'cp4', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      // step 8
      cp6: ['cp3', 'cp4', (results, callback) => helpers.copyAndMerge({
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      // cp alpha and delta to beta, merge
      cp7: ['cp6', (results, callback) => helpers.copyAndMerge({
        from: [nodes.alpha, nodes.delta],
        to: nodes.beta
      }, callback)],
      // cp alpha and delta to gamma, merge
      cp8: ['cp6', (results, callback) => helpers.copyAndMerge({
        from: [nodes.alpha, nodes.delta],
        to: nodes.gamma
      }, callback)],
      // cp beta to alpha, merge
      cp9: ['cp7', (results, callback) => helpers.copyAndMerge({
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      // cp gamma to delta, merge
      cp10: ['cp8', (results, callback) => helpers.copyAndMerge({
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      // cp alpha and delta to beta, merge
      cp11: ['cp10', (results, callback) => helpers.copyAndMerge({
        from: [nodes.alpha, nodes.delta],
        to: nodes.beta
      }, callback)],
      // cp alpha and delta to gamma, merge
      cp12: ['cp10', (results, callback) => helpers.copyAndMerge({
        from: [nodes.alpha, nodes.delta],
        to: nodes.gamma
      }, callback)],
      // cp beta and gamma to alpha, merge
      cp13: ['cp12', (results, callback) => helpers.copyAndMerge({
        from: [nodes.beta, nodes.gamma],
        to: nodes.alpha
      }, callback)],
      // cp beta and gamma to delta, merge
      cp14: ['cp12', (results, callback) => helpers.copyAndMerge({
        from: [nodes.beta, nodes.gamma],
        to: nodes.delta
      }, callback)],
      proof: ['cp14', (results, callback) => {
        const proofs = {};
        // all peers are electors
        const electors = _.values(peers);
        console.log('ELECTORS', electors);
        async.eachOf(nodes, (ledgerNode, i, callback) => {
          async.auto({
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
                tails: results.branches,
                electors
              });
              proofs[i] = proof;
              callback();
            }]
          }, callback);
        }, err => callback(err, proofs));
      }],
      test: ['proof', (results, callback) => {
        console.log('PROOF', util.inspect(results.proof));
        callback();
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

// FIXME: _copyEvents does not currently need eventHash or treeHash until a
// more efficient query is developed
function _copyFromMerge({from, mergeEvent, to}, callback) {
  const treeHash = mergeEvent.event.treeHash;
  const eventHash = mergeEvent.meta.eventHash;
  _copyEvents({eventHash, from, to, treeHash}, callback);
}

// FIXME: make this a helper
function _copyEvents({from, to}, callback) {
  async.auto({
    events: callback => {
      const collection = from.storage.events.collection;
      // FIXME: use a more efficient query, the commented aggregate function
      // is evidently missing some events.
      collection.find({
        'meta.consensus': {$exists: false}
      }).sort({$natural: 1}).toArray(callback);
      // collection.aggregate([
      //   {$match: {eventHash}},
      //   {
      //     $graphLookup: {
      //       from: collection.s.name,
      //       startWith: '$eventHash',
      //       connectFromField: "event.parentHash",
      //       connectToField: "eventHash",
      //       as: "_parents",
      //       restrictSearchWithMatch: {
      //         eventHash: {$ne: treeHash},
      //         'meta.consensus': {$exists: false}
      //       }
      //     },
      //   },
      //   {$unwind: '$_parents'},
      //   {$replaceRoot: {newRoot: '$_parents'}},
      //   // the order of events is unpredictable without this sort, and we
      //   // must ensure that events are added in chronological order
      //   {$sort: {'meta.created': 1}}
      // ], callback);
    },
    add: ['events', (results, callback) => {
      async.eachSeries(results.events, (e, callback) => {
        to.events.add(e.event, {continuity2017: {peer: true}}, err => {
          // FIXME: only ignore dup error
          // ignore errors
          callback();
        });
      }, callback);
    }]
  }, callback);
}
