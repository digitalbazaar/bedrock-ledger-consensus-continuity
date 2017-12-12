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
  it.only('Test 1', done => {
    console.log('PEERS', peers);
    console.log('COLLECTIONS');
    Object.keys(nodes).forEach(nodeLabel => {
      console.log(
        `${nodeLabel}: ${nodes[nodeLabel].storage.events.collection.s.name}`);
    });
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
      cpa: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp1: ['cpa', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cpb: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp2: ['cpb', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // snapshot gamma before copy
      ss1: ['cp1', 'cp2', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp3: ['ss1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.beta, nodes.delta],
        to: nodes.gamma
      }, callback)],
      cp4: ['ss1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.gamma],
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      // snapshot gamma before copy
      ss2: ['cp3', 'cp4', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp5: ['ss2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp6: ['ss2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      cp7: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp8: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp9: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp10: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // snapshot gamma before copy
      ss3: ['cp9', 'cp10', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp11: ['ss3', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp12: ['ss3', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      // snapshot gamma before copy
      ss4: ['cp11', 'cp12', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp13: ['ss4', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp14: ['ss4', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      cp15: ['cp13', 'cp14', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp16: ['cp13', 'cp14', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp17: ['cp15', 'cp16', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp18: ['cp15', 'cp16', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // snapshot gamma before copy
      ss5: ['cp17', 'cp18', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp19: ['ss5', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp20: ['ss5', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      // snapshot gamma before copy
      ss6: ['cp19', 'cp20', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp21: ['ss6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp22: ['ss6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      cp23: ['cp21', 'cp22', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp24: ['cp21', 'cp22', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      testAlpha: ['cp23', 'cp24', (results, callback) => {
        // all peers are electors
        const electors = _.values(peers);
        const ledgerNode = nodes.alpha;
        const copyMergeHashes = {};
        const copyMergeHashesIndex = {};
        Object.keys(results).forEach(key => {
          if(key.startsWith('cp')) {
            copyMergeHashes[key] = results[key].meta.eventHash;
            copyMergeHashesIndex[results[key].meta.eventHash] = key;
          }
        });
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
              history: results.history,
              tails: results.branches,
              electors
            });
            // console.log('ALPHA COLLECTION: ',
            //   ledgerNode.storage.events.collection.s.name);
            // proofReport({proof, copyMergeHashes, copyMergeHashesIndex});
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(2);
            allXs.should.have.same.members([
              copyMergeHashes.cp5, copyMergeHashes.cp6
            ]);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(2);
            allYs.should.have.same.members([
              copyMergeHashes.cp13, copyMergeHashes.cp14
            ]);
            callback();
          }]
        }, callback);
      }]
    }, done);
  });
  it.only('Test 2', done => {
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
      cpa: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp1: ['cpa', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cpb: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp2: ['cpb', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // snapshot gamma before copy
      ss1: ['cp1', 'cp2', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp3: ['ss1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.beta, nodes.delta],
        to: nodes.gamma
      }, callback)],
      cp4: ['ss1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.gamma],
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      // snapshot gamma before copy
      ss2: ['cp3', 'cp4', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp5: ['ss2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp6: ['ss2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      cp7: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp8: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp9: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp10: ['cp7', 'cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // snapshot gamma before copy
      ss3: ['cp9', 'cp10', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp11: ['ss3', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp12: ['ss3', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      // snapshot gamma before copy
      ss4: ['cp11', 'cp12', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp13: ['ss4', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp14: ['ss4', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      cp15: ['cp14', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.delta
      }, callback)],
      cp16: ['cp15', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.beta, nodes.delta],
        to: nodes.alpha
      }, callback)],
      cp17: ['cp16', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.delta],
        to: nodes.beta
      }, callback)],
      cp18: ['cp17', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp19: ['cp18', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.delta
      }, callback)],
      cp20: ['cp19', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      testAlpha: ['cp20', (results, callback) => {
        // all peers are electors
        const electors = _.values(peers);
        const ledgerNode = nodes.alpha;
        console.log('ELECTORS', electors);
        const copyMergeHashes = {};
        const copyMergeHashesIndex = {};
        Object.keys(results).forEach(key => {
          if(key.startsWith('cp')) {
            copyMergeHashes[key] = results[key].meta.eventHash;
            copyMergeHashesIndex[results[key].meta.eventHash] = key;
          }
        });
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
              history: results.history,
              tails: results.branches,
              electors
            });
            // console.log('ALPHA COLLECTION: ', ledgerNode.storage.events.collection.s.name);
            // proofReport({proof, copyMergeHashes, copyMergeHashesIndex});
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(1);
            allXs.should.have.same.members([copyMergeHashes.cp6]);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(1);
            allYs.should.have.same.members([copyMergeHashes.cp14]);
            callback();
          }]
        }, callback);
      }]
    }, done);
  }); // end test 2
  it.only('Test 3', done => {
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
      cpa: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp1: ['cpa', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cpb: ['regularEvent', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp2: ['cpb', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      // snapshot gamma before copy
      ss1: ['cp1', 'cp2', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp3: ['ss1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.beta, nodes.delta],
        to: nodes.gamma
      }, callback)],
      cp4: ['ss1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.gamma],
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      // snapshot gamma before copy
      ss2: ['cp3', 'cp4', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp5: ['ss2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp6: ['ss2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      cp7: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp8: ['cp5', 'cp6', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp9: ['cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.delta],
        to: nodes.beta
      }, callback)],
      cp10: ['cp8', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.delta],
        to: nodes.gamma
      }, callback)],
      cp11: ['cp9', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp12: ['cp10', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      cp13: ['cp11', 'cp12', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.delta],
        to: nodes.beta
      }, callback)],
      cp14: ['cp11', 'cp12', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.alpha, nodes.delta],
        to: nodes.gamma
      }, callback)],
      cp15: ['cp13', 'cp14', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.beta, nodes.gamma],
        to: nodes.alpha
      }, callback)],
      cp16: ['cp13', 'cp14', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: [nodes.beta, nodes.gamma],
        to: nodes.delta
      }, callback)],
      testAlpha: ['cp15', 'cp16', (results, callback) => {
        // all peers are electors
        const electors = _.values(peers);
        const ledgerNode = nodes.alpha;
        console.log('ELECTORS', electors);
        const copyMergeHashes = {};
        const copyMergeHashesIndex = {};
        Object.keys(results).forEach(key => {
          if(key.startsWith('cp')) {
            copyMergeHashes[key] = results[key].meta.eventHash;
            copyMergeHashesIndex[results[key].meta.eventHash] = key;
          }
        });
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
              history: results.history,
              tails: results.branches,
              electors
            });
            // console.log('ALPHA COLLECTION: ', ledgerNode.storage.events.collection.s.name);
            // proofReport({proof, copyMergeHashes, copyMergeHashesIndex});
            const allXs = proof.consensus.map(p => p.x.eventHash);
            allXs.should.have.length(2);
            allXs.should.have.same.members([
              copyMergeHashes.cp5, copyMergeHashes.cp6
            ]);
            const allYs = proof.consensus.map(p => p.y.eventHash);
            allYs.should.have.length(2);
            allYs.should.have.same.members([
              copyMergeHashes.cp13, copyMergeHashes.cp14
            ]);
            callback();
          }]
        }, callback);
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
