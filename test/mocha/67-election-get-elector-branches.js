/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');

const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

describe.skip('Election API _getElectorBranches', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let genesisMerge;
  let eventHash;
  let testEventId;
  const nodes = {};
  const peers = {};
  beforeEach(function(done) {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        helpers.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.alpha = result;
          callback(null, result);
        })],
      creatorId: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._voters.get(nodes.alpha.id, (err, result) => {
          callback(null, result.id);
        });
      }],
      genesisMerge: ['creatorId', (results, callback) => {
        consensusApi._events._getLocalBranchHead({
          ledgerNodeId: nodes.alpha.id,
          eventsCollection: nodes.alpha.storage.events.collection,
          creatorId: results.creatorId
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
          consensusApi._voters.get(n.id, (err, result) => {
            if(err) {
              return callback(err);
            }
            peers[i] = result.id;
            callback();
          }), callback)]
    }, done);
  });
  it('Test 1', done => {
    console.log('PEERS', peers);
    console.log('COLLECTIONS');
    Object.keys(nodes).forEach(nodeLabel => {
      console.log(
        `${nodeLabel}: ${nodes[nodeLabel].storage.events.collection.s.name}`);
    });
    const getRecentHistory = consensusApi._events.getRecentHistory;
    const _getElectorBranches = consensusApi._election._getElectorBranches;
    const _findMergeEventProof = consensusApi._election._findMergeEventProof;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      // add a regular event and merge on every node
      addEvent1: callback => {
        const events = {};
        async.eachOf(nodes, (n, i, callback) => {
          helpers.addEventAndMerge(
            {consensusApi, eventTemplate, ledgerNode: n}, (err, result) => {
              if(err) {
                return callback(err);
              }
              events[i] = result;
              callback();
            });
        }, err => callback(err, events));
      },
      test1: ['addEvent1', (results, callback) => {
        // all peers are electors
        const addEvent = results.addEvent1;
        const electors = _.values(peers).map(p => ({id: p}));
        async.eachOfSeries(nodes, (n, i, callback) => {
          async.auto({
            history: callback => getRecentHistory({ledgerNode: n}, callback),
            branches: ['history', (results, callback) => {
              const branches = _getElectorBranches({
                history: results.history,
                electors
              });
              const peerId = peers[i];
              const keys = Object.keys(branches);
              keys.should.have.length(1);
              keys.should.have.same.members([peerId]);
              const tailArray = branches[peerId];
              tailArray.should.be.an('array');
              // NOTE: an honest/healthy node should always have exactly 1 tail
              tailArray.should.have.length(1);
              const tail = tailArray[0];
              // tail should be merge event
              const mergeEventHash = addEvent[i].merge.meta.eventHash;
              tail.eventHash.should.equal(mergeEventHash);
              tail._children.should.have.length(0);
              // the regular event
              tail._parents.should.have.length(1);
              const parent = tail._parents[0];
              const regularEventHash = Object.keys(addEvent[i].regular)[0];
              parent.eventHash.should.equal(regularEventHash);
              should.equal(tail._treeParent, null);
              parent._children.should.have.length(1);
              const childOfRegularEvent = parent._children[0];
              childOfRegularEvent.eventHash.should.equal(mergeEventHash);
              callback();
            }],
          }, callback);
        }, callback);
      }],
      // step 3
      cp1: ['test1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      test2: ['cp1', (results, callback) => {
        // test beta
        const addEvent = results.addEvent1;
        const cp1 = results.cp1;
        const electors = _.values(peers).map(p => ({id: p}));
        const ledgerNode = nodes.beta;
        async.auto({
          history: callback => getRecentHistory({ledgerNode}, callback),
          branches: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            const peerId = [peers.alpha, peers.beta];
            const keys = Object.keys(branches);
            keys.should.have.length(2);
            keys.should.have.same.members(peerId);

            // inspect beta tail
            const tailBeta = branches[peers.beta];
            tailBeta.should.have.length(1);
            let tail = tailBeta[0];
            // tail is oldest merge even which has not reached consensus
            tail.eventHash.should.equal(addEvent.beta.merge.meta.eventHash);
            // tail's child should be merge after copy
            const mergeEventHash_cp1 = cp1.meta.eventHash;
            tail._children.should.have.length(1);
            const child0 = tail._children[0];
            child0.eventHash.should.equal(mergeEventHash_cp1);
            child0._children.should.have.length(0);
            child0._parents.should.have.length(2);
            const child0ParentHashes = child0._parents.map(e => e.eventHash);
            child0ParentHashes.should.have.same.members([
              addEvent.beta.merge.meta.eventHash,
              addEvent.alpha.merge.meta.eventHash
            ]);

            // inspect alpha tail
            const tailAlpha = branches[peers.alpha];
            tailAlpha.should.have.length(1);
            tail = tailAlpha[0];
            // tail should be merge event
            const mergeEventHash = addEvent.alpha.merge.meta.eventHash;
            tail.eventHash.should.equal(mergeEventHash);
            tail._children.should.have.length(1);
            tail._children[0].eventHash.should.equal(mergeEventHash_cp1);
            tail._children.should.have.length(1);
            const alphaChild0 = tail._children[0];
            alphaChild0.eventHash.should.equal(cp1.meta.eventHash);
            alphaChild0._children.should.have.length(0);
            alphaChild0._parents.should.have.length(2);
            const alphaChild0ParentHashes =
              alphaChild0._parents.map(e => e.eventHash);
            alphaChild0ParentHashes.should.have.same.members([
              addEvent.alpha.merge.meta.eventHash,
              addEvent.beta.merge.meta.eventHash
            ]);
            // the regular event on alpha will not be refenced here
            tail._parents.should.have.length(0);
            should.equal(tail._treeParent, null);
            callback();
          }],
        }, callback);
      }],
      // step 4
      cp2: ['test2', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      test3: ['cp2', (results, callback) => {
        // test gamma
        const addEvent = results.addEvent1;
        const cp2 = results.cp2;
        const electors = _.values(peers).map(p => ({id: p}));
        const ledgerNode = nodes.gamma;
        async.auto({
          history: callback => getRecentHistory({ledgerNode}, callback),
          branches: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            const peerId = [peers.gamma, peers.delta];
            const keys = Object.keys(branches);
            keys.should.have.length(2);
            keys.should.have.same.members(peerId);
            // inspect gamma tail
            const tailGamma = branches[peers.gamma];
            tailGamma.should.have.length(1);
            let tail = tailGamma[0];
            // tail is oldest merge even which has not reached consensus
            tail.eventHash.should.equal(addEvent.gamma.merge.meta.eventHash);
            // tail's child should be merge after copy
            const mergeEventHash_cp2 = cp2.meta.eventHash;
            tail._children.should.have.length(1);
            const child0 = tail._children[0];
            child0.eventHash.should.equal(mergeEventHash_cp2);
            child0._children.should.have.length(0);
            child0._parents.should.have.length(2);
            const child0ParentHashes = child0._parents.map(e => e.eventHash);
            child0ParentHashes.should.have.same.members([
              addEvent.gamma.merge.meta.eventHash,
              addEvent.delta.merge.meta.eventHash
            ]);
            // inspect delta tail
            const tailDelta = branches[peers.delta];
            tailDelta.should.have.length(1);
            tail = tailDelta[0];
            // tail should be merge event
            const mergeEventHash = addEvent.delta.merge.meta.eventHash;
            tail.eventHash.should.equal(mergeEventHash);
            tail._children.should.have.length(1);
            tail._children[0].eventHash.should.equal(mergeEventHash_cp2);
            // the regular event on alpha will not be refenced here
            tail._parents.should.have.length(0);
            should.equal(tail._treeParent, null);
            callback();
          }],
        }, callback);
      }],
      // step 5
      // snapshot gamma before copy
      ss1: ['test3', (results, callback) => helpers.snapshotEvents(
        {ledgerNode: nodes.gamma}, callback)],
      cp3: ['ss1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      test4: ['cp3', (results, callback) => {
        // test gamma
        const addEvent = results.addEvent1;
        const cp1 = results.cp1;
        const cp2 = results.cp2;
        const cp3 = results.cp3;
        const electors = _.values(peers).map(p => ({id: p}));
        const ledgerNode = nodes.gamma;
        async.auto({
          history: callback => getRecentHistory({ledgerNode}, callback),
          branches: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            // all electors should now be represented
            const peerId = electors;
            const keys = Object.keys(branches);
            keys.should.have.length(4);
            keys.should.have.same.members(_.values(peers));
            // inspect gamma tail
            const tailGamma = branches[peers.gamma];
            tailGamma.should.have.length(1);
            let tail = tailGamma[0];
            // tail is oldest merge even which has not reached consensus
            tail.eventHash.should.equal(addEvent.gamma.merge.meta.eventHash);
            // tail's child should be merge after cp2
            const mergeEventHash_cp2 = cp2.meta.eventHash;
            tail._children.should.have.length(1);
            const child0 = tail._children[0];
            child0.eventHash.should.equal(mergeEventHash_cp2);
            child0._parents.should.have.length(2);
            const child0ParentHashes = child0._parents.map(e => e.eventHash);
            child0ParentHashes.should.have.same.members([
              addEvent.gamma.merge.meta.eventHash,
              addEvent.delta.merge.meta.eventHash
            ]);
            child0._children.should.have.length(1);
            // should be merge event after cp3
            const child0Child0 = child0._children[0];
            child0Child0.eventHash.should.equal(cp3.meta.eventHash);

            // inspect delta tail
            const tailDelta = branches[peers.delta];
            tailDelta.should.have.length(1);
            tail = tailDelta[0];
            // tail should be merge event
            const mergeEventHash = addEvent.delta.merge.meta.eventHash;
            tail.eventHash.should.equal(mergeEventHash);
            tail._children.should.have.length(1);
            tail._children[0].eventHash.should.equal(mergeEventHash_cp2);
            // the regular event on alpha will not be refenced here
            tail._parents.should.have.length(0);
            should.equal(tail._treeParent, null);

            // inspect alpha tail
            const tailAlpha = branches[peers.alpha];
            tailAlpha.should.have.length(1);
            tail = tailAlpha[0];
            // tail should be merge event
            tail.eventHash.should.equal(addEvent.alpha.merge.meta.eventHash);
            tail._children.should.have.length(1);
            const alphaChild0 = tail._children[0];
            alphaChild0.eventHash.should.equal(cp1.meta.eventHash);
            alphaChild0._children.should.have.length(1);
            const alphaChild0Child0 = alphaChild0._children[0];
            alphaChild0Child0.eventHash.should.equal(cp3.meta.eventHash);
            alphaChild0._parents.should.have.length(2);
            const alphaChild0ParentHashes =
              alphaChild0._parents.map(e => e.eventHash);
            alphaChild0ParentHashes.should.have.same.members([
              addEvent.alpha.merge.meta.eventHash,
              addEvent.beta.merge.meta.eventHash
            ]);
            // the regular event on alpha will not be refenced here
            tail._parents.should.have.length(0);
            should.equal(tail._treeParent, null);

            // inspect beta tail
            const tailBeta = branches[peers.beta];
            tailBeta.should.have.length(1);
            tail = tailBeta[0];
            // tail is oldest merge even which has not reached consensus
            tail.eventHash.should.equal(addEvent.beta.merge.meta.eventHash);
            // tail's child should be merge after copy
            const mergeEventHash_cp1 = cp1.meta.eventHash;
            tail._children.should.have.length(1);
            const betaChild0 = tail._children[0];
            betaChild0.eventHash.should.equal(mergeEventHash_cp1);
            betaChild0._children.should.have.length(1);
            const betaChild0Child0 = betaChild0._children[0];
            betaChild0Child0.eventHash.should.equal(cp3.meta.eventHash);
            // inspect parents
            betaChild0._parents.should.have.length(2);
            const betaChild0ParentHashes =
              betaChild0._parents.map(e => e.eventHash);
            betaChild0ParentHashes.should.have.same.members([
              addEvent.beta.merge.meta.eventHash,
              addEvent.alpha.merge.meta.eventHash
            ]);
            callback();
          }],
        }, callback);
      }],
      // step 6
      cp4: ['test4', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.gamma,
        to: nodes.beta,
        useSnapshot: true
      }, callback)],
      test5: ['cp4', (results, callback) => {
        // test gamma
        const addEvent = results.addEvent1;
        const cp1 = results.cp1;
        const cp2 = results.cp2;
        const cp3 = results.cp3;
        const cp4 = results.cp4;
        const electors = _.values(peers).map(p => ({id: p}));
        const ledgerNode = nodes.beta;
        async.auto({
          history: callback => getRecentHistory({ledgerNode}, callback),
          branches: ['history', (results, callback) => {
            const branches = _getElectorBranches({
              history: results.history,
              electors
            });
            // all electors should now be represented
            const peerId = electors;
            const keys = Object.keys(branches);
            keys.should.have.length(4);
            keys.should.have.same.members(_.values(peers));
            // inspect gamma tail
            const tailGamma = branches[peers.gamma];
            tailGamma.should.have.length(1);
            let tail = tailGamma[0];
            // tail is oldest merge even which has not reached consensus
            tail.eventHash.should.equal(addEvent.gamma.merge.meta.eventHash);
            // tail's child should be merge after cp2
            const mergeEventHash_cp2 = cp2.meta.eventHash;
            tail._children.should.have.length(1);
            const child0 = tail._children[0];
            child0.eventHash.should.equal(mergeEventHash_cp2);
            child0._parents.should.have.length(2);
            const child0ParentHashes = child0._parents.map(e => e.eventHash);
            child0ParentHashes.should.have.same.members([
              addEvent.gamma.merge.meta.eventHash,
              addEvent.delta.merge.meta.eventHash
            ]);
            child0._children.should.have.length(1);
            // should be merge event after cp3
            const child0Child0 = child0._children[0];
            child0Child0.eventHash.should.equal(cp4.meta.eventHash);

            // inspect delta tail
            const tailDelta = branches[peers.delta];
            tailDelta.should.have.length(1);
            tail = tailDelta[0];
            // tail should be merge event
            const mergeEventHash = addEvent.delta.merge.meta.eventHash;
            tail.eventHash.should.equal(mergeEventHash);
            tail._children.should.have.length(1);
            tail._children[0].eventHash.should.equal(mergeEventHash_cp2);
            // the regular event on alpha will not be refenced here
            tail._parents.should.have.length(0);
            should.equal(tail._treeParent, null);

            // inspect alpha tail
            const tailAlpha = branches[peers.alpha];
            tailAlpha.should.have.length(1);
            tail = tailAlpha[0];
            // tail should be merge event
            tail.eventHash.should.equal(addEvent.alpha.merge.meta.eventHash);
            tail._children.should.have.length(1);
            const alphaChild0 = tail._children[0];
            alphaChild0.eventHash.should.equal(cp1.meta.eventHash);
            alphaChild0._children.should.have.length(1);
            // FIXME: this is broken, should not be seeing cp3 here
            alphaChild0._children.map(e => e.eventHash)
              .should.have.same.members([cp4.meta.eventHash]);
            return callback();
            // const alphaChild0Child0 = alphaChild0._children[0];
            // alphaChild0Child0.eventHash.should.equal(cp3.meta.eventHash);
            alphaChild0._parents.should.have.length(2);
            const alphaChild0ParentHashes =
              alphaChild0._parents.map(e => e.eventHash);
            alphaChild0ParentHashes.should.have.same.members([
              addEvent.alpha.merge.meta.eventHash,
              addEvent.beta.merge.meta.eventHash
            ]);
            // the regular event on alpha will not be refenced here
            tail._parents.should.have.length(0);
            should.equal(tail._treeParent, null);

            // inspect beta tail
            const tailBeta = branches[peers.beta];
            tailBeta.should.have.length(1);
            tail = tailBeta[0];
            // tail is oldest merge even which has not reached consensus
            tail.eventHash.should.equal(addEvent.beta.merge.meta.eventHash);
            // tail's child should be merge after copy
            const mergeEventHash_cp1 = cp1.meta.eventHash;
            tail._children.should.have.length(1);
            const betaChild0 = tail._children[0];
            betaChild0.eventHash.should.equal(mergeEventHash_cp1);
            betaChild0._children.should.have.length(2);
            // FIXME: inspect children
            const betaChild0Child0 = betaChild0._children[0];
            betaChild0Child0.eventHash.should.equal(cp3.meta.eventHash);
            // inspect parents
            betaChild0._parents.should.have.length(2);
            const betaChild0ParentHashes =
              betaChild0._parents.map(e => e.eventHash);
            betaChild0ParentHashes.should.have.same.members([
              addEvent.beta.merge.meta.eventHash,
              addEvent.alpha.merge.meta.eventHash
            ]);
            callback();
          }],
        }, callback);
      }],
    }, done);
  });
});
