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

describe.only('Election API _getElectorBranches', () => {
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
        const electors = _.values(peers);
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
        const electors = _.values(peers);
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
            // should be merge after copy
            let mergeEventHash = cp1.meta.eventHash;

            // FIXME: the tail should be the latest merge event from copy?
            // PASSES tail is still the merge event from addEvent
            tail.eventHash.should.equal(addEvent.beta.merge.meta.eventHash);
            // FAILS tail is not the latest merge event
            tail.eventHash.should.equal(mergeEventHash);

            // tail._children.should.have.length(0);
            // inspect alpha tail
            const tailAlpha = branches[peers.alpha];
            tailAlpha.should.have.length(1);
            tail = tailAlpha[0];
            // tail should be merge event
            mergeEventHash = addEvent.alpha.merge.meta.eventHash;
            tail.eventHash.should.equal(mergeEventHash);
            tail._children.should.have.length(1);
            // FIXME: assert child is beta tail
            // the regular event
            // tail._parents.should.have.length(1);
            // const parent = tail._parents[0];
            // const regularEventHash = Object.keys(addEvent.alpha.regular)[0];
            // parent.eventHash.should.equal(regularEventHash);
            // should.equal(tail._treeParent, null);
            // parent._children.should.have.length(1);
            // const childOfRegularEvent = parent._children[0];
            // childOfRegularEvent.eventHash.should.equal(mergeEventHash);
            callback();
          }],
        }, callback);
      }],
    }, done);
  });
});
