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
              const peerId = [peers[i]];
              const keys = Object.keys(branches);
              keys.should.have.length(1);
              keys.should.have.same.members([peers[i]]);
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
      }]
    }, done);
  });
});
