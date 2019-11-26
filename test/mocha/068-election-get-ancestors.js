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

describe('Election API _getAncestors', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let genesisMerge;
  let EventWriter;
  const nodes = {};
  const peers = {};
  beforeEach(function(done) {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      flush: helpers.flushCache,
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
        const ledgerNode = nodes.alpha;
        const {id: ledgerNodeId} = ledgerNode;
        consensusApi._voters.get({ledgerNodeId}, (err, result) => {
          ledgerNode.creatorId = result.id;
          callback(null, result.id);
        });
      }],
      genesisMerge: ['creatorId', (results, callback) => {
        const ledgerNode = nodes.alpha;
        const {creatorId} = ledgerNode;
        consensusApi._events.getHead({creatorId, ledgerNode}, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMerge = result.eventHash;
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
        async.eachOf(nodes, (ledgerNode, i, callback) => {
          const {id: ledgerNodeId} = ledgerNode;
          // attach eventWriter to the node
          ledgerNode.eventWriter = new EventWriter({ledgerNode});
          consensusApi._voters.get({ledgerNodeId}, (err, result) => {
            if(err) {
              return callback(err);
            }
            ledgerNode.creatorId = result.id;
            peers[i] = result.id;
            callback();
          });
        }, callback)]
    }, done);
  });
  it('gets no events', async () => {
    // the genesisMerge already has consensus
    const getAncestors = consensusApi._election._getAncestors;
    const hashes = {mergeEventHashes: [], parentHashes: [genesisMerge]};
    const result = await getAncestors({ledgerNode: nodes.alpha, hashes});
    should.exist(result);
    result.should.be.an('array');
    result.should.have.length(0);
  });
  it('gets two events', done => {
    const getAncestors = consensusApi._election._getAncestors;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      test: ['event1', async results => {
        const hashes = {
          mergeEventHashes: [results.event1.mergeHash],
          parentHashes: results.event1.merge.event.parentHash
        };
        try {
          const result = await getAncestors({hashes, ledgerNode});
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(2);
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });
  it('gets four events', done => {
    const getAncestors = consensusApi._election._getAncestors;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      event2: ['event1', (results, callback) => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback)],
      test: ['event2', async results => {
        const hashes = {
          mergeEventHashes: [results.event2.mergeHash],
          parentHashes: [
            ...results.event1.merge.event.parentHash,
            ...results.event2.merge.event.parentHash
          ]
        };
        try {
          const result = await getAncestors({hashes, ledgerNode});
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });
  it('gets 4 events involving 2 nodes', done => {
    const getAncestors = consensusApi._election._getAncestors;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      cp1: ['event1', (results, callback) => helpers.copyAndMerge({
        consensusApi, from: 'alpha', nodes, to: 'beta'}, callback)],
      cp2: ['cp1', (results, callback) => helpers.copyAndMerge({
        consensusApi, from: 'beta', nodes, to: 'alpha'}, callback)],
      test: ['cp2', async results => {
        const hashes = {
          mergeEventHashes: [
            results.cp1.meta.eventHash,
            results.cp2.meta.eventHash
          ],
          parentHashes: _.uniq([
            ...results.cp1.event.parentHash,
            ...results.cp2.event.parentHash,
          ])
        };
        try {
          const result = await getAncestors({hashes, ledgerNode});
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });

  // FIXME: this test likely needs to be removed, the returned data structure
  // no longer matches the assertions
  it.skip('gets 4 events without duplicates', done => {
    const getAncestors = consensusApi._election._getAncestors;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode}, callback),
      cp1: ['event1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp2: ['cp1', (results, callback) => helpers.copyAndMerge({
        consensusApi,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      test: ['cp2', async results => {
        try {
          const result = await getAncestors({
            ledgerNode,
            eventHash: [results.cp1.meta.eventHash, results.cp2.meta.eventHash]
          });
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
          result.forEach(e => {
            e.event.should.be.an('array');
            e.event.should.have.length(1);
          });
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });
});
