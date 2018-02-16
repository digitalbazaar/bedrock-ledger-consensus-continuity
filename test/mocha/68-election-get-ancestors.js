/*!
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

describe.skip('Election API _getAncestors', () => {
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
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
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
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.alpha = result;
          callback(null, result);
        })],
      creatorId: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(nodes.alpha.id, (err, result) => {
          callback(null, result.id);
        });
      }],
      genesisMerge: ['creatorId', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          ledgerNodeId: nodes.alpha.id,
          eventsCollection: nodes.alpha.storage.events.collection,
          creatorId: results.creatorId,
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
  it('gets no events', done => {
    // the genesisMerge already has consensus
    const getAncestors = consensusApi._worker._election._getAncestors;
    getAncestors(
      {ledgerNode: nodes.alpha, eventHash: genesisMerge},
      (err, result) => {
        assertNoError(err);
        should.exist(result);
        result.should.be.an('array');
        result.should.have.length(0);
        done();
      });
  });
  it('gets two events', done => {
    const getAncestors = consensusApi._worker._election._getAncestors;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode}, callback),
      test: ['event1', (results, callback) => getAncestors(
        {ledgerNode, eventHash: results.event1.merge.meta.eventHash},
        (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(2);
          callback();
        })]
    }, done);
  });
  it('gets four events', done => {
    const getAncestors = consensusApi._worker._election._getAncestors;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      event1: callback => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode}, callback),
      event2: ['event1', (results, callback) => helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode}, callback)],
      test: ['event2', (results, callback) => getAncestors(
        {ledgerNode, eventHash: results.event2.merge.meta.eventHash},
        (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
          callback();
        })]
    }, done);
  });
  it('gets 4 events involving 2 nodes', done => {
    const getAncestors = consensusApi._worker._election._getAncestors;
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
      test: ['cp2', (results, callback) => getAncestors(
        {ledgerNode, eventHash: results.cp2.meta.eventHash},
        (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
          callback();
        })]
    }, done);
  });
  it('gets 4 events without duplicates', done => {
    const getAncestors = consensusApi._worker._election._getAncestors;
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
      test: ['cp2', (results, callback) => getAncestors({
        ledgerNode,
        eventHash: [results.cp1.meta.eventHash, results.cp2.meta.eventHash]
      }, (err, result) => {
        assertNoError(err);
        should.exist(result);
        result.should.be.an('array');
        result.should.have.length(4);
        result.forEach(e => {
          e.event.should.be.an('array');
          e.event.should.have.length(1);
        });
        callback();
      })]
    }, done);
  });
});
