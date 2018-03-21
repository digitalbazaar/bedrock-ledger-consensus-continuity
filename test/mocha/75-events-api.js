/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const expect = global.chai.expect;
// const hasher = brLedgerNode.consensus._hasher;
const helpers = require('./helpers');
const mockData = require('./mock.data');
// const util = require('util');
const uuid = require('uuid/v4');

let consensusApi;

describe('events API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let repairCache;
  let _cacheKey;
  let getRecentHistory;
  let mergeBranches;
  let genesisMergeHash;
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
          getRecentHistory = consensusApi._worker._events.getRecentHistory;
          mergeBranches = consensusApi._worker._events.mergeBranches;
          _cacheKey = consensusApi._cacheKey;
          repairCache = consensusApi._events.repairCache;
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
        consensusApi._worker._voters.get(
          {ledgerNodeId: nodes.alpha.id}, (err, result) => {
            callback(null, result.id);
          });
      }],
      genesisMerge: ['creatorId', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          creatorId: results.creatorId,
          ledgerNode: nodes.alpha,
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMergeHash = result;
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
          consensusApi._worker._voters.get(
            {ledgerNodeId: n.id}, (err, result) => {
              if(err) {
                return callback(err);
              }
              peers[i] = result.id;
              n.creatorId = result.id;
              callback();
            }), callback)]
    }, done);
  });

  describe('events.repairCache API', () => {
    it('behaves properly when run after a successful merge', done => {
      const ledgerNode = nodes.alpha;
      const eventTemplate = mockData.events.alpha;
      async.auto({
        merge: callback => helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode}, callback),
        repair: ['merge', (results, callback) => {
          const {mergeHash: eventHash} = results.merge;
          repairCache({eventHash, ledgerNode}, (err, result) => {
            assertNoError(err);
            const {updateCache} = result;
            updateCache.should.be.an('array');
            // this set of results indicates that no events were removed
            updateCache.should.eql([0, 'OK', 0, 'OK', 0]);
            callback();
          });
        }]
      }, done);
    });
    it('repairs the cache after a failed merge', done => {
      const ledgerNode = nodes.alpha;
      // NOTE: creatorID is added to ledgerNode object in tests only
      const {creatorId} = ledgerNode;
      const ledgerNodeId = ledgerNode.id;
      const childlessKey = _cacheKey.childless(ledgerNodeId);
      const eventTemplate = mockData.events.alpha;
      async.auto({
        merge: callback => helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode}, callback),
        // recreate conditions that would exist if mongodb write had succeeded
        // but cache update had failed
        rebuildCache: ['merge', (results, callback) => {
          const {mergeHash: eventHash, regularHashes} = results.merge;

          const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
          const headKey = _cacheKey.head({creatorId, ledgerNodeId});
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const parentHashes = [...regularHashes];
          cache.client.multi()
            .sadd(childlessKey, parentHashes)
            .del(eventKey, headKey)
            .srem(outstandingMergeKey, eventKey)
            .exec(callback);
        }],
        repair: ['rebuildCache', (results, callback) => {
          const {mergeHash: eventHash} = results.merge;
          repairCache({eventHash, ledgerNode}, (err, result) => {
            assertNoError(err);
            const {updateCache} = result;
            updateCache.should.be.an('array');
            // this set of results indicates that the cache was updated properly
            updateCache.should.eql([1, 'OK', 1, 'OK', 0]);
            callback();
          });
        }],
        test: ['repair', (results, callback) => {
          // childlessKey should not exist (empty)
          cache.client.get(childlessKey, (err, result) => {
            assertNoError(err);
            expect(result).to.be.null;
            callback();
          });
        }]
      }, done);
    });
    it('repairs cache after a failed merge involving multiple events', done => {
      const ledgerNode = nodes.alpha;
      // NOTE: creatorID is added to ledgerNode object in tests only
      const {creatorId} = ledgerNode;
      const ledgerNodeId = ledgerNode.id;
      const childlessKey = _cacheKey.childless(ledgerNodeId);
      const eventTemplate = mockData.events.alpha;
      async.auto({
        merge: callback => helpers.addEventAndMerge(
          {consensusApi, count: 5, eventTemplate, ledgerNode}, callback),
        // recreate conditions that would exist if mongodb write had succeeded
        // but cache update had failed
        rebuildCache: ['merge', (results, callback) => {
          const {mergeHash: eventHash, regularHashes} = results.merge;

          const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
          const headKey = _cacheKey.head({creatorId, ledgerNodeId});
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const parentHashes = [...regularHashes];
          cache.client.multi()
            .sadd(childlessKey, parentHashes)
            .del(eventKey, headKey)
            .srem(outstandingMergeKey, eventKey)
            .exec(callback);
        }],
        repair: ['rebuildCache', (results, callback) => {
          const {mergeHash: eventHash} = results.merge;
          repairCache({eventHash, ledgerNode}, (err, result) => {
            assertNoError(err);
            const {updateCache} = result;
            updateCache.should.be.an('array');
            // this set of results indicates that the cache was updated properly
            updateCache.should.eql([5, 'OK', 1, 'OK', 0]);
            callback();
          });
        }],
        test: ['repair', (results, callback) => {
          // childlessKey should not exist (empty)
          cache.client.get(childlessKey, (err, result) => {
            assertNoError(err);
            expect(result).to.be.null;
            callback();
          });
        }]
      }, done);
    });
  }); // repairCache
});
