/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const {callbackify} = require('util');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const expect = global.chai.expect;
const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

describe('events API', () => {
  before(async () => {
    helpers.prepareDatabase();
  });
  let repairCache;
  let _cacheKey;
  let Worker;
  const nodes = {};
  const peers = {};
  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const consensusPlugin = await helpers.use('Continuity2017');
    consensusApi = consensusPlugin.api;
    Worker = consensusApi._worker.Worker;
    _cacheKey = consensusApi._cache.cacheKey;
    repairCache = callbackify(consensusApi._events.repairCache);
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    // FIXME is this used anywhere? I might have made a mistake
    // in the refactoring
    // const voter = await consensusApi._peers.get(
    //  {ledgerNodeId: nodes.alpha.id});
    const {genesisBlock: _genesisBlock} = await nodes.alpha.blocks.getGenesis();
    const genesisBlock = _genesisBlock.block;
    nodes.beta = await brLedgerNode.add(null, {genesisBlock});
    nodes.gamma = await brLedgerNode.add(null, {genesisBlock});
    nodes.delta = await brLedgerNode.add(null, {genesisBlock});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const {id: ledgerNodeId} = ledgerNode;
      ledgerNode.worker = new Worker({session: {ledgerNode}});
      const voter = await consensusApi._peers.get({ledgerNodeId});
      peers[key] = voter.id;
      ledgerNode.creatorId = voter.id;
    }
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

  describe('events.repairCache API', () => {
    it('behaves properly when run after a successful merge', done => {
      const ledgerNode = nodes.alpha;
      const eventTemplate = mockData.events.alpha;
      const opTemplate = mockData.operations.alpha;
      async.auto({
        merge: callback => callbackify(helpers.addEventAndMerge)(
          {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
        repair: ['merge', (results, callback) => {
          const {mergeHash: eventHash} = results.merge;
          repairCache({eventHash, ledgerNode}, (err, result) => {
            assertNoError(err);
            const {updateCache} = result;
            updateCache.should.be.an('array');
            // this set of results indicates that no events were removed
            // see `/lib/cache/events.addLocalMergeEvent` for the redis
            // transaction that is executed that returns this result
            updateCache.should.eql([0, 0, 'OK', 'OK', 1, 0, 'OK']);
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
      const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
      const eventTemplate = mockData.events.alpha;
      const opTemplate = mockData.operations.alpha;
      async.auto({
        merge: callback => callbackify(helpers.addEventAndMerge)(
          {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
        // recreate conditions that would exist if mongodb write had succeeded
        // but cache update had failed
        rebuildCache: ['merge', (results, callback) => {
          const {mergeHash: eventHash, regularHashes} = results.merge;

          const outstandingMergeEventKey = _cacheKey.outstandingMergeEvent(
            {eventHash, ledgerNodeId});
          const headKey = _cacheKey.head({creatorId, ledgerNodeId});
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const parentHashes = [...regularHashes];
          cache.client.multi()
            .sadd(childlessKey, parentHashes)
            .sadd(localChildlessKey, regularHashes)
            .del(outstandingMergeEventKey, headKey)
            .srem(outstandingMergeKey, outstandingMergeEventKey)
            .exec(callback);
        }],
        repair: ['rebuildCache', (results, callback) => {
          const {mergeHash: eventHash} = results.merge;
          repairCache({eventHash, ledgerNode}, (err, result) => {
            assertNoError(err);
            const {updateCache} = result;
            updateCache.should.be.an('array');
            // this set of results indicates that the cache was updated properly
            updateCache.should.eql([1, 1, 'OK', 'OK', 1, 1, 'OK']);
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
      const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
      const eventTemplate = mockData.events.alpha;
      const opTemplate = mockData.operations.alpha;
      async.auto({
        merge: callback => callbackify(helpers.addEventAndMerge)(
          {consensusApi, count: 5, eventTemplate, ledgerNode, opTemplate},
          callback),
        // recreate conditions that would exist if mongodb write had succeeded
        // but cache update had failed
        rebuildCache: ['merge', (results, callback) => {
          const {mergeHash: eventHash, regularHashes} = results.merge;

          const outstandingMergeEventKey = _cacheKey.outstandingMergeEvent(
            {eventHash, ledgerNodeId});
          const headKey = _cacheKey.head({creatorId, ledgerNodeId});
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const parentHashes = [...regularHashes];
          cache.client.multi()
            .sadd(childlessKey, parentHashes)
            .sadd(localChildlessKey, regularHashes)
            .del(outstandingMergeEventKey, headKey)
            .srem(outstandingMergeKey, outstandingMergeEventKey)
            .exec(callback);
        }],
        repair: ['rebuildCache', (results, callback) => {
          const {mergeHash: eventHash} = results.merge;
          repairCache({eventHash, ledgerNode}, (err, result) => {
            assertNoError(err);
            const {updateCache} = result;
            updateCache.should.be.an('array');
            // this set of results indicates that the cache was updated properly
            updateCache.should.eql([5, 5, 'OK', 'OK', 1, 1, 'OK']);
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
