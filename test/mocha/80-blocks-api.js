/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const {config} = bedrock;
const database = require('bedrock-mongodb');
const hasher = brLedgerNode.consensus._hasher;
const helpers = require('./helpers');
const mockData = require('./mock.data');
// const util = require('util');
const uuid = require('uuid/v4');

let consensusApi;

describe('blocks API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let repairCache;
  let _cacheKey;
  let genesisMergeHash;
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
          _cacheKey = consensusApi._cacheKey;
          repairCache = consensusApi._blocks.repairCache;
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
              callback();
            }), callback)]
    }, done);
  });

  describe('blocks.repairCache API', () => {
    // add the genesis merge event to the cache
    beforeEach(done => {
      const hashes = [genesisMergeHash.eventHash];
      const ledgerNodeId = nodes.alpha.id;
      const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
      const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
      const eventKeys = hashes.map(eventHash => _cacheKey.event(
        {eventHash, ledgerNodeId}));
      const multi = cache.client.multi();
      multi.sadd(outstandingMergeKey, eventKeys);
      multi.set(blockHeightKey, 0);
      eventKeys.forEach(k => multi.set(k, JSON.stringify({test: 'string'})));
      multi.exec(done);
    });
    // genesis merge event associated with block 0 should be removed from
    // the cache and blockHeight should be properly incremented
    it('removes one item from the cache and increments blockHeight', done => {
      const ledgerNode = nodes.alpha;
      async.auto({
        prepare: callback => {
          const hashes = [genesisMergeHash.eventHash];
          const ledgerNodeId = nodes.alpha.id;
          const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const eventKeys = hashes.map(eventHash => _cacheKey.event(
            {eventHash, ledgerNodeId}));
          const multi = cache.client.multi();
          multi.sadd(outstandingMergeKey, eventKeys);
          multi.set(blockHeightKey, 0);
          eventKeys.forEach(k => multi.set(
            k, JSON.stringify({test: 'string'})));
          multi.exec(callback);
        },
        repair: ['prepare', (results, callback) => repairCache(
          {blockHeight: 0, ledgerNode}, (err, result) => {
            assertNoError(err);
            should.exist(result.cache);
            result.cache.should.be.an('array');
            result.cache.should.have.length(3);
            // redis indicates that
            //   1 item removed from set
            //   1 key removed
            //   result of blockHeight increment was 1
            result.cache.should.eql([1, 1, 1]);
            callback();
          })]
      }, done);
    });
    it('removes multiple cache items and increments blockHeight', done => {
      const ledgerNode = nodes.alpha;
      async.auto({
        events: callback => async.times(5, (i, callback) => {
          const treeHash = uuid();
          const parentHashes = [uuid(), uuid()];
          const event = {
            '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
            type: 'ContinuityMergeEvent',
            parentHash: [treeHash, ...parentHashes],
            treeHash,
          };
          async.auto({
            eventHash: callback => hasher(event, callback),
            store: ['eventHash', (results, callback) => {
              const {eventHash} = results;
              const meta = {
                blockHeight: 500,
                blockOrder: i,
                continuity2017: {
                  type: 'm'
                },
                eventHash
              };
              const eventRecord = {
                event, meta
              };
              ledgerNode.storage.events.collection.insert(
                eventRecord, callback);
            }]
          }, callback);
        }, callback),
        cache: ['events', (results, callback) => {
          const hashes = results.events.map(e => e.eventHash);
          const ledgerNodeId = nodes.alpha.id;
          const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const eventKeys = hashes.map(eventHash => _cacheKey.event(
            {eventHash, ledgerNodeId}));
          const multi = cache.client.multi();
          multi.sadd(outstandingMergeKey, eventKeys);
          multi.set(blockHeightKey, 499);
          eventKeys.forEach(k => multi.set(
            k, JSON.stringify({test: 'string'})));
          multi.exec(callback);
        }],
        repair: ['cache', (results, callback) => repairCache(
          {blockHeight: 500, ledgerNode}, (err, result) => {
            assertNoError(err);
            should.exist(result.cache);
            result.cache.should.be.an('array');
            result.cache.should.have.length(3);
            // redis indicates that
            //   5 items removed from set
            //   5 keys removed
            //   result of blockHeight increment was 500
            result.cache.should.eql([5, 5, 500]);
            callback();
          })]
      }, done);
    });
    it('returns NotFoundError on invalid blockHeight', done => {
      const ledgerNode = nodes.alpha;
      repairCache({blockHeight: 999, ledgerNode}, err => {
        should.exist(err);
        err.name.should.equal('NotFoundError');
        done();
      });
    });
  }); // _repairCache
});
