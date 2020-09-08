/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const {config} = bedrock;
const hasher = brLedgerNode.consensus._hasher;
const helpers = require('./helpers');
const mockData = require('./mock.data');
const {util: {uuid}} = bedrock;

let consensusApi;

describe('blocks API', () => {
  before(async function() {
    await helpers.prepareDatabase(mockData);
  });
  let repairCache;
  let _cacheKey;
  let genesisMergeHash;
  const nodes = {};
  const peers = {};
  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    await helpers.flushCache();
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const consensusPlugin = helpers.use('Continuity2017');
    consensusApi = consensusPlugin.api;
    _cacheKey = consensusApi._cache.cacheKey;
    repairCache = consensusApi._blocks.repairCache;
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    const alphaVoter = await consensusApi._voters.get(
      {ledgerNodeId: nodes.alpha.id});
    const {id: creatorId} = alphaVoter;
    const ledgerNode = nodes.alpha;
    genesisMergeHash = await consensusApi._events.getHead(
      {creatorId, ledgerNode});
    const {genesisBlock: _genesisBlock} = await nodes.alpha.blocks.getGenesis();
    const genesisBlock = _genesisBlock.block;
    nodes.beta = await brLedgerNode.add(null, {genesisBlock});
    nodes.gamma = await brLedgerNode.add(null, {genesisBlock});
    nodes.delta = await brLedgerNode.add(null, {genesisBlock});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const {id: ledgerNodeId} = ledgerNode;
      const voter = await consensusApi._voters.get({ledgerNodeId});
      peers[key] = voter.id;
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
          const outstandingMergeEventKeys = hashes.map(eventHash =>
            _cacheKey.outstandingMergeEvent({eventHash, ledgerNodeId}));
          const multi = cache.client.multi();
          multi.sadd(outstandingMergeKey, outstandingMergeEventKeys);
          multi.set(blockHeightKey, 0);
          outstandingMergeEventKeys.forEach(k => multi.set(
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
              ledgerNode.storage.events.collection.insertOne(
                eventRecord, callback);
            }]
          }, callback);
        }, callback),
        cache: ['events', (results, callback) => {
          const hashes = results.events.map(e => e.eventHash);
          const ledgerNodeId = nodes.alpha.id;
          const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const outstandingMergeEventKeys = hashes.map(eventHash =>
            _cacheKey.outstandingMergeEvent({eventHash, ledgerNodeId}));
          const multi = cache.client.multi();
          multi.sadd(outstandingMergeKey, outstandingMergeEventKeys);
          multi.set(blockHeightKey, 499);
          outstandingMergeEventKeys.forEach(k => multi.set(
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
