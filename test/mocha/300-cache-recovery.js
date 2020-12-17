/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const helpers = require('./helpers');
const mockData = require('./mock.data');

const TEST_TIMEOUT = 300000;

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

const opTemplate = mockData.operations.alpha;

// NOTE: alpha is assigned manually
// NOTE: all these may not be used
const nodeLabels = [
  'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota'
];
const nodes = {};
const peers = {};
const heads = {};

describe('Cache Recovery', () => {
  before(async function() {
    this.timeout(TEST_TIMEOUT);
    await helpers.prepareDatabase();
  });

  const nodeCount = 6;
  describe(`Consensus with ${nodeCount} Nodes`, () => {

    // override elector selection to force cycling and 3f+1
    before(() => {
      const witnessSelectionApi = brLedgerNode.use('MostRecentParticipants');
      witnessSelectionApi.api.getBlockElectors = async ({blockHeight}) => {
        const candidates = [];
        for(const p of Object.keys(peers)) {
          candidates.push({id: peers[p]});
        }
        const f = Math.floor((nodeCount - 1) / 3);
        const count = 3 * f + 1;
        // cycle electors deterministically using `blockHeight`
        const start = blockHeight % candidates.length;
        const electors = candidates.slice(start, start + count);
        if(electors.length < count) {
          electors.push(...candidates.slice(0, count - electors.length));
        }
        return {electors};
      };
    });

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    const ledgerConfiguration = mockData.ledgerConfiguration;
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      await cache.client.flushall();
      const consensusPlugin = await helpers.use('Continuity2017');
      nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
      consensusApi = consensusPlugin.api;
    });

    // get genesis record (block + meta)
    let genesisRecord;
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      const result = await nodes.alpha.blocks.getGenesis();
      genesisRecord = result.genesisBlock;
    });

    // add N - 1 more private nodes
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      for(let i = 0; i < nodeCount - 1; ++i) {
        const ledgerNode = await brLedgerNode.add(null, {
          genesisBlock: genesisRecord.block
        });
        nodes[nodeLabels[i]] = ledgerNode;
      }
    });

    // populate peers and init heads
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      for(const nodeName in nodes) {
        const ledgerNode = nodes[nodeName];
        const result = await consensusApi._peers.get(
          {ledgerNodeId: ledgerNode.id});
        peers[nodeName] = result.id;
        ledgerNode._peerId = result.id;
        heads[nodeName] = [];
      }
    });

    describe('Check Genesis Block', () => {
      it('should have the proper information', async () => {
        const blockHashes = [];
        for(const nodeName in nodes) {
          const ledgerNode = nodes[nodeName];
          const result = await ledgerNode.storage.blocks.getLatest();
          const eventBlock = result.eventBlock;
          should.exist(eventBlock.block);
          eventBlock.block.blockHeight.should.equal(0);
          eventBlock.block.event.should.be.an('array');
          // genesis config and genesis merge events
          eventBlock.block.event.should.have.length(2);
          const event = eventBlock.block.event[0];
          event.type.should.equal('WebLedgerConfigurationEvent');
          const {ledgerConfiguration} = mockData;
          event.ledgerConfiguration.should.eql(ledgerConfiguration);
          should.exist(eventBlock.meta);
          should.exist(eventBlock.block.consensusProof);
          const consensusProof = eventBlock.block.consensusProof;
          consensusProof.should.be.an('array');
          consensusProof.should.have.length(1);
          blockHashes.push(eventBlock.meta.blockHash);
        }
        blockHashes.every(h => h === blockHashes[0]).should.be.true;
      });
    });

    /*
     * 1. add new unique operations/records on nodes alpha, beta, gamma, delta
     * 2. run worker on *all* nodes
     * 3. repeat 1 and 2 until target block height is reached on all nodes
     * 4. ensure that blockHash for the target block height is identical on all
     * 5. settle the network, see notes on _settleNetwork
     * 6. ensure that the final blockHeight and blockHash is identical on all
     * 7. attempt to retrieve all records added in 1 from the `records` API
     */

    const targetBlockHeight = 10;

    describe(`${targetBlockHeight} Blocks`, () => {
      it('makes many more blocks', async function() {
        this.timeout(0);
        const nBlocks = await _nBlocks({consensusApi, targetBlockHeight});
        console.log(
          'targetBlockHashMap',
          JSON.stringify(nBlocks, null, 2));
        Object.values(nBlocks.targetBlockHashMap)
          .every(h => h === nBlocks.targetBlockHashMap.alpha)
          .should.be.true;
        // inspect outstandingMerge key
        const outstandingMergeEventsBeforePrime =
          await _inspectOutstandingMergeEvents({nodes});

        const keysBefore = await cache.client.keys('*');
        keysBefore.should.be.an('array');
        keysBefore.should.have.length.gt(0);
        await cache.client.flushdb();
        const keys = await cache.client.keys('*');
        keys.should.be.an('array');
        keys.should.have.length(0);

        for(const nodeLabel in nodes) {
          const ledgerNode = nodes[nodeLabel];
          await ledgerNode.consensus._cache.prime.primeAll({ledgerNode});
        }

        // compare outstanding merge events and block height before/after
        // prime
        const outstandingMergeEventsAfterPrime =
          await _inspectOutstandingMergeEvents({nodes});
        for(const nodeLabel in outstandingMergeEventsAfterPrime) {
          const {blockHeight, eventHashes} =
            outstandingMergeEventsAfterPrime[nodeLabel];
          blockHeight.should.equal(
            outstandingMergeEventsBeforePrime[nodeLabel].blockHeight);
          eventHashes.should.have.same.members(
            outstandingMergeEventsBeforePrime[nodeLabel].eventHashes);
        }

        await helpers.settleNetwork(
          {consensusApi, nodes: Object.values(nodes)});
        const summaryMap = await _latestBlockSummary();

        const summaries = {};
        for(const k in summaryMap) {
          const summary = summaryMap[k];
          summaries[k] = {
            blockCollection: nodes[k].storage.blocks.collection.collectionName,
            blockHeight: summary.eventBlock.block.blockHeight,
            blockHash: summary.eventBlock.meta.blockHash,
            previousBlockHash: summary.eventBlock.block.previousBlockHash
          };
        }
        console.log('Finishing block summaries:', JSON.stringify(
          summaries, null, 2));
        Object.values(summaries).forEach(b => {
          b.blockHeight.should.equal(summaries.alpha.blockHeight);
          b.blockHash.should.equal(summaries.alpha.blockHash);
        });

        const allRecordIds = [].concat(..._.values(nBlocks.recordIds));
        console.log(`Total operation count: ${allRecordIds.length}`);
        for(const recordId of allRecordIds) {
          await nodes.alpha.records.get({recordId});
          // just need to ensure that there is no NotFoundError
        }
      });
    }); // end one block
  });
});

async function _addOperations({count}) {
  const [alpha, beta, gamma, delta] = await Promise.all([
    helpers.addOperation({count, ledgerNode: nodes.alpha, opTemplate}),
    helpers.addOperation({count, ledgerNode: nodes.beta, opTemplate}),
    helpers.addOperation({count, ledgerNode: nodes.gamma, opTemplate}),
    helpers.addOperation({count, ledgerNode: nodes.delta, opTemplate})
  ]);
  return {alpha, beta, gamma, delta};
}

async function _inspectOutstandingMergeEvents({nodes}) {
  const report = {};
  for(const nodeLabel in nodes) {
    report[nodeLabel] = {};
    const ledgerNode = nodes[nodeLabel];
    const ledgerNodeId = ledgerNode.id;

    // test blockHeight
    const cacheBlockHeight = await ledgerNode.consensus._cache.blocks
      .blockHeight(ledgerNodeId);
    // get blockHeight from latestSummary
    const {eventBlock: {block: {blockHeight}}} = await ledgerNode
      .storage.blocks.getLatestSummary();
    cacheBlockHeight.should.equal(blockHeight);
    report[nodeLabel].blockHeight = blockHeight;

    const {consensus: {_cache: {cacheKey: _cacheKey}}} = ledgerNode;
    const outstandingMergeKey = _cacheKey.outstandingMerge(
      ledgerNodeId);
    const keys = await cache.client.smembers(outstandingMergeKey);
    keys.every(k => k.startsWith('ome')).should.be.true;
    if(keys.length > 0) {
      const keyPrefix = keys[0].substr(0, keys[0].lastIndexOf('|') + 1);
      const eventKeysInCache = await cache.client.keys(`${keyPrefix}*`);
      eventKeysInCache.should.have.same.members(keys);
    }

    const eventHashes = [];
    for(const key of keys) {
      eventHashes.push(key.substr(key.lastIndexOf('|') + 1));
    }
    // get events from mongodb
    const result = await ledgerNode.storage.events.collection.find({
      'meta.continuity2017.type': 'm',
      'meta.consensus': false
    }).project({
      _id: 0,
      'meta.eventHash': 1,
    }).toArray();
    const mongoEventHashes = result.map(r => r.meta.eventHash);
    eventHashes.should.have.same.members(mongoEventHashes);
    report[nodeLabel].eventHashes = eventHashes;
  }
  return report;
}

async function _latestBlockSummary() {
  const blocks = {};
  for(const nodeName in nodes) {
    const ledgerNode = nodes[nodeName];
    const result = await ledgerNode.storage.blocks.getLatestSummary();
    blocks[nodeName] = result;
  }
  return blocks;
}

async function _nBlocks({consensusApi, targetBlockHeight}) {
  const recordIds = {alpha: [], beta: [], gamma: [], delta: []};
  const targetBlockHashMap = {};
  while(Object.keys(targetBlockHashMap).length !==
    Object.keys(nodes).length) {
    const count = 1;
    const operations = await _addOperations(count);

    // record the IDs for the records that were just added
    for(const n of ['alpha', 'beta', 'gamma', 'delta']) {
      for(const opHash of Object.keys(operations[n])) {
        recordIds[n].push(operations[n][opHash].record.id);
      }
    }

    // in this test `nodes` is an object that needs to be converted to
    // an array for the helper
    await helpers.runWorkerCycle(
      {consensusApi, nodes: Object.values(nodes), series: false});
    // report
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const result = await ledgerNode.storage.blocks.getLatestSummary();
      const {block} = result.eventBlock;
      if(block.blockHeight >= targetBlockHeight) {
        const result = await ledgerNode.storage.blocks.getByHeight(
          targetBlockHeight);
        targetBlockHashMap[key] = result.meta.blockHash;
      }
    }
  }
  return {recordIds, targetBlockHashMap};
}
