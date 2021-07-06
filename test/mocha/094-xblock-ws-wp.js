/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');

const TEST_TIMEOUT = 300000;

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

const opTemplate = mockData.operations.alpha;

// NOTE: all these may not be used
const nodeLabels = [
  'alpha', 'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota'
];
const nodes = {};
const peers = {};
const nodeCount = 7;

describe('X Block Test with witness pool and non-witnesses', () => {
  before(async function() {
    this.timeout(TEST_TIMEOUT);
    await helpers.prepareDatabase();
  });

  describe(`Consensus with ${nodeCount} Nodes`, () => {
    // get consensus plugin and create genesis ledger node
    let consensusApi;
    const ledgerConfiguration = mockData.ledgerConfiguration;
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      await helpers.flushCache();
      ({api: consensusApi} = await helpers.use('Continuity2017'));
      nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    });

    // get genesis record (block + meta)
    let genesisRecord;
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      ({genesisBlock: genesisRecord} = await nodes.alpha.blocks.getGenesis());
    });

    // add N - 1 more nodes
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      const promises = [];
      for(let i = 0; i < nodeCount - 1; ++i) {
        promises.push(brLedgerNode.add(
          null, {genesisBlock: genesisRecord.block}));
      }
      const ledgerNodes = await Promise.all(promises);
      for(let i = 0; i < nodeCount - 1; ++i) {
        nodes[nodeLabels[i + 1]] = ledgerNodes[i];
      }
    });

    // populate peers
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      for(const key in nodes) {
        const ledgerNode = nodes[key];
        const peerId = await consensusApi._localPeers.getPeerId(
          {ledgerNodeId: ledgerNode.id});
        peers[key] = peerId;
        ledgerNode._peerId = peerId;
        if(key === 'alpha') {
          // skip genesis peer
          continue;
        }
        // add genesis peer to the peer's peers collection
        // FIXME: use proper URL do not just repeat ID
        const remotePeer = {id: peers.alpha, url: peers.alpha};
        await consensusApi._peers.optionallyAdd(
          {ledgerNode, remotePeer});
        // add new peer to genesis node
        await consensusApi._peers.optionallyAdd({
          ledgerNode: nodes.alpha,
          // FIXME: use proper URL do not just repeat ID
          remotePeer: {id: ledgerNode._peerId, url: ledgerNode._peerId}
        });
      }
    });

    describe('Witness Pool', () => {
      it('add a witness pool document with one witness', async function() {
        const operation = bedrock.util.clone(
          mockData.operations.witnessPoolOperation);
        const ledgerNode = nodes.alpha;
        const {record} = operation;
        operation.creator = ledgerNode._peerId;
        record.maximumWitnessCount = 1;
        record.primaryWitnessCandidate = [ledgerNode._peerId];
        record.secondaryWitnessCandidate = [];

        // add the witness pool document and run consensus
        await ledgerNode.operations.add({operation, ledgerNode});
        await helpers.runWorkerCycle(
          {consensusApi, nodes: Object.values(nodes), targetCyclesPerNode: 10});

        // ensure that the latest witness pool is the information we wrote
        let latestWitnessPool;
        try {
          latestWitnessPool = await ledgerNode.records.get({
            recordId: record.id});
        } catch(e) {
          assertNoError(e);
        }
        latestWitnessPool.record.id.should.equal(operation.record.id);
        latestWitnessPool.record.maximumWitnessCount.should.equal(1);
        latestWitnessPool.record.primaryWitnessCandidate[0].should.equal(
          ledgerNode._peerId);
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

    describe.skip(`${targetBlockHeight} Blocks`, () => {
      it('makes many more blocks', async function() {
        this.timeout(0);

        // create N blocks
        const nBlocks = await _nBlocks({consensusApi, targetBlockHeight});
        console.log('nBlocks output', JSON.stringify(nBlocks, null, 2));
        Object.values(nBlocks.targetBlockHashMap)
          .every(h => h === nBlocks.targetBlockHashMap.alpha).should.be.true;

        // wait for network to settle
        await helpers.settleNetwork(
          {consensusApi, nodes: Object.values(nodes)});

        // get all block summaries
        const summaries = {};
        for(const key in nodes) {
          const ledgerNode = nodes[key];
          const result = await ledgerNode.storage.blocks.getLatestSummary();
          summaries[key] = {
            blockCollection:
              ledgerNode.storage.blocks.collection.collectionName,
            blockHeight: result.eventBlock.block.blockHeight,
            blockHash: result.eventBlock.meta.blockHash,
            previousBlockHash: result.eventBlock.block.previousBlockHash
          };
        }
        console.log(
          'Finishing block summaries:', JSON.stringify(summaries, null, 2));
        Object.values(summaries).forEach(b => {
          b.blockHeight.should.equal(summaries.alpha.blockHeight);
          b.blockHash.should.equal(summaries.alpha.blockHash);
        });

        // check all records were created
        const allRecordIds = [].concat(...Object.values(nBlocks.recordIds));
        console.log(`Total operation count: ${allRecordIds.length}`);
        for(const recordId of allRecordIds) {
          // just need to ensure that there is no NotFoundError
          try {
            await nodes.alpha.records.get({recordId});
          } catch(e) {
            assertNoError(e);
          }
        }
      });
    });
  });
});

async function _nBlocks({consensusApi, targetBlockHeight}) {
  const recordIds = {};
  for(let i = 0; i < nodeCount; ++i) {
    recordIds[nodeLabels[i]] = [];
  }
  const targetBlockHashMap = {};
  while(Object.keys(targetBlockHashMap).length !== Object.keys(nodes).length) {
    const count = 1;
    const operations = await _addOperations({count});
    // record the IDs for the records that were just added
    for(let i = 0; i < nodeCount; ++i) {
      const n = nodeLabels[i];
      for(const opHash of Object.keys(operations[n])) {
        recordIds[n].push(operations[n][opHash].record.id);
      }
    }
    // run worker cycle
    // in this test `nodes` is an object that needs to be converted to
    // an array for the helper
    await helpers.runWorkerCycle({
      consensusApi, nodes: Object.values(nodes), series: false,
      targetCyclesPerNode: 10
    });
    // generate report
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

async function _addOperations({count}) {
  const promises = [];
  for(let i = 0; i < nodeCount; ++i) {
    const ledgerNode = nodes[nodeLabels[i]];
    promises.push(helpers.addOperation({count, ledgerNode, opTemplate}));
  }
  const results = await Promise.all(promises);
  const resultMap = {};
  for(let i = 0; i < results.length; ++i) {
    resultMap[nodeLabels[i]] = results[i];
  }
  return resultMap;
}
