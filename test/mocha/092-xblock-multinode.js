/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');
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

describe('X Block Test', () => {
  before(async function() {
    this.timeout(TEST_TIMEOUT);
    await helpers.prepareDatabase();
  });

  const nodeCount = 6;
  describe(`Consensus with ${nodeCount} Nodes`, () => {
    // used to overload and replace the getBlockWitnesses algorithm
    let witnessSelectionApi;
    let originalGetBlockWitnesses;

    // override witness selection to force cycling and 3f+1
    before(() => {
      witnessSelectionApi = brLedgerNode.use('WitnessPoolWitnessSelection');
      originalGetBlockWitnesses = witnessSelectionApi.api.getBlockWitnesses;
      witnessSelectionApi.api.getBlockWitnesses = async ({blockHeight}) => {
        const candidates = [];
        for(const p of Object.keys(peers)) {
          candidates.push(peers[p]);
        }
        const f = Math.floor((nodeCount - 1) / 3);
        const count = 3 * f + 1;
        // cycle witnesses deterministically using `blockHeight`
        const start = blockHeight % candidates.length;
        const witnesses = candidates.slice(start, start + count);
        if(witnesses.length < count) {
          witnesses.push(...candidates.slice(0, count - witnesses.length));
        }
        return {witnesses};
      };
    });

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
        nodes[nodeLabels[i]] = ledgerNodes[i];
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
        const remotePeer = {
          id: peers.alpha,
          url: consensusApi._localPeers.getLocalPeerUrl(
            {peerId: peers.alpha})
        };
        await consensusApi._peers.optionallyAdd(
          {ledgerNode, remotePeer});
        // add new peer to genesis node
        await consensusApi._peers.optionallyAdd({
          ledgerNode: nodes.alpha,
          remotePeer: {
            id: ledgerNode._peerId,
            url: consensusApi._localPeers.getLocalPeerUrl(
              {peerId: ledgerNode._peerId})
          }
        });
      }
    });

    // put the getBlockWitness API back to the original
    after(async function() {
      witnessSelectionApi.api.getBlockWitnesses = originalGetBlockWitnesses;
    });

    describe('Check Genesis Block', () => {
      it('should have the proper information', async function() {
        const blockHashes = [];
        const allLatest = await Promise.all(Object.values(nodes).map(
          ledgerNode => ledgerNode.storage.blocks.getLatest()));
        for(const {eventBlock} of allLatest) {
          should.exist(eventBlock.block);
          eventBlock.block.blockHeight.should.equal(0);
          eventBlock.block.event.should.be.an('array');
          eventBlock.block.event.should.have.length(2);
          const event = eventBlock.block.event[0];
          // TODO: signature is dynamic... needs a better check
          delete event.signature;
          delete event.proof;
          event.ledgerConfiguration.should.deep.equal(ledgerConfiguration);
          should.exist(eventBlock.meta);
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

    const targetBlockHeight = 50;

    describe(`${targetBlockHeight} Blocks`, () => {
      it('makes many more blocks', async function() {
        this.timeout(0);

        // create N blocks
        const nBlocks = await _nBlocks({consensusApi, targetBlockHeight});
        console.log('nBlocks output', JSON.stringify(nBlocks, null, 2));
        Object.values(nBlocks.targetBlockHashMap)
          .every(h => h === nBlocks.targetBlockHashMap.alpha).should.be.true;

        // wait for network to settle
        const allRecordIds = [].concat(...Object.values(nBlocks.recordIds));
        await helpers.settleNetwork(
          {consensusApi, nodes: Object.values(nodes), recordIds: allRecordIds});

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
  const recordIds = {alpha: [], beta: [], gamma: [], delta: []};
  const targetBlockHashMap = {};
  while(Object.keys(targetBlockHashMap).length !== Object.keys(nodes).length) {
    const count = 1;
    const operations = await _addOperations({count});
    // record the IDs for the records that were just added
    for(const n of ['alpha', 'beta', 'gamma', 'delta']) {
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
  const [alpha, beta, gamma, delta] = await Promise.all([
    helpers.addOperation({count, ledgerNode: nodes.alpha, opTemplate}),
    helpers.addOperation({count, ledgerNode: nodes.beta, opTemplate}),
    helpers.addOperation({count, ledgerNode: nodes.gamma, opTemplate}),
    helpers.addOperation({count, ledgerNode: nodes.delta, opTemplate})
  ]);
  return {alpha, beta, gamma, delta};
}
