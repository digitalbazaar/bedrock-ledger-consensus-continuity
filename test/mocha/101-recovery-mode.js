/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const {promisify} = require('util');

const helperUse = promisify(helpers.use);
const helperNBlock = promisify(helpers.nBlocks);
const helperSettleNetwork = promisify(helpers.settleNetwork);

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

const opTemplate = mockData.operations.alpha;

const TEST_TIMEOUT = 600000;

// NOTE: alpha is assigned manually
// NOTE: all these may not be used
const nodeLabels = [
  'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota'
];
const nodes = {};
const disabledNodes = {};
const peers = {};
const heads = {};

describe('Recovery mode simulation', () => {
  before(function(done) {
    this.timeout(TEST_TIMEOUT);
    helpers.prepareDatabase(mockData, done);
  });

  // override elector selection helpers for tests
  before(() => {
    const electorSelectionApi = brLedgerNode.use(
      'MostRecentParticipantsWithRecovery');

    let savedElectors;
    electorSelectionApi.api._computeElectors = async () => {
      // electors only need to be computed once
      if(savedElectors) {
        return savedElectors;
      }
      const electors = [];
      for(const p of Object.keys(peers)) {
        electors.push({id: peers[p]});
      }
      savedElectors = electors;
      return electors;
    };

    // always return alpha as the sole elector
    electorSelectionApi.api._computeElectorsForRecoveryMode = () => {
      return [{id: peers.alpha}];
    };

    // since the recoveryElectors in this test are based on `nodes` which
    // changes throughout the tests as nodes are removed and added, the
    // value computed at each blockHeight is stored for the benefit of nodes
    // that must catch up as they are re-enabled.
    // const recoveryElectorsByBlockHeight = new Map();
    electorSelectionApi.api._computeRecoveryElectors =
      ({blockHeight, electors, f}) => {
        // const activePeers = new Set();
        let recoveryElectors = [];
        for(const n of ['alpha', 'beta', 'gamma', 'delta']) {
          // activePeers.add(peers[n]);
          recoveryElectors.push({id: peers[n]});
        }

        // let r = recoveryElectorsByBlockHeight.get(blockHeight);
        // if(r) {
        //   return r;
        // }

        if(electors.length === 1) {
          // recoveryElectorsByBlockHeight.set(blockHeight, []);
          return [];
        }

        // NOTE: computing recovery electors based on `electors` does not work
        // because the elector selection algorithm may have chosen multiple
        // electors that are no longer active. This results in the intersection
        // of activePeers and electors being < f + 1 which results in [] being
        // incorrectly returned.
        // Below is an example of what *not* to do
        // const recoveryElectors = electors.filter(e => activePeers.has(e.id))
        //   .slice(0, f + 1);

        // recoveryElectors = recoveryElectors
        //   .sort((a, b) => a.id.localeCompare(b.id)).slice(0, f + 1);

        recoveryElectors = recoveryElectors.slice(0, f + 1);

        const r = recoveryElectors.length < f + 1 ? [] : recoveryElectors;
        // recoveryElectorsByBlockHeight.set(blockHeight, r);
        return r;
      };
    // the return value here gets multiplied by 10
    electorSelectionApi.api._computeRecoveryMinimumMergeEvents = () => 2;
  });

  const nodeCount = 7;
  describe(`Consensus with ${nodeCount} Nodes`, () => {

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    const mockIdentity = mockData.identities.regularUser;
    const ledgerConfiguration = mockData.ledgerConfigurationRecovery;
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      await cache.client.flushall();

      const consensusPlugin = await helperUse('Continuity2017');
      const ledgerNode = await brLedgerNode.add(null, {ledgerConfiguration});
      nodes.alpha = ledgerNode;
      consensusApi = consensusPlugin.api;
    });

    // get genesis record (block + meta)
    let genesisRecord;
    before(async () => {
      ({genesisBlock: genesisRecord} = await nodes.alpha.blocks.getGenesis());
    });

    // add N - 1 more private nodes
    before(async function() {
      this.timeout(TEST_TIMEOUT);
      for(let i = 0; i < nodeCount - 1; ++i) {
        nodes[nodeLabels[i]] = await brLedgerNode.add(null, {
          genesisBlock: genesisRecord.block,
          owner: mockIdentity.identity.id
        });
      }
    });

    // populate peers and init heads
    before(async () => {
      for(const n in nodes) {
        const ledgerNode = nodes[n];
        const ledgerNodeId = ledgerNode.id;
        const voter = await consensusApi._voters.get({ledgerNodeId});
        peers[n] = voter.id;
        ledgerNode._peerId = voter.id;
        heads[n] = [];
      }
    });

    describe('Check Genesis Block', () => {
      it('should have the proper information', async () => {

        for(const n of Object.keys(nodes)) {
          console.log(`----- ${n} ----`);
          console.log(`Storage ${nodes[n].storage.events.collection.s.name}`);
          console.log(`PeerId ${peers[n]}`);
        }

        const blockHashes = [];
        for(const n in nodes) {
          const ledgerNode = nodes[n];
          const {eventBlock} = await ledgerNode.storage.blocks.getLatest();
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
          should.exist(eventBlock.block.consensusProof);
          const consensusProof = eventBlock.block.consensusProof;
          consensusProof.should.be.an('array');
          consensusProof.should.have.length(1);
          // FIXME: make assertions about the contents of consensusProof
          blockHashes.push(eventBlock.meta.blockHash);
        }
        blockHashes.every(h => h === blockHashes[0]).should.be.true;
      });
    });

    /*
     * 1. add new unique operations/records on *all* nodes
     * 2. run worker on *all* nodes
     * 3. repeat 1 and 2 until target block height is reached on all nodes
     * 4. ensure that blockHash for the target block height is identical on all
     * 5. settle the network, see notes on _settleNetwork
     * 6. ensure that the final blockHeight and blockHash is identical on all
     * 7. attempt to retrieve all records added in 1 from the `records` API
     *
     * Test recovery mode with four active nodes:
     * 8. Reduce `nodes` from seven to four nodes: alpha, beta, gamma, delta
     * 9. add new unique operations/records on nodes alpha, beta, gamma, delta
     * 10. run worker on all remaining nodes
     * 11. repeat 9 and 10 until target block height is reached on all nodes
     *     it is anticipated that the network should go into recovery mode
     *     under these conditions.
     * 12. attempt to retrieve all records added in 9 from the `records` API
     *
     * Test recovery mode with three active nodes:
     * 13. Reduce `nodes` from four to three nodes: alpha, beta, gamma
     * 14. add new unique operations/records on nodes alpha, beta, gamma
     * 15. run worker on all remaining nodes
     * 16. repeat 9 and 10 until target block height is reached on all nodes
     *     it is anticipated that the network should go into recovery mode
     *     under these conditions.
     * 17. attempt to retrieve all records added in 14 from the `records` API
     *
     * Restore network to normal operation with all 7 nodes
     * 18. Add all previously disabled nodes back into `nodes`
     * 19. Settle the network to allow the previously disabled nodes to catch up
     * 20. add new unique operations/records on *all* nodes
     * 21. run worker on *all* nodes
     * 22. repeat 20 and 21 until target block height is reached on all nodes
     * 23. ensure that blockHash for the target block height is identical on all
     * 24. settle the network, see notes on _settleNetwork
     * 25. ensure that the final blockHeight and blockHash is identical on all
     * 26. attempt to retrieve all records added in 1 from the `records` API
     */

    const targetBlockHeight = 5;
    let stageOneBlockHeight;
    describe(`${targetBlockHeight} Blocks`, () => {
      it(`makes ${targetBlockHeight} blocks with all nodes`, async function() {
        this.timeout(TEST_TIMEOUT);
        const result = await helperNBlock({
          consensusApi, nodes, opTemplate, operationOnWorkCycle: 'all',
          targetBlockHeight
        });
        console.log(
          'targetBlockHashMap',
          JSON.stringify(result, null, 2));
        _.values(result.targetBlockHashMap)
          .every(h => h === result.targetBlockHashMap.alpha)
          .should.be.true;

        await helperSettleNetwork({consensusApi, nodes: _.values(nodes)});

        const blockSummary = await _latestBlockSummary();

        const summaries = _createBlockSummaries(blockSummary);
        stageOneBlockHeight = blockSummary[
          Object.keys(blockSummary)[0]].eventBlock
          .block.blockHeight;

        console.log('Finishing block summaries:', JSON.stringify(
          summaries, null, 2));
        _.values(summaries).forEach(b => {
          b.blockHeight.should.equal(summaries.alpha.blockHeight);
          b.blockHash.should.equal(summaries.alpha.blockHash);
        });
        await _testRecords(result.recordIds);
      });
    }); // end one block

    const recoveryBlocksFourNodes = 5;
    let stageTwoBlockHeight;
    describe(`${recoveryBlocksFourNodes} Recovery Blocks`, () => {
      it(`makes ${recoveryBlocksFourNodes} blocks w/4 nodes`, async function() {
        this.timeout(TEST_TIMEOUT);

        const newTargetBlockHeight = stageOneBlockHeight +
          recoveryBlocksFourNodes;

        // remove 3 out of 7 nodes
        _disableNodes(['epsilon', 'zeta', 'eta']);
        const result = await helperNBlock({
          consensusApi, nodes, opTemplate, operationOnWorkCycle: 'all',
          targetBlockHeight: newTargetBlockHeight
        });
        console.log(
          'targetBlockHashMap', JSON.stringify(result, null, 2));
        const firstNodeLabel = Object.keys(result.targetBlockHashMap)[0];
        _.values(result.targetBlockHashMap)
          .every(h => h === result.targetBlockHashMap[firstNodeLabel])
          .should.be.true;

        await helperSettleNetwork({consensusApi, nodes: _.values(nodes)});

        const blockSummary = await _latestBlockSummary();

        stageTwoBlockHeight = blockSummary[
          Object.keys(blockSummary)[0]].eventBlock
          .block.blockHeight;

        const summaries = _createBlockSummaries(blockSummary);
        console.log('Finishing block summaries:', JSON.stringify(
          summaries, null, 2));
        _.values(summaries).forEach(b => {
          b.blockHeight.should.equal(summaries.alpha.blockHeight);
          b.blockHash.should.equal(summaries.alpha.blockHash);
        });

        await _testRecords(result.recordIds);
      });
    }); // end recovery blocks four nodes

    const recoveryBlocksThreeNodes = 5;
    let stageThreeBlockHeight;
    describe(`${recoveryBlocksThreeNodes} Recovery Blocks`, () => {
      it(`make ${recoveryBlocksThreeNodes} blocks w/3 nodes`, async function() {
        this.timeout(TEST_TIMEOUT);

        const newTargetBlockHeight = stageTwoBlockHeight +
          recoveryBlocksThreeNodes;

        // remove 1 out of the remaining 4 nodes
        _disableNodes(['delta']);

        const result = await helperNBlock({
          consensusApi, nodes, opTemplate, operationOnWorkCycle: 'all',
          targetBlockHeight: newTargetBlockHeight
        });
        console.log(
          'targetBlockHashMap', JSON.stringify(result, null, 2));
        const firstNodeLabel = Object.keys(result.targetBlockHashMap)[0];
        _.values(result.targetBlockHashMap)
          .every(h => h === result.targetBlockHashMap[firstNodeLabel])
          .should.be.true;

        await helperSettleNetwork({consensusApi, nodes: _.values(nodes)});

        const blockSummary = await _latestBlockSummary();
        stageThreeBlockHeight = blockSummary[
          Object.keys(blockSummary)[0]].eventBlock
          .block.blockHeight;

        const summaries = _createBlockSummaries(blockSummary);
        console.log('Finishing block summaries:', JSON.stringify(
          summaries, null, 2));
        _.values(summaries).forEach(b => {
          b.blockHeight.should.equal(summaries.alpha.blockHeight);
          b.blockHash.should.equal(summaries.alpha.blockHash);
        });

        await _testRecords(result.recordIds);
      });
    }); // end recovery blocks three nodes

    let stageFourBlockHeight;
    describe('Enable formerly disable nodes', () => {
      it('let disabled nodes catch up', async function() {
        this.timeout(TEST_TIMEOUT);

        // enable all previously disabled nodes, back to seven nodes
        _enableNodes(['delta', 'epsilon', 'zeta', 'eta']);

        // let the previously disabled nodes catch up
        await _countEvents();

        await helperSettleNetwork({consensusApi, nodes: _.values(nodes)});

        const blockSummary = await _latestBlockSummary();
        stageFourBlockHeight = blockSummary[
          Object.keys(blockSummary)[0]].eventBlock
          .block.blockHeight;

        const summaries = _createBlockSummaries(blockSummary);
        console.log('Finishing block summaries:', JSON.stringify(
          summaries, null, 2));
        _.values(summaries).forEach(b => {
          b.blockHeight.should.equal(summaries.alpha.blockHeight);
          b.blockHash.should.equal(summaries.alpha.blockHash);
        });

        await _countEvents();
      });
    });

    const regularBlocksSevenNodes = 10;
    let stageFiveBlockHeight;
    describe(`${regularBlocksSevenNodes} Regular Blocks`, () => {
      it(`makes ${regularBlocksSevenNodes} blocks w/7 nodes`, async function() {
        this.timeout(TEST_TIMEOUT);

        const newTargetBlockHeight = stageFourBlockHeight +
          regularBlocksSevenNodes;

        const result = await helperNBlock({
          consensusApi, nodes, opTemplate, operationOnWorkCycle: 'all',
          targetBlockHeight: newTargetBlockHeight
        });
        console.log(
          'targetBlockHashMap', JSON.stringify(result, null, 2));
        const firstNodeLabel = Object.keys(result.targetBlockHashMap)[0];
        _.values(result.targetBlockHashMap)
          .every(h => h === result.targetBlockHashMap[firstNodeLabel])
          .should.be.true;

        await helperSettleNetwork({consensusApi, nodes: _.values(nodes)});

        const blockSummary = await _latestBlockSummary();
        stageFiveBlockHeight = blockSummary[
          Object.keys(blockSummary)[0]].eventBlock
          .block.blockHeight;

        const summaries = _createBlockSummaries(blockSummary);
        console.log('Finishing block summaries:', JSON.stringify(
          summaries, null, 2));
        _.values(summaries).forEach(b => {
          b.blockHeight.should.equal(summaries.alpha.blockHeight);
          b.blockHash.should.equal(summaries.alpha.blockHash);
        });

        await _testRecords(result.recordIds);

        // test participants in last block
        const ledgerNode = nodes.alpha;
        const {getParticipants} = ledgerNode.consensus._blocks;
        const p = await getParticipants(
          {blockHeight: stageFiveBlockHeight, ledgerNode});

        // proof on the last block created should involve all 7 nodes
        p.consensusProofPeers.should.have.length(7);
        p.consensusProofPeers.should.have.same.members(
          _.values(peers));
      });
    }); // end regular blocks seven nodes
  });
});

async function _countEvents() {
  const result = await nodes.alpha.storage.events.collection.aggregate([
    {$match: {'meta.continuity2017.type': 'm'}},
    {$group: {
      _id: '$meta.continuity2017.creator',
      total: {$sum: 1}
    }}
  ]).toArray();
  console.log('EVENTS', JSON.stringify(result, null, 2));
}

function _createBlockSummaries(blockSummary) {
  const summaries = {};
  Object.keys(blockSummary).forEach(k => {
    summaries[k] = {
      blockCollection: nodes[k].storage.blocks.collection.s.name,
      blockHeight: blockSummary[k].eventBlock.block.blockHeight,
      blockHash: blockSummary[k].eventBlock.meta.blockHash,
      previousBlockHash: blockSummary[k].eventBlock.block
        .previousBlockHash,
    };
  });
  return summaries;
}

function _disableNodes(nodeLabels) {
  for(const node of nodeLabels) {
    disabledNodes[node] = nodes[node];
    delete nodes[node];
  }
}

function _enableNodes(nodeLabels) {
  for(const node of nodeLabels) {
    nodes[node] = disabledNodes[node];
    delete disabledNodes[node];
  }
}

async function _latestBlockSummary() {
  const blocks = {};
  for(const n in nodes) {
    const ledgerNode = nodes[n];
    blocks[n] = await ledgerNode.storage.blocks.getLatestSummary();
  }
  return blocks;
}

async function _testRecords(recordIds) {
  const allRecordIds = [].concat(..._.values(recordIds));
  console.log(`Total operation count: ${allRecordIds.length}`);
  // all nodes should have all operations
  // just need to ensure that there is no NotFoundError
  for(const n in nodes) {
    await Promise.all(allRecordIds.map(recordId =>
      nodes[n].records.get({recordId})));
  }
}
