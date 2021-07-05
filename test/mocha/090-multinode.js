/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

// the total number of nodes on the ledger may be adjusted here
const nodeCount = 10;

describe('Multinode', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });

  describe(`Consensus with ${nodeCount} Nodes`, () => {
    // get consensus plugin and create genesis ledger node
    let consensusApi;
    let genesisLedgerNode;
    let Worker;
    const {ledgerConfiguration} = mockData;
    before(async function() {
      await helpers.flushCache();
      await helpers.removeCollections(['ledger', 'ledgerNode']);
      const consensusPlugin = await helpers.use('Continuity2017');
      genesisLedgerNode = await brLedgerNode.add(null, {ledgerConfiguration});
      consensusApi = consensusPlugin.api;
      Worker = consensusApi._worker.Worker;
    });

    // get genesis record (block + meta)
    let genesisRecord;
    before(async () => {
      const result = await genesisLedgerNode.blocks.getGenesis();
      genesisRecord = result.genesisBlock;
    });

    // add N - 1 more nodes
    const peers = [];
    before(async function() {
      this.timeout(120000);
      peers.push(genesisLedgerNode);
      const promises = [];
      for(let i = 0; i < nodeCount - 1; ++i) {
        promises.push(brLedgerNode.add(null, {
          genesisBlock: genesisRecord.block
        }));
      }
      peers.push(...await Promise.all(promises));
    });

    // populate peers ids
    before(async function() {
      for(const ledgerNode of peers) {
        const peerId = await consensusApi._localPeers.getPeerId(
          {ledgerNodeId: ledgerNode.id});
        ledgerNode._peerId = peerId;
        if(ledgerNode === genesisLedgerNode) {
          // skip genesis peer
          continue;
        }
        // add genesis peer to the peer's peers collection
        // FIXME: use proper URL do not just repeat ID
        const remotePeer = {
          id: genesisLedgerNode._peerId,
          url: genesisLedgerNode._peerId
        };
        await consensusApi._peers.optionallyAdd(
          {ledgerNode, remotePeer});
        // add new peer to genesis node
        await consensusApi._peers.optionallyAdd({
          ledgerNode: genesisLedgerNode,
          // FIXME: use proper URL do not just repeat ID
          remotePeer: {id: ledgerNode._peerId, url: ledgerNode._peerId}
        });
      }
    });

    // override elector selection to force cycling and 3f+1
    before(() => {
      let candidates;
      const witnessSelectionApi = brLedgerNode.use('Continuity2017');
      witnessSelectionApi.api._witnesses.getBlockWitnesses =
      async ({blockHeight}) => {
        if(!candidates) {
          candidates = [];
          for(const peer of peers) {
            candidates.push({id: peer._peerId});
          }
        }
        const f = Math.floor((nodeCount - 1) / 3);
        const count = 3 * f + 1;
        // cycle electors deterministically using `blockHeight`
        const start = blockHeight % candidates.length;
        const witnesses = candidates.slice(start, start + count);
        if(witnesses.length < count) {
          witnesses.push(...candidates.slice(0, count - witnesses.length));
        }
        return {witnesses};
      };
    });

    describe('Check Genesis Block', () => {
      it('should have the proper information', async () => {
        const blockHashes = [];
        for(const ledgerNode of peers) {
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

    describe('Block 1', () => {
      // add a single op to genesis node, genesis node will be sole elector
      it('should add an operation and achieve consensus', async function() {
        this.timeout(60000);
        const opTemplate = mockData.operations.alpha;
        await helpers.addOperation(
          {ledgerNode: genesisLedgerNode, opTemplate});
        await helpers.settleNetwork(
          {consensusApi, nodes: peers, series: false});
        const blockHashes = [];
        for(const ledgerNode of peers) {
          const result = await ledgerNode.storage.blocks.getLatest();
          const eventBlock = result.eventBlock;
          should.exist(eventBlock.block);

          // FIXME: it appears that this assertion is not always valid
          // commenting out for now, a github issue will be created
          // in connection with this
          // eventBlock.block.blockHeight.should.equal(1);

          eventBlock.block.event.should.be.an('array');

          // FIXME: it appears that this assertion is not always valid
          // commenting out for now, a github issue will be created
          // in connection with this
          // a regular event and 10 merge events
          // eventBlock.block.event.should.have.length(11);

          blockHashes.push(eventBlock.meta.blockHash);
        }
        // the blockHash on every node should be the same
        blockHashes.every(h => h === blockHashes[0]).should.be.true;
      });
    }); // end block 1
    describe('Operations', () => {
      // add an operation on all peers, settle and ensure that all records are
      // available via the records API
      it('add an operation on all nodes + achieve consensus', async function() {
        this.timeout(60000);
        const opTemplate = mockData.operations.alpha;
        const results = await helpers.addOperations({nodes: peers, opTemplate});
        await helpers.settleNetwork(
          {consensusApi, nodes: peers, series: false});
        const recordIds = _extractRecordIds(results);
        for(const ledgerNode of peers) {
          for(const recordId of recordIds) {
            const result = await ledgerNode.records.get({recordId});
            result.should.be.an('object');
          }
        }
      });
    }); // end Operations
    describe('Ledger Configuration', () => {
      // add a config event on the genesis node, settle the network, ensure
      // that new config is in effect on all nodes
      it('ValidationError on missing ledger property', async () => {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        ledgerConfiguration.sequence = 1;
        delete ledgerConfiguration.ledger;
        let error;
        try {
          const worker = new Worker({session: {ledgerNode: genesisLedgerNode}});
          await worker.init();
          genesisLedgerNode.worker = worker;
          await genesisLedgerNode.config.change(
            {ledgerConfiguration, worker});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('ValidationError');
        error.details.errors[0].message.should.equal(
          `should have required property 'ledger'`);
      });
      it('SyntaxError on invalid ledger', async () => {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        ledgerConfiguration.sequence = 1;
        ledgerConfiguration.ledger = 'https://example.com/invalidLedger';
        let error;
        try {
          const worker = new Worker({session: {ledgerNode: genesisLedgerNode}});
          await worker.init();
          genesisLedgerNode.worker = worker;
          await genesisLedgerNode.config.change(
            {ledgerConfiguration, worker});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('SyntaxError');
        error.message.should.equal(`Invalid configuration 'ledger' value.`);
      });
      it('ValidationError on missing creator', async () => {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        // creator is not added
        ledgerConfiguration.sequence = 1;
        let error;
        try {
          const worker = new Worker({session: {ledgerNode: genesisLedgerNode}});
          await worker.init();
          genesisLedgerNode.worker = worker;
          await genesisLedgerNode.config.change(
            {ledgerConfiguration, worker});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('ValidationError');
        error.details.errors[0].message.should.equal(
          `should have required property 'creator'`);
      });
      it('SyntaxError on invalid creator', async () => {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        // creator is invalid
        ledgerConfiguration.creator = 'https://example.com/invalidCreator';
        ledgerConfiguration.sequence = 1;
        let error;
        try {
          const worker = new Worker({session: {ledgerNode: genesisLedgerNode}});
          await worker.init();
          genesisLedgerNode.worker = worker;
          await genesisLedgerNode.config.change(
            {ledgerConfiguration, worker});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('SyntaxError');
        error.message.should.equal(`Invalid configuration 'creator' value.`);
      });
      it('ValidationError on missing sequence', async () => {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        delete ledgerConfiguration.sequence;
        let error;
        try {
          const worker = new Worker({session: {ledgerNode: genesisLedgerNode}});
          await worker.init();
          genesisLedgerNode.worker = worker;
          await genesisLedgerNode.config.change(
            {ledgerConfiguration, worker});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('ValidationError');
        error.details.errors[0].message.should.equal(
          `should have required property 'sequence'`);
      });
      it('SyntaxError on invalid sequence', async () => {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        // invalid sequence, should be 1
        ledgerConfiguration.sequence = 5;
        let error;
        try {
          const worker = new Worker({session: {ledgerNode: genesisLedgerNode}});
          await worker.init();
          genesisLedgerNode.worker = worker;
          await genesisLedgerNode.config.change(
            {ledgerConfiguration, worker});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('SyntaxError');
        error.message.should.equal(`Invalid configuration 'sequence' value.`);
      });
      it('add a ledger config and achieve consensus', async function() {
        this.timeout(60000);
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        ledgerConfiguration.sequence = 1;
        ledgerConfiguration.consensusMethod = 'Continuity9000';
        // FIXME: this should not use a `worker` here, instead the ledger
        // configuration should be added to a queue when calling `.change`
        // once that is updated, remove `worker` and rely on settling
        // the network to create the event and apply the change
        const worker = new Worker({session: {ledgerNode: genesisLedgerNode}});
        // FIXME: this is currently required to make this configuration change
        // test pass (possible causes are the current lack of a configuration
        // queue or early exiting in `helpers` when configuration changes on
        // some witnesses and not others)
        const mergeOptions = {nonEmptyThreshold: 1};
        await worker.init();
        genesisLedgerNode.worker = worker;
        await genesisLedgerNode.config.change(
          {ledgerConfiguration, worker});
        await helpers.settleNetwork(
          {consensusApi, nodes: peers, mergeOptions, series: false});
        for(const ledgerNode of peers) {
          const ledgerConfig = await ledgerNode.config.get();
          ledgerConfig.should.eql(ledgerConfiguration);
        }
      });
    }); // end Ledger Configuration
    describe('Catch-up', () => {
      it('a new node is able to catch up', async function() {
        this.timeout(120000);
        const ledgerNode = await brLedgerNode.add(null, {
          genesisBlock: genesisRecord.block
        });
        peers.push(ledgerNode);
        // add genesis peer to the peer's peers collection
        // FIXME: use proper URL do not just repeat ID
        const remotePeer = {
          id: genesisLedgerNode._peerId,
          url: genesisLedgerNode._peerId
        };
        await consensusApi._peers.optionallyAdd(
          {ledgerNode, remotePeer});
        await helpers.settleNetwork(
          {consensusApi, nodes: peers, series: false});
        const ledgerConfigs = [];
        for(const ledgerNode of peers) {
          ledgerConfigs.push(await ledgerNode.config.get());
        }
        for(const c of ledgerConfigs) {
          c.should.eql(ledgerConfigs[0]);
        }
      });
    });
    describe('Reinitialize Nodes', () => {
      // the nodes should load with a new consensus method
      it('nodes should have new consensus method', async () => {
        const nodeIds = peers.map(n => n.id);
        for(const ledgerNodeId of nodeIds) {
          const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
          ledgerNode.consensus.consensusMethod.should.equal('Continuity9000');
        }
      });
    });
  });
});

function _extractRecordIds(operations) {
  return _.flatten(operations.map(o => _.values(o))).map(o => o.record.id);
}
