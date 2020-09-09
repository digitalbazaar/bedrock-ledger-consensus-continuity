/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

// the total number of nodes on the ledger may be adjusted here
const nodeCount = 10;

describe('Multinode', function() {
  before(async function() {
    await helpers.prepareDatabase(mockData);
  });

  describe(`Consensus with ${nodeCount} Nodes`, function() {

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    let genesisLedgerNode;
    // get genesis record (block + meta)
    let genesisRecord;
    const mockAccount = mockData.accounts.regularUser;
    const {ledgerConfiguration} = mockData;
    before(async function() {
      this.timeout(120000);
      await helpers.flushCache();
      await helpers.removeCollections(['ledger', 'ledgerNode']);
      const consensusPlugin = helpers.use('Continuity2017');
      consensusApi = consensusPlugin.api;
      genesisLedgerNode = await brLedgerNode.add(null, {ledgerConfiguration});
      const {genesisBlock} = await genesisLedgerNode.
        blocks.getGenesis();
      genesisRecord = genesisBlock;
    });

    // add N - 1 more private nodes
    const peers = [];
    before(function(done) {
      this.timeout(120000);
      peers.push(genesisLedgerNode);
      async.times(nodeCount - 1, (i, callback) => {
        brLedgerNode.add(null, {
          genesisBlock: genesisRecord.block,
          owner: mockAccount.account.id
        }, (err, ledgerNode) => {
          if(err) {
            return callback(err);
          }
          peers.push(ledgerNode);
          callback();
        });
      }, err => {
        assertNoError(err);
        done();
      });
    });

    // populate peers ids
    before(done => async.eachOf(peers, (ledgerNode, i, callback) =>
      consensusApi._voters.get(
        {ledgerNodeId: ledgerNode.id}, (err, result) => {
          assertNoError(err);
          ledgerNode._peerId = result.id;
          callback();
        }),
    err => {
      assertNoError(err);
      done();
    }));

    // override elector selection to force cycling and 3f+1
    before(function() {
      this.timeout(120000);
      let candidates;
      const electorSelectionApi = brLedgerNode.use('MostRecentParticipants');
      electorSelectionApi.api.getBlockElectors = async ({blockHeight}) => {
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
        const electors = candidates.slice(start, start + count);
        if(electors.length < count) {
          electors.push(...candidates.slice(0, count - electors.length));
        }
        return {electors};
      };
    });

    describe('Check Genesis Block', function() {
      it('should have the proper information', done => async.auto({
        getLatest: callback => async.map(peers, (ledgerNode, callback) =>
          ledgerNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            const eventBlock = result.eventBlock;
            should.exist(eventBlock.block);
            eventBlock.block.blockHeight.should.equal(0);
            eventBlock.block.event.should.be.an('array');
            // genesis config and genesis merge events
            eventBlock.block.event.should.have.length(2);
            const event = eventBlock.block.event[0];
            // TODO: signature is dynamic... needs a better check
            delete event.signature;
            event.type.should.equal('WebLedgerConfigurationEvent');
            const {ledgerConfiguration} = mockData;
            event.ledgerConfiguration.should.eql(ledgerConfiguration);
            should.exist(eventBlock.meta);
            should.exist(eventBlock.block.consensusProof);
            const consensusProof = eventBlock.block.consensusProof;
            consensusProof.should.be.an('array');
            consensusProof.should.have.length(1);
            // FIXME: make assertions about the contents of consensusProof
            // console.log('8888888', JSON.stringify(eventBlock, null, 2));
            callback(null, eventBlock.meta.blockHash);
          }), callback),
        testHash: ['getLatest', (results, callback) => {
          const blockHashes = results.getLatest;
          blockHashes.every(h => h === blockHashes[0]).should.be.true;
          callback();
        }]
      }, err => {
        assertNoError(err);
        done();
      }));
    });

    describe('Block 1', function() {
      // add a single op to genesis node, genesis node will be sole elector
      it('should add an operation and achieve consensus', function(done) {
        this.timeout(30000);
        const opTemplate = mockData.operations.alpha;
        async.auto({
          addOperation: callback => helpers.addOperation(
            {ledgerNode: genesisLedgerNode, opTemplate}, callback),
          settleNetwork: ['addOperation', (results, callback) =>
            helpers.settleNetwork(
              {consensusApi, nodes: peers, series: false}, callback)],
          getLatest: ['settleNetwork', (results, callback) =>
            async.map(peers, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                assertNoError(err);
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

                callback(null, eventBlock.meta.blockHash);
              }), callback)],
          testHash: ['getLatest', (results, callback) => {
            const blockHashes = results.getLatest;
            // the blockHash on every node should be the same
            blockHashes.every(h => h === blockHashes[0]).should.be.true;
            callback();
          }]
        }, done);
      });
    }); // end block 1
    describe('Operations', function() {
      // add an operation on all peers, settle and ensure that all records are
      // available via the records API
      it('add an operation on all nodes and achieve consensus', function(done) {
        this.timeout(210000);
        const opTemplate = mockData.operations.alpha;
        async.auto({
          addOperation: callback => helpers.addOperations(
            {nodes: peers, opTemplate}, callback),
          settleNetwork: ['addOperation', (results, callback) =>
            helpers.settleNetwork(
              {consensusApi, nodes: peers, series: false}, callback)],
          test: ['settleNetwork', (results, callback) => {
            const recordIds = _extractRecordIds(results.addOperation);
            async.eachSeries(peers, (ledgerNode, callback) =>
              async.every(recordIds, (recordId, callback) =>
                ledgerNode.records.get({recordId}, (err, result) => {
                  if(err) {
                    if(err.name === 'NotFoundError') {
                      return callback(null, false);
                    }
                    return callback(err);
                  }
                  result.should.be.an('object');
                  callback(null, true);
                }),
              (err, result) => {
                assertNoError(err);
                result.should.be.true;
                callback();
              }), callback);
          }],
        }, err => {
          assertNoError(err);
          done();
        });
      });
    }); // end Operations
    describe('Ledger Configuration', function() {
      // add a config event on the genesis node, settle the network, ensure
      // that new config is in effect on all nodes
      it('ValidationError on missing ledger property', async function() {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        ledgerConfiguration.sequence = 1;
        delete ledgerConfiguration.ledger;
        let error;
        try {
          await genesisLedgerNode.config.change({ledgerConfiguration});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('ValidationError');
        error.details.errors[0].message.should.equal(
          `should have required property 'ledger'`);
      });
      it('SyntaxError on invalid ledger', async function() {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        ledgerConfiguration.sequence = 1;
        ledgerConfiguration.ledger = 'https://example.com/invalidLedger';
        let error;
        try {
          await genesisLedgerNode.config.change({ledgerConfiguration});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('SyntaxError');
        error.message.should.equal(`Invalid configuration 'ledger' value.`);
      });
      it('ValidationError on missing creator', async function() {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        // creator is not added
        ledgerConfiguration.sequence = 1;
        let error;
        try {
          await genesisLedgerNode.config.change({ledgerConfiguration});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('ValidationError');
        error.details.errors[0].message.should.equal(
          `should have required property 'creator'`);
      });
      it('SyntaxError on invalid creator', async function() {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        // creator is invalid
        ledgerConfiguration.creator = 'https://example.com/invalidCreator';
        ledgerConfiguration.sequence = 1;
        let error;
        try {
          await genesisLedgerNode.config.change({ledgerConfiguration});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('SyntaxError');
        error.message.should.equal(`Invalid configuration 'creator' value.`);
      });
      it('ValidationError on missing sequence', async function() {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        delete ledgerConfiguration.sequence;
        let error;
        try {
          await genesisLedgerNode.config.change({ledgerConfiguration});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('ValidationError');
        error.details.errors[0].message.should.equal(
          `should have required property 'sequence'`);
      });
      it('SyntaxError on invalid sequence', async function() {
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        // invalid sequence, should be 1
        ledgerConfiguration.sequence = 5;
        let error;
        try {
          await genesisLedgerNode.config.change({ledgerConfiguration});
        } catch(e) {
          error = e;
        }
        should.exist(error);
        error.name.should.equal('SyntaxError');
        error.message.should.equal(`Invalid configuration 'sequence' value.`);
      });
      it('add a ledger config and achieve consensus', function(done) {
        this.timeout(210000);
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
        ledgerConfiguration.creator = genesisLedgerNode._peerId;
        ledgerConfiguration.sequence = 1;
        ledgerConfiguration.consensusMethod = 'Continuity9000';
        async.auto({
          changeConfig: callback => genesisLedgerNode.config.change(
            {ledgerConfiguration}, callback),
          settleNetwork: ['changeConfig', (results, callback) =>
            helpers.settleNetwork(
              {consensusApi, nodes: peers, series: false}, callback)],
          test: ['settleNetwork', (results, callback) => {
            async.map(peers, (ledgerNode, callback) =>
              ledgerNode.config.get(callback),
            (err, result) => {
              if(err) {
                return callback(err);
              }
              for(const c of result) {
                c.should.eql(ledgerConfiguration);
              }
              callback();
            });
          }],
        }, err => {
          assertNoError(err);
          done();
        });
      });
    }); // end Ledger Configuration
    describe('Catch-up', function() {
      it('a new node is able to catch up', function(done) {
        this.timeout(120000);
        async.auto({
          addNode: callback => brLedgerNode.add(null, {
            genesisBlock: genesisRecord.block,
            owner: mockAccount.account.id
          }, (err, ledgerNode) => {
            if(err) {
              return callback(err);
            }
            peers.push(ledgerNode);
            callback();
          }),
          settleNetwork: ['addNode', (results, callback) =>
            helpers.settleNetwork(
              {consensusApi, nodes: peers, series: false}, callback)],
          test: ['settleNetwork', (results, callback) => {
            async.map(peers, (ledgerNode, callback) =>
              ledgerNode.config.get(callback),
            (err, result) => {
              if(err) {
                return callback(err);
              }
              for(const c of result) {
                c.should.eql(result[0]);
              }
              callback();
            });
          }],
        }, err => {
          assertNoError(err);
          done();
        });
      });
    });
    describe('Reinitialize Nodes', function() {
      // the nodes should load with a new consensus method
      it('nodes should have new consensus method', async function() {
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
