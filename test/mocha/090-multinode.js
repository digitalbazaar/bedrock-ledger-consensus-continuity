/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const helpers = require('./helpers');
const mockData = require('./mock.data');

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

// the total number of nodes on the ledger may be adjusted here
const nodeCount = 10;

describe('Multinode', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  describe(`Consensus with ${nodeCount} Nodes`, () => {

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    let genesisLedgerNode;
    const mockIdentity = mockData.identities.regularUser;
    const {ledgerConfiguration} = mockData;
    before(done => {
      async.auto({
        clean: callback =>
          helpers.removeCollections(['ledger', 'ledgerNode'], callback),
        actor: ['clean', (results, callback) => brIdentity.get(
          null, mockIdentity.identity.id, (err, identity) => {
            callback(err, identity);
          })],
        consensusPlugin: callback => helpers.use('Continuity2017', callback),
        ledgerNode: ['actor', (results, callback) => {
          brLedgerNode.add(null, {ledgerConfiguration}, (err, ledgerNode) => {
            if(err) {
              return callback(err);
            }
            callback(null, ledgerNode);
          });
        }]
      }, (err, results) => {
        if(err) {
          return done(err);
        }
        genesisLedgerNode = results.ledgerNode;
        consensusApi = results.consensusPlugin.api;
        done();
      });
    });

    // get genesis record (block + meta)
    let genesisRecord;
    before(done => {
      genesisLedgerNode.blocks.getGenesis((err, result) => {
        if(err) {
          return done(err);
        }
        genesisRecord = result.genesisBlock;
        done();
      });
    });

    // add N - 1 more private nodes
    const peers = [];
    before(function(done) {
      this.timeout(120000);
      peers.push(genesisLedgerNode);
      async.times(nodeCount - 1, (i, callback) => {
        brLedgerNode.add(null, {
          genesisBlock: genesisRecord.block,
          owner: mockIdentity.identity.id
        }, (err, ledgerNode) => {
          if(err) {
            return callback(err);
          }
          peers.push(ledgerNode);
          callback();
        });
      }, done);
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

    describe('Check Genesis Block', () => {
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

    describe('Block 1', () => {
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
                eventBlock.block.blockHeight.should.equal(1);
                eventBlock.block.event.should.be.an('array');
                // a regular event and a merge event
                eventBlock.block.event.should.have.length(2);
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
    describe('Operations', () => {
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
                  result.should.be.an('object');
                  if(err) {
                    if(err.name === 'NotFoundError') {
                      return callback(null, false);
                    }
                    return callback(err);
                  }
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
    describe('Ledger Configuration', () => {
      // add a config event on the genesis node, settle the network, ensure
      // that new config is in effect on all nodes
      it('should add a config event and achieve consensus', function(done) {
        this.timeout(210000);
        const ledgerConfiguration = bedrock.util.clone(
          mockData.ledgerConfiguration);
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
    describe('Catch-up', () => {
      it('a new node is able to catch up', function(done) {
        this.timeout(120000);
        async.auto({
          addNode: callback => brLedgerNode.add(null, {
            genesisBlock: genesisRecord.block,
            owner: mockIdentity.identity.id
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
