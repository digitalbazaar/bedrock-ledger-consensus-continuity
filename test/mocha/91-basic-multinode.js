/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedger = require('bedrock-ledger-node');
const async = require('async');
const uuid = require('uuid/v4');

const helpers = require('./helpers');
const mockData = require('./mock.data');

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

describe.only('Multinode Basics', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  describe('Consensus with 2 Nodes', () => {
    const nodes = 2;

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    let genesisLedgerNode;
    const mockIdentity = mockData.identities.regularUser;
    const configEvent = mockData.events.config;
    before(done => {
      async.auto({
        clean: callback =>
          helpers.removeCollections(['ledger', 'ledgerNode'], callback),
        actor: ['clean', (results, callback) => brIdentity.get(
          null, mockIdentity.identity.id, (err, identity) => {
            callback(err, identity);
          })],
        consensusPlugin: callback => brLedger.use('Continuity2017', callback),
        ledgerNode: ['actor', (results, callback) => {
          brLedger.add(null, {configEvent}, (err, ledgerNode) => {
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
      async.times(nodes - 1, (i, callback) => {
        brLedger.add(null, {
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

    describe('Check Genesis Block', () => {
      it('should have the proper information', done => async.auto({
        getLatest: callback => async.map(peers, (ledgerNode, callback) =>
          ledgerNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            const eventBlock = result.eventBlock;
            should.exist(eventBlock.block);
            eventBlock.block.blockHeight.should.equal(0);
            eventBlock.block.event.should.be.an('array');
            eventBlock.block.event.should.have.length(1);
            const event = eventBlock.block.event[0];
            // TODO: signature is dynamic... needs a better check
            delete event.signature;
            event.should.deep.equal(configEvent);
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
      }, done));
    });
    /*
      going into this test, there are two node, peer[0] which is the genesisNode
      and peer[1].
      1. add regular event on peer[1]
      2. run worker on peer[1]
     */
    describe('Block 1', () => {
      it('should add an event and achieve consensus', function(done) {
        this.timeout(120000);
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEvent.input[0].id = 'https://example.com/events/' + uuid();
        async.auto({
          addEvent: callback => peers[1].events.add(
            testEvent, callback),
          // this will merge event on peer[1] and transmit to peer[0]
          runWorker1: ['addEvent', (results, callback) =>
            consensusApi._worker._run(peers[1], callback)],
          // this should merge events from peer[1] and create a new block
          runWorker2: ['runWorker1', (results, callback) =>
            consensusApi._worker._run(peers[0], err => {
              assertNoError(err);
              callback(err);
            })],
          test1: ['runWorker2', (results, callback) =>
            peers[0].storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
          // this should receive events from peers[0], merge and generate block
          runWorker3: ['test1', (results, callback) =>
            consensusApi._worker._run(peers[1], err => {
              assertNoError(err);
              callback(err);
            })],
          // FIXME: having to run worker a second time to generate a block
          runWorker4: ['runWorker3', (results, callback) =>
            consensusApi._worker._run(peers[1], err => {
              assertNoError(err);
              callback(err);
            })],
          test2: ['runWorker4', (results, callback) =>
            peers[1].storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
        }, done);
      });
    }); // end block 1
  });
});
