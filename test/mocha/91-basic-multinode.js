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

// NOTE: alpha is assigned manually
const nodeLabels = ['beta', 'gamma', 'delta', 'epsilon'];
const nodes = {};

describe('Multinode Basics', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  describe('Consensus with 2 Nodes', () => {
    const nodeCount = 2;

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
    before(function(done) {
      this.timeout(120000);
      nodes.alpha = genesisLedgerNode;
      async.times(nodeCount - 1, (i, callback) => {
        brLedger.add(null, {
          genesisBlock: genesisRecord.block,
          owner: mockIdentity.identity.id
        }, (err, ledgerNode) => {
          if(err) {
            return callback(err);
          }
          nodes[nodeLabels[i]] = ledgerNode;
          callback();
        });
      }, done);
    });

    describe('Check Genesis Block', () => {
      it('should have the proper information', done => async.auto({
        getLatest: callback => async.each(nodes, (ledgerNode, callback) =>
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
    describe.only('One Block', () => {
      it('should add an event and achieve consensus', function(done) {
        this.timeout(120000);
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEvent.input[0].id = 'https://example.com/events/' + uuid();
        async.auto({
          addEvent: callback => nodes.beta.events.add(
            testEvent, callback),
          // this will merge event on peer[1] and transmit to peer[0]
          runWorker1: ['addEvent', (results, callback) =>
            consensusApi._worker._run(nodes.beta, callback)],
          // this should merge events from peer[1] and create a new block
          runWorker2: ['runWorker1', (results, callback) =>
            consensusApi._worker._run(nodes.beta, err => {
              assertNoError(err);
              callback(err);
            })],
          test1: ['runWorker2', (results, callback) =>
            nodes.alpha.storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
          // this should receive events from peers[0], merge and generate block
          runWorker3: ['test1', (results, callback) =>
            consensusApi._worker._run(nodes.beta, err => {
              assertNoError(err);
              callback(err);
            })],
          // FIXME: having to run worker a second time to generate a block
          runWorker4: ['runWorker3', (results, callback) =>
            consensusApi._worker._run(nodes.beta, err => {
              assertNoError(err);
              callback(err);
            })],
          test2: ['runWorker4', (results, callback) =>
            nodes.beta.storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
        }, done);
      });
    }); // end one block
    describe('More Blocks', () => {
      it.skip('should add an event and achieve consensus', function(done) {
        console.log('ALPHA COLL', nodes.alpha.storage.events.collection.s.name);
        this.timeout(120000);
        const eventTemplate = mockData.events.alpha;
        async.auto({
          addEvent: callback => nodes.beta.events.add(
            helpers.createEventBasic({eventTemplate}), callback),
          // this will merge event on peer[1] and transmit to peer[0]
          worker1: ['addEvent', (results, callback) =>
            consensusApi._worker._run(nodes.beta, callback)],
          count1: ['worker1', (results, callback) =>
            async.each(nodes, (ledgerNode, callback) => {
              ledgerNode.storage.events.collection.find({})
                .toArray((err, result) => {
                  assertNoError(err);
                  result.should.have.length(6);
                  callback();
                });
            }, callback)],
          // this should merge events from beta and create a new block
          worker2: ['count1', (results, callback) =>
            consensusApi._worker._run(nodes.alpha, err => {
              assertNoError(err);
              callback(err);
            })],
          test1: ['worker2', (results, callback) =>
            nodes.alpha.storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
          // this should receive events from alpha, merge and generate block
          worker3: ['test1', (results, callback) =>
            consensusApi._worker._run(nodes.beta, err => {
              assertNoError(err);
              console.log('AFTER WORKER3 -----------------------------');
              callback(err);
            })],
          // FIXME: this will likely change to have a new merge event on beta
          // after runWorker3
          count2: ['worker3', (results, callback) =>
            async.each(nodes, (ledgerNode, callback) => {
              ledgerNode.storage.events.collection.find({})
                .toArray((err, result) => {
                  assertNoError(err);
                  result.should.have.length(5);
                  callback();
                });
            }, callback)],
          // FIXME: having to run worker a second time to generate a block
          worker4: ['count2', (results, callback) =>
            consensusApi._worker._run(nodes.beta, err => {
              console.log('AFTER WORKER4 -----------------------------');
              assertNoError(err);
              callback(err);
            })],
          test2: ['worker4', (results, callback) =>
            nodes.beta.storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              // console.log('999999999', result.eventBlock.block.event);
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
          // // add another event on beta
          addEvent2: ['test2', (results, callback) => nodes.beta.events.add(
            helpers.createEventBasic({eventTemplate}), callback)],
          // this iteration only transmit that merge event that beta created
          // after the alpha merge event
          worker5: ['addEvent2', (results, callback) =>
            consensusApi._worker._run(nodes.beta, err => {
              assertNoError(err);
              console.log('AFTER WORKER4 -----------------------------');
              callback(err);
            })],
          worker6: ['worker5', (results, callback) =>
            consensusApi._worker._run(nodes.beta, err => {
              assertNoError(err);
              console.log('AFTER WORKER5 -----------------------------');
              callback(err);
            })],
        }, done);
      });
    }); // end block 1
  });
});
