/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const cache = require('bedrock-redis');
const helpers = require('./helpers');
const mockData = require('./mock.data');

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

const eventTemplate = mockData.events.alpha;

// NOTE: alpha is assigned manually
// NOTE: all these may not be used
const nodeLabels = [
  'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota'
];
const nodes = {};
const peers = {};
const heads = {};

describe.only('X Block Test', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  const nodeCount = 6;
  describe(`Consensus with ${nodeCount} Nodes`, () => {

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    const mockIdentity = mockData.identities.regularUser;
    const ledgerConfiguration = mockData.ledgerConfiguration;
    before(done => {
      async.auto({
        clean: callback => cache.client.flushall(callback),
        actor: ['clean', (results, callback) => brIdentity.get(
          null, mockIdentity.identity.id, (err, identity) => {
            callback(err, identity);
          })],
        consensusPlugin: ['clean', (results, callback) => brLedgerNode.use(
          'Continuity2017', callback)],
        ledgerNode: ['actor', (results, callback) => {
          brLedgerNode.add(null, {ledgerConfiguration}, (err, ledgerNode) => {
            if(err) {
              return callback(err);
            }
            nodes.alpha = ledgerNode;
            callback(null, ledgerNode);
          });
        }]
      }, (err, results) => {
        if(err) {
          return done(err);
        }
        consensusApi = results.consensusPlugin.api;
        done();
      });
    });

    // get genesis record (block + meta)
    let genesisRecord;
    before(done => {
      nodes.alpha.blocks.getGenesis((err, result) => {
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
      async.times(nodeCount - 1, (i, callback) => {
        brLedgerNode.add(null, {
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

    // populate peers and init heads
    before(done => async.eachOf(nodes, (ledgerNode, i, callback) =>
      consensusApi._worker._voters.get(
        {ledgerNodeId: ledgerNode.id}, (err, result) => {
          peers[i] = result.id;
          heads[i] = [];
          callback();
        }),
    err => {
      // add reporting here
      // helpers.report({nodes, peers});
      done(err);
    }));

    describe('Check Genesis Block', () => {
      it('should have the proper information', done => {
        const blockHashes = [];
        async.auto({
          getLatest: callback => async.each(nodes, (ledgerNode, callback) =>
            ledgerNode.storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              const eventBlock = result.eventBlock;
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
              // console.log('8888888', JSON.stringify(eventBlock, null, 2));
              blockHashes.push(eventBlock.meta.blockHash);
              callback();
            }), callback),
          testHash: ['getLatest', (results, callback) => {
            blockHashes.every(h => h === blockHashes[0]).should.be.true;
            callback();
          }]
        }, done);
      });
    });
    /*
      going into this test, there are two node, peer[0] which is the genesisNode
      and peer[1].
      1. add regular event on peer[1]
      2. run worker on peer[1]
     */
    const targetBlockHeight = 500;
    describe(`${targetBlockHeight} Blocks`, () => {
      // 1. add new regular event on each node
      // 2. run worker on all nodes
      // 3. report blockheight and event counts

      it('makes many more blocks', function(done) {
        this.timeout(0);

        const targetBlockHashMap = {};
        async.until(() => {
          return Object.keys(targetBlockHashMap).length ===
            Object.keys(nodes).length;
        }, callback => {
          const count = 1;
          async.auto({
            alphaAddEvent1: callback => helpers.addEvent(
              {count, eventTemplate, ledgerNode: nodes.alpha}, callback),
            betaAddEvent1: callback => helpers.addEvent(
              {count, eventTemplate, ledgerNode: nodes.beta}, callback),
            gammaAddEvent1: callback => helpers.addEvent(
              {count, eventTemplate, ledgerNode: nodes.gamma}, callback),
            deltaAddEvent1: callback => helpers.addEvent(
              {count, eventTemplate, ledgerNode: nodes.delta}, callback),
            workCycle1: [
              'alphaAddEvent1', 'betaAddEvent1',
              'gammaAddEvent1', 'deltaAddEvent1',
              (results, callback) =>
                _workerCycle({consensusApi, nodes, series: false}, callback)],
            report: ['workCycle1', (results, callback) => async.forEachOfSeries(
              nodes, (ledgerNode, i, callback) => {
                ledgerNode.storage.blocks.getLatestSummary(
                  (err, result) => {
                    assertNoError(err);
                    const block = result.eventBlock.block;
                    if(block.blockHeight >= targetBlockHeight) {
                      return ledgerNode.storage.blocks.getByHeight(
                        targetBlockHeight, (err, result) => {
                          if(err) {
                            return callback(err);
                          }
                          targetBlockHashMap[i] = result.meta.blockHash;
                          callback();
                        });
                    }
                    callback();
                  });
              }, callback)]
          }, err => {
            if(err) {
              return callback(err);
            }
            callback();
          });
        }, err => {
          if(err) {
            return done(err);
          }
          console.log(
            'targetBlockHashMap', JSON.stringify(targetBlockHashMap, null, 2));
          _.values(targetBlockHashMap)
            .every(h => h === targetBlockHashMap.alpha).should.be.true;
          done(err);
        });
      });
    }); // end one block
  });
});

function _workerCycle({consensusApi, nodes, series = false}, callback) {
  const func = series ? async.eachSeries : async.each;
  func(nodes, (ledgerNode, callback) =>
    consensusApi._worker._run(ledgerNode, callback), callback);
}
