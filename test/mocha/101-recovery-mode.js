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

const opTemplate = mockData.operations.alpha;

// NOTE: alpha is assigned manually
// NOTE: all these may not be used
const nodeLabels = [
  'beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta', 'theta', 'iota'
];
const nodes = {};
const peers = {};
const heads = {};

describe('Recovery mode simulation', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  // override elector selection helpers for tests
  before(() => {
    const electorSelectionApi = brLedgerNode.use(
      'MostRecentParticipantsWithRecovery');

    electorSelectionApi.api._computeElectors = async () => {
      const electors = [];
      for(const p of Object.keys(nodes)) {
        electors.push({id: peers[p]});
      }
      console.log(`${Date.now()} _COMPUTEELECTORS`, electors.length);
      return electors;
    };

    electorSelectionApi.api._computeRecoveryElectors =
      ({electors, f}) => {
        // const activePeers = new Set();
        let recoveryElectors = [];
        for(const n of Object.keys(nodes)) {
          // activePeers.add(peers[n]);
          recoveryElectors.push({id: peers[n]});
        }

        if(electors.length === 1) {
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

        recoveryElectors = recoveryElectors
          .sort((a, b) => a.id.localeCompare(b.id)).slice(0, f + 1);
        console.log(`${Date.now()} _COMPUTERECOVERYELECTORS, f`,
          Object.keys(nodes), recoveryElectors, f);
        return recoveryElectors.length < f + 1 ? [] : recoveryElectors;
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
    before(function(done) {
      this.timeout(180000);
      async.auto({
        clean: callback => cache.client.flushall(callback),
        actor: ['clean', (results, callback) => brIdentity.get(
          null, mockIdentity.identity.id, (err, identity) => {
            callback(err, identity);
          })],
        consensusPlugin: ['clean', (results, callback) => helpers.use(
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
      this.timeout(180000);
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
      consensusApi._voters.get(
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

        for(const n of Object.keys(nodes)) {
          console.log(`----- ${n} ----`);
          console.log(`Storage ${nodes[n].storage.events.collection.s.name}`);
          console.log(`PeerId ${peers[n]}`);
        }

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
     * 1. add new unique operations/records on nodes alpha, beta, gamma, delta
     * 2. run worker on *all* nodes
     * 3. repeat 1 and 2 until target block height is reached on all nodes
     * 4. ensure that blockHash for the target block height is identical on all
     * 5. settle the network, see notes on _settleNetwork
     * 6. ensure that the final blockHeight and blockHash is identical on all
     * 7. attempt to retrieve all records added in 1 from the `records` API
     *
     * Test recovery mode:
     * 8. Reduce `nodes` from seven to four nodes: alpha, beta, gamma, delta
     * 9. add new unique operations/records on nodes alpha, beta, gamma, delta
     * 10. run worker on all remaining nodes
     * 11. repeat 9 and 10 until target block height is reached on all nodes
     *     it is anticipated that the network should go into recovery mode
     *     under these conditions.
     */

    const targetBlockHeight = 5;
    describe(`${targetBlockHeight} Blocks`, () => {
      it(`makes ${targetBlockHeight} blocks with all nodes`, function(done) {
        this.timeout(0);
        async.auto({
          nBlocks: callback => helpers.nBlocks({
            consensusApi, nodes, opTemplate, operationOnWorkCycle: 'all',
            targetBlockHeight
          }, (err, result) => {
            if(err) {
              return callback(err);
            }
            console.log(
              'targetBlockHashMap',
              JSON.stringify(result, null, 2));
            _.values(result.targetBlockHashMap)
              .every(h => h === result.targetBlockHashMap.alpha)
              .should.be.true;
            callback(null, result);
          }),
          settle: ['nBlocks', (results, callback) => helpers.settleNetwork(
            {consensusApi, nodes: _.values(nodes)}, callback)],
          blockSummary: ['settle', (results, callback) =>
            _latestBlockSummary((err, result) => {
              if(err) {
                return callback(err);
              }
              const summaries = {};
              Object.keys(result).forEach(k => {
                summaries[k] = {
                  blockCollection: nodes[k].storage.blocks.collection.s.name,
                  blockHeight: result[k].eventBlock.block.blockHeight,
                  blockHash: result[k].eventBlock.meta.blockHash,
                  previousBlockHash: result[k].eventBlock.block
                    .previousBlockHash,
                };
              });
              console.log('Finishing block summaries:', JSON.stringify(
                summaries, null, 2));
              _.values(summaries).forEach(b => {
                b.blockHeight.should.equal(summaries.alpha.blockHeight);
                b.blockHash.should.equal(summaries.alpha.blockHash);
              });
              callback();
            })],
          state: ['blockSummary', (results, callback) => {
            const allRecordIds = [].concat(..._.values(
              results.nBlocks.recordIds));
            console.log(`Total operation count: ${allRecordIds.length}`);
            async.eachSeries(allRecordIds, (recordId, callback) => {
              nodes.alpha.records.get({recordId}, err => {
                // just need to ensure that there is no NotFoundError
                assertNoError(err);
                callback();
              });
            }, callback);
          }]
        }, err => {
          assertNoError(err);
          done();
        });
      });
    }); // end one block

    const recoveryBlocks = 10;
    const newTargetBlockHeight = targetBlockHeight + recoveryBlocks;
    describe(`${recoveryBlocks} Recovery Blocks`, () => {
      it(`makes ${recoveryBlocks} blocks with four nodes`, function(done) {
        this.timeout(0);

        // remove 3 out of 7 nodes
        delete nodes.epsilon;
        delete nodes.zeta;
        delete nodes.eta;

        async.auto({
          nBlocks: callback => helpers.nBlocks({
            consensusApi, nodes, opTemplate, operationOnWorkCycle: 'first',
            targetBlockHeight: newTargetBlockHeight
          }, (err, result) => {
            if(err) {
              return callback(err);
            }
            console.log(
              'targetBlockHashMap', JSON.stringify(result, null, 2));
            const firstNodeLabel = Object.keys(result.targetBlockHashMap)[0];
            _.values(result.targetBlockHashMap)
              .every(h => h === result.targetBlockHashMap[firstNodeLabel])
              .should.be.true;

            callback(null, result);
          }),
          settle: ['nBlocks', (results, callback) => helpers.settleNetwork(
            {consensusApi, nodes: _.values(nodes)}, callback)],
          blockSummary: ['settle', (results, callback) =>
            _latestBlockSummary((err, result) => {
              if(err) {
                return callback(err);
              }
              const summaries = {};
              Object.keys(result).forEach(k => {
                summaries[k] = {
                  blockCollection: nodes[k].storage.blocks.collection.s.name,
                  blockHeight: result[k].eventBlock.block.blockHeight,
                  blockHash: result[k].eventBlock.meta.blockHash,
                  previousBlockHash: result[k].eventBlock.block
                    .previousBlockHash,
                };
              });
              console.log('Finishing block summaries:', JSON.stringify(
                summaries, null, 2));
              _.values(summaries).forEach(b => {
                b.blockHeight.should.equal(summaries.alpha.blockHeight);
                b.blockHash.should.equal(summaries.alpha.blockHash);
              });
              callback();
            })],
          state: ['blockSummary', (results, callback) => {
            const allRecordIds = [].concat(..._.values(
              results.nBlocks.recordIds));
            console.log(`Total operation count: ${allRecordIds.length}`);
            async.eachSeries(allRecordIds, (recordId, callback) => {
              nodes.alpha.records.get({recordId}, err => {
                // just need to ensure that there is no NotFoundError
                assertNoError(err);
                callback();
              });
            }, callback);
          }]
        }, err => {
          assertNoError(err);
          done();
        });
      });
    }); // end one block
  });
});

function _latestBlockSummary(callback) {
  const blocks = {};
  async.eachOf(nodes, (ledgerNode, nodeName, callback) => {
    ledgerNode.storage.blocks.getLatestSummary((err, result) => {
      blocks[nodeName] = result;
      callback();
    });
  }, err => callback(err, blocks));
}
