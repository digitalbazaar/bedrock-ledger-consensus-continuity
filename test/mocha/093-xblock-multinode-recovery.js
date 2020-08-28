/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
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

describe('X Block Test using elector selector with recovery', () => {
  before(async function() {
    await helpers.prepareDatabase(mockData);
  });

  // FIXME: update bedrock-ledger...es-most-recent-participants-with-recovery
  // to fix recovery elector count
  before(() => {
    const electorSelectionApi = brLedgerNode.use(
      'MostRecentParticipantsWithRecovery');
    electorSelectionApi.api._computeRecoveryElectors = ({electors, f}) => {
      if(f <= 1) {
        return [];
      }
      const r = f - 1;
      return electors.slice(0, 3 * r + 1);
    };
  });

  const nodeCount = 8;
  describe(`Consensus with ${nodeCount} Nodes`, () => {

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    const mockAccount = mockData.accounts.regularUser;
    const ledgerConfiguration = mockData.ledgerConfigurationRecovery;
    before(function(done) {
      this.timeout(180000);
      async.auto({
        clean: callback => cache.client.flushall(callback),
        consensusPlugin: ['clean', (results, callback) => helpers.use(
          'Continuity2017', callback)],
        ledgerNode: ['clean', (results, callback) => {
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
          owner: mockAccount.account.id
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
          assertNoError(err);
          peers[i] = result.id;
          ledgerNode._peerId = result.id;
          heads[i] = [];
          callback();
        }),
    err => {
      assertNoError(err);
      done();
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
      it('makes many more blocks', function(done) {
        this.timeout(0);
        async.auto({
          nBlocks: callback => _nBlocks(
            {consensusApi, targetBlockHeight}, (err, result) => {
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
  });
});

function _addOperations({count}, callback) {
  async.auto({
    alpha: callback => helpers.addOperation(
      {count, ledgerNode: nodes.alpha, opTemplate}, callback),
    beta: callback => helpers.addOperation(
      {count, ledgerNode: nodes.beta, opTemplate}, callback),
    gamma: callback => helpers.addOperation(
      {count, ledgerNode: nodes.gamma, opTemplate}, callback),
    delta: callback => helpers.addOperation(
      {count, ledgerNode: nodes.delta, opTemplate}, callback),
  }, callback);
}

function _latestBlockSummary(callback) {
  const blocks = {};
  async.eachOf(nodes, (ledgerNode, nodeName, callback) => {
    ledgerNode.storage.blocks.getLatestSummary((err, result) => {
      blocks[nodeName] = result;
      callback();
    });
  }, err => callback(err, blocks));
}

function _nBlocks({consensusApi, targetBlockHeight}, callback) {
  const recordIds = {alpha: [], beta: [], gamma: [], delta: []};
  const targetBlockHashMap = {};
  async.until(() => {
    return Object.keys(targetBlockHashMap).length ===
      Object.keys(nodes).length;
  }, callback => {
    const count = 1;
    async.auto({
      operations: callback => _addOperations({count}, callback),
      workCycle: ['operations', (results, callback) => {
        // record the IDs for the records that were just added
        for(const n of ['alpha', 'beta', 'gamma', 'delta']) {
          for(const opHash of Object.keys(results.operations[n])) {
            recordIds[n].push(results.operations[n][opHash].record.id);
          }
        }
        // in this test `nodes` is an object that needs to be converted to
        // an array for the helper
        helpers.runWorkerCycle(
          {consensusApi, nodes: _.values(nodes), series: false}, callback);
      }],
      report: ['workCycle', (results, callback) => async.forEachOfSeries(
        nodes, (ledgerNode, i, callback) => {
          ledgerNode.storage.blocks.getLatestSummary((err, result) => {
            if(err) {
              return callback(err);
            }
            const {block} = result.eventBlock;
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
      return callback(err);
    }
    callback(null, {recordIds, targetBlockHashMap});
  });
}
