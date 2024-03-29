/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const {callbackify} = require('util');

const helpers = require('./helpers');
const mockData = require('./mock.data');
const blessed = require('blessed');

let screen;
let table;
let tableHead;
let tableData;

const blessedEnabled = true;
const tracerInterval = 10;

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

const eventTemplate = mockData.events.alpha;

// NOTE: alpha is assigned manually
const nodeLabels = ['beta', 'gamma', 'delta', 'epsilon', 'zeta', 'eta'];
const nodes = {};
const peers = {};
const heads = {};

/* eslint-disable no-unused-vars */
describe.skip('Multinode Basics', () => {
  before(async () => {
    if(blessedEnabled) {
      screen = blessed.screen({smartCSR: true});
      screen.key(['C-c'], (ch, key) => process.exit(0));
    }
    await helpers.prepareDatabase();
  });

  describe('Consensus with 2 Nodes', () => {
    const nodeCount = 6;

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    const ledgerConfiguration = mockData.ledgerConfiguration;
    before(async function() {
      await helpers.flushCache();
      const consensusPlugin = await helpers.use('Continuity2017');
      consensusApi = consensusPlugin.api;
      nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
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

    // add N - 1 more nodes
    before(async function() {
      this.timeout(120000);
      for(let i = 0; i < nodeCount - 1; ++i) {
        const ledgerNode = await brLedgerNode.add(null, {
          genesisBlock: genesisRecord.block
        });
        nodes[nodeLabels[i]] = ledgerNode;
      }
    });

    // populate peers and init heads
    before(async () => {
      let i = 0;
      for(const ledgerNode of nodes) {
        const peerId = await consensusApi._localPeers.getPeerId(
          {ledgerNode: ledgerNode.id});
        peers[i] = peerId;
        heads[i] = [];
        if(i === 0) {
          // skip genesis peer
          continue;
        }
        // add genesis peer to the peer's peers collection
        const remotePeer = {
          id: peers[0],
          url: consensusApi._localPeers.getLocalPeerUrl(
            {peerId: peers[0]})
        };
        await consensusApi._peers.optionallyAdd(
          {ledgerNode, remotePeer});
        i++;
      }
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
          blockHashes.push(eventBlock.meta.blockHash);
        }
        blockHashes.every(h => h === blockHashes[0]).should.be.true;
      });
    });
    /*
      going into this test, there are two node, peer[0] which is the genesisNode
      and peer[1].
      1. add regular event on peer[1]
      2. run worker on peer[1]
     */
    describe('Two Nodes', () => {
      after(() => {
        if(blessedEnabled) {
          screen.destroy();
        }
        console.log('Summary',
          JSON.stringify(blessedSummary(tableData), null, 2));
      });
      it('two nodes reach consensus on two blocks', function(done) {
        this.timeout(120000);
        console.log('ALPHA COLL',
          nodes.alpha.storage.events.collection.collectionName);
        async.auto({
          betaAddEvent1: callback => nodes.beta.consensus._events.add(
            helpers.createEventBasic({eventTemplate}), nodes.beta, callback),
          // beta will merge its new regular event
          betaWorker1: ['betaAddEvent1', (results, callback) => {
            console.log('running beta worker 1 ------------');
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, callback);
          }],
          test1: ['betaWorker1', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(2);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(4);
                callback();
              }),
          }, callback)],
          // beta will push the regular and merge events to alpha
          betaWorker2: ['test1', (results, callback) => {
            console.log('running beta worker 2 ------------');
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                callback(err);
              });
          }],
          test2: ['betaWorker2', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(4);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(4);
                callback();
              }),
          }, callback)],
          // alpha will merge the event received from beta and create a block
          alphaWorker1: ['test2', (results, callback) => {
            console.log('running alpha worker 1 ------------');
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.alpha}, err => {
                assertNoError(err);
                callback(err);
              });
          }],
          test3: ['alphaWorker1', (results, callback) =>
            nodes.alpha.storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              console.log('testing for first block ---------------');
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
          // add a regular event on beta
          betaAddEvent2: ['test3', (results, callback) =>
            nodes.beta.consensus._events.add(
              helpers.createEventBasic({eventTemplate}), nodes.beta, callback)],
          // this will merge the regular event on beta and create its first
          // block now that alpha has endorsed its previous events
          betaWorker3: ['betaAddEvent2', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, callback)],
          test4: ['betaWorker3', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(5);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(7);
                callback();
              }),
            alphaBlock: callback => nodes.alpha.storage.blocks.getLatest(
              (err, result) => {
                assertNoError(err);
                // should not be a new block on alpha yet
                result.eventBlock.block.blockHeight.should.equal(1);
                callback(null, result);
              }),
            betaBlock: ['alphaBlock', (results, callback) =>
              nodes.beta.storage.blocks.getLatest((err, result) => {
                assertNoError(err);
                // should be a new block on beta
                result.eventBlock.block.blockHeight.should.equal(1);
                result.eventBlock.block.previousBlockHash.should.equal(
                  results.alphaBlock.eventBlock.block.previousBlockHash
                );
                // check blockHash last, it encompasses much of the above
                result.eventBlock.meta.blockHash.should.equal(
                  results.alphaBlock.eventBlock.meta.blockHash);
                callback();
              })],
          }, callback)],
          // beta pushes regular and merge events to alpha
          betaWorker4: ['test4', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                console.log('after beta worker 4 --------------------');
                callback();
              })],
          test5: ['betaWorker4', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(7);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(7);
                callback();
              }),
          }, callback)],
          // this will merge the events from beta, and alpha will consider
          // beta a witness and attempt to gossip with it
          // ... and alpha's merge event will be an endorsement of beta's
          // first merge event, so the next merge event on beta will be an X
          alphaWorker2: ['test5', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.alpha}, err => {
                assertNoError(err);
                console.log('after alpha worker 2 --------------------');
                callback(err);
              })],
          test6: ['alphaWorker2', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(8);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(7);
                callback();
              }),
          }, callback)],
          // beta will retrieve merge event from alpha and see an X; it will
          // also generate its own merge event ... which endorses alpha's
          // merge event
          betaWorker5: ['test6', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                console.log('after beta worker 5 --------------------');
                callback(err);
              })],
          test7: ['betaWorker5', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(8);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(9);
                callback();
              }),
          }, callback)],
          // beta will send merge event to alpha
          betaWorker6: ['test7', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                console.log('after beta worker 6 --------------------');
                callback(err);
              })],
          test8: ['betaWorker6', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(9);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(9);
                callback();
              }),
          }, callback)],
          // alpha will create a merge event that is alpha's X and that
          // endorse's beta's X
          alphaWorker3: ['test8', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.alpha}, err => {
                assertNoError(err);
                console.log('after alpha worker 3 --------------------');
                callback(err);
              })],
          // beta will receive alpha's merge event and create its own that
          // endorse's alpha's X and that is its Y
          betaWorker7: ['alphaWorker3', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                console.log('after beta worker 7 --------------------');
                callback(err);
              })],
          // alpha receives beta's Y and merges it creating alpha's Y; alpha's
          // Y supports [betaY, alphaY]
          alphaWorker4: ['betaWorker7', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.alpha}, err => {
                assertNoError(err);
                console.log('after alpha worker 4 --------------------');
                callback(err);
              })],
          // beta receives alpha's Y and creates a merge event, beta's Y
          // supports [betaY] and this new merge event supports [betaY, alphaY]
          // which creates a block
          betaWorker8: ['alphaWorker4', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                console.log('after beta worker 8 --------------------');
                callback(err);
              })],
          test9: ['betaWorker8', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(12);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(13);
                callback();
              }),
            alphaBlock: callback => nodes.alpha.storage.blocks.getLatest(
              (err, result) => {
                assertNoError(err);
                // should not be a new block on alpha yet
                result.eventBlock.block.blockHeight.should.equal(1);
                callback();
              }),
            betaBlock: callback => nodes.beta.storage.blocks.getLatest(
              (err, result) => {
                assertNoError(err);
                // should be a new block on beta
                result.eventBlock.block.blockHeight.should.equal(2);
                callback();
              }),
          }, callback)],
          alphaWorker5: ['test9', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.alpha}, err => {
                assertNoError(err);
                console.log('after alpha worker 5 --------------------');
                callback(err);
              })],
          test10: ['alphaWorker5', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(14);
                callback();
              }),
            beta: callback => nodes.beta.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                result.should.have.length(13);
                callback();
              }),
            alphaBlock: callback => nodes.alpha.storage.blocks.getLatest(
              (err, result) => {
                assertNoError(err);
                // should be a new block on alpha
                result.eventBlock.block.blockHeight.should.equal(2);
                callback(null, result);
              }),
            betaBlock: ['alphaBlock', (results, callback) =>
              nodes.beta.storage.blocks.getLatest((err, result) => {
                assertNoError(err);
                // should be a new block on beta
                result.eventBlock.block.blockHeight.should.equal(2);
                result.eventBlock.block.previousBlockHash.should.equal(
                  results.alphaBlock.eventBlock.block.previousBlockHash
                );
                // check blockHash last, it encompasses much of the above
                result.eventBlock.meta.blockHash.should.equal(
                  results.alphaBlock.eventBlock.meta.blockHash);
                callback(null, result);
              })],
            eventTest: ['betaBlock', (results, callback) => {
              const alphaEvent = results.alphaBlock.eventBlock.block.event;
              const betaEvent = results.betaBlock.eventBlock.block.event;
              async.auto({
                alpha: callback => async.map(alphaEvent, (e, callback) =>
                  helpers.testHasher(e, callback), callback),
                beta: callback => async.map(betaEvent, (e, callback) =>
                  helpers.testHasher(e, callback), callback),
              }, (err, results) => {
                assertNoError(err);
                results.alpha.should.have.same.members(results.beta);
                callback();
              });
            }]
          }, callback)],
        }, done);
      });
      it('two nodes reach consensus on a 3rd block', function(done) {
        this.timeout(120000);
        const testNodes = {
          alpha: nodes.alpha,
          beta: nodes.beta
        };
        async.auto({
          alphaAddEvent1: callback => testNodes.alpha.consensus._events.add(
            helpers.createEventBasic({eventTemplate}),
            testNodes.alpha, callback),
          // beta will merge its new regular event
          workCycle1: ['alphaAddEvent1', (results, callback) => {
            console.log('WORKER CYCLE 1 ---------------------');
            _workerCycle({consensusApi, nodes: testNodes}, callback);
          }],
          workCycle2: ['workCycle1', (results, callback) => {
            console.log('WORKER CYCLE 2 ---------------------');
            _workerCycle({consensusApi, nodes: testNodes}, callback);
          }],
          workCycle3: ['workCycle2', (results, callback) => {
            console.log('WORKER CYCLE 3 ---------------------');
            _workerCycle({consensusApi, nodes: testNodes}, callback);
          }],
          // workCycle4: ['workCycle3', (results, callback) =>
          //   _workerCycle({consensusApi, nodes}, callback)],

          // run workers on both nodes to equalize events
          betaWorker1: ['workCycle3', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, callback)],
          alphaWorker1: ['betaWorker1', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.alpha}, callback)],
          betaWorker2: ['alphaWorker1', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, callback)],
          // new merge events are continually generated on alpha, must end
          // with a beta worker to equalize the events

          test10: ['betaWorker2', (results, callback) => async.auto({
            alpha: callback => nodes.alpha.storage.events.collection.find({})
              .toArray((err, result) => {
                assertNoError(err);
                // result.should.have.length(23);
                callback(err, result.map(e => e.eventHash));
              }),
            beta: ['alpha', (results, callback) =>
              nodes.beta.storage.events.collection.find({})
                .toArray((err, result) => {
                  assertNoError(err);
                  // result.should.have.length(23);
                  // result.map(e => e.eventHash)
                  //   .should.have.same.members(results.alpha);
                  callback();
                })],
            alphaBlock: ['beta', (results, callback) =>
              nodes.alpha.storage.blocks.getLatest(
                (err, result) => {
                  assertNoError(err);
                  result.eventBlock.block.blockHeight.should.equal(3);
                  callback(null, result);
                })],
            betaBlock: ['alphaBlock', (results, callback) =>
              nodes.beta.storage.blocks.getLatest(
                (err, result) => {
                  assertNoError(err);
                  result.eventBlock.block.blockHeight.should.equal(3);
                  result.eventBlock.block.previousBlockHash.should.equal(
                    results.alphaBlock.eventBlock.block.previousBlockHash
                  );
                  result.eventBlock.meta.blockHash.should.equal(
                    results.alphaBlock.eventBlock.meta.blockHash);
                  callback(null, result);
                })],
            eventTest: ['betaBlock', (results, callback) => {
              const alphaEvent = results.alphaBlock.eventBlock.block.event;
              const betaEvent = results.betaBlock.eventBlock.block.event;
              async.auto({
                alpha: callback => async.map(alphaEvent, (e, callback) =>
                  helpers.testHasher(e, callback), callback),
                beta: callback => async.map(betaEvent, (e, callback) =>
                  helpers.testHasher(e, callback), callback),
              }, (err, results) => {
                assertNoError(err);
                results.alpha.should.have.same.members(results.beta);
                callback();
              });
            }]
          }, callback)],
        }, done);
      });

      // NOTE: `nodes` includes 4 nodes, nodes gamma and delta have not been
      // participating in the ledger until now. Each iteration of the test
      // 1. add new regular event on each node
      // 2. run worker on all nodes
      // 3. report blockheight and event counts
      it('makes many more blocks', function(done) {
        this.timeout(900000);
        if(blessedEnabled) {
          screen.render();
        }
        let blockStartTime = Date.now();
        let blockTime = 0;
        let maxBlockHeight = 0;
        if(blessedEnabled) {
          table = blessed.listtable({parent: screen,
            top: 'center',
            left: 'center',
            // data: [['A', 'B', 'C', 'D', 'E']],
            border: 'line',
            align: 'center',
            keys: true,
            width: '75%',
            height: '75%',
            vi: false,
            name: 'table',
            style: {
              bg: 'white',
              cell: {
                fg: 'white',
                bg: 'blue',
                border: {
                  fg: '#f0f0f0'
                },
              },
              header: {
                fg: 'white',
                bg: 'magenta',
                border: {
                  fg: '#f0f0f0'
                },
              }
            }
          });
          screen.append(table);
          tableHead = blessed.listtable({parent: screen,
            top: '500',
            left: '10',
            // data: [['A', 'B', 'C', 'D', 'E']],
            border: 'line',
            align: 'center',
            keys: true,
            width: '75%',
            height: '30%',
            vi: false,
            name: 'table',
            style: {
              bg: 'white',
              cell: {
                fg: 'white',
                bg: 'blue',
                border: {
                  fg: '#f0f0f0'
                },
              },
              header: {
                fg: 'white',
                bg: 'magenta',
                border: {
                  fg: '#f0f0f0'
                },
              }
            }
          });
          screen.append(tableHead);
        }
        let tableHeadData;
        let counterHead = -1;
        const blockMap = {};
        const tracerEvent = {};
        async.timesSeries(1000, (i, callback) => {
          counterHead = -1;
          tableData = [
            ['label'], ['blockHeight'], ['block time'],
            ['block events'], ['block proof events'], ['events'],
            ['iteration', i.toString()], ['head']
          ];
          tableHeadData = [['label'], [''], ['localHead']];
          async.auto({
            alphaAddEvent1: callback => callbackify(helpers.addEvent)(
              {count: 1, eventTemplate, ledgerNode: nodes.alpha}, callback),
            betaAddEvent1: callback => callbackify(helpers.addEvent)(
              {count: 1, eventTemplate, ledgerNode: nodes.beta}, callback),
            gammaAddEvent1: callback => callbackify(helpers.addEvent)(
              {count: 1, eventTemplate, ledgerNode: nodes.gamma}, callback),
            deltaAddEvent1: callback => callbackify(helpers.addEvent)(
              {count: 1, eventTemplate, ledgerNode: nodes.delta}, callback),
            workCycle1: [
              'alphaAddEvent1', 'betaAddEvent1',
              'gammaAddEvent1', 'deltaAddEvent1',
              (results, callback) =>
                _workerCycle({consensusApi, nodes, series: false}, callback)],
            setTracer: ['workCycle1', (results, callback) => {
              if(i % tracerInterval === 0) {
                tracerEvent.alpha = Object.keys(results.alphaAddEvent1)[0];
                tracerEvent.beta = Object.keys(results.betaAddEvent1)[0];
                tracerEvent.gamma = Object.keys(results.gammaAddEvent1)[0];
                tracerEvent.delta = Object.keys(results.deltaAddEvent1)[0];
              }
              tableData.push([`alpha-${tracerEvent.alpha.substring(50)}`]);
              tableData.push([`beta-${tracerEvent.beta.substring(50)}`]);
              tableData.push([`gamma-${tracerEvent.gamma.substring(50)}`]);
              tableData.push([`delta-${tracerEvent.delta.substring(50)}`]);
              callback();
            }],
            report: ['setTracer', (results, callback) => async.auto({
              // get each peers own head
              peerHead: callback => async.eachOfSeries(
                nodes, (ledgerNode, iNode, callback) =>
                  consensusApi._history.getHead({
                    peerId: peers[iNode],
                    ledgerNode
                  }, (err, result) => {
                    if(err) {
                      return callback(err);
                    }
                    heads[iNode].push(result);
                    callback();
                  }), callback),
              generateReport: ['peerHead', (results, callback) =>
                async.forEachOfSeries(nodes, (ledgerNode, i, callback) => {
                  const offsetHead = 3;
                  counterHead++;
                  const shortHead = heads[i][heads[i].length - 1].substring(50);
                  tableHeadData[2].push(shortHead);
                  tableHeadData[offsetHead + counterHead] = [i];
                  tableData[0].push(i);
                  tableData[8].push(shortHead);
                  // add column header for each node (alpha, beta ...)
                  // 0 element is already setup with 'label'
                  tableHeadData[0].push(i);
                  async.auto({
                    alphaTracer: callback => ledgerNode.storage.events.exists(
                      tracerEvent.alpha, (err, result) => {
                        if(err) {
                          return callback(err);
                        }
                        tableData[9].push(result.toString());
                        callback();
                      }),
                    betaTracer: callback => ledgerNode.storage.events.exists(
                      tracerEvent.beta, (err, result) => {
                        if(err) {
                          return callback(err);
                        }
                        tableData[10].push(result.toString());
                        callback();
                      }),
                    gammaTracer: callback => ledgerNode.storage.events.exists(
                      tracerEvent.gamma, (err, result) => {
                        if(err) {
                          return callback(err);
                        }
                        tableData[11].push(result.toString());
                        callback();
                      }),
                    deltaTracer: callback => ledgerNode.storage.events.exists(
                      tracerEvent.delta, (err, result) => {
                        if(err) {
                          return callback(err);
                        }
                        tableData[12].push(result.toString());
                        callback();
                      }),
                    blockHeight: callback =>
                      ledgerNode.storage.blocks.getLatest(
                        (err, result) => {
                          assertNoError(err);
                          const block = result.eventBlock.block;
                          const blockHeight = block.blockHeight;
                          tableData[1].push(blockHeight.toString());
                          if(blockHeight > maxBlockHeight) {
                            // possibly more than 1 block created
                            const blocks = blockHeight - maxBlockHeight;
                            blockTime = (Date.now() - blockStartTime) / blocks;
                            blockStartTime = Date.now();
                            maxBlockHeight = blockHeight;
                          }
                          tableData[2].push(
                            (blockTime / 1000).toFixed(3).toString());
                          tableData[3].push(block.event.length.toString());
                          callback();
                        }),
                    events: callback =>
                      ledgerNode.storage.events.collection.find({})
                        .count((err, result) => {
                          assertNoError(err);
                          tableData[5].push(result.toString());
                          // reportText += `events ${result}\n`;
                          // console.log('  events', result);
                          callback();
                        }),
                    consensus: callback =>
                      ledgerNode.storage.events.collection.find({
                        'meta.consensus': true
                      }).count((err, result) => {
                        assertNoError(err);
                        tableData[6].push(result.toString());
                        callback();
                      }),
                    head: callback => {
                      // for the node being reported on, find out what heads it
                      // has for all the other nodes
                      async.eachOfSeries(
                        nodes, (ledgerNode, iNode, callback) =>
                          consensusApi._history.getHead({
                            peerId: peers[iNode],
                            ledgerNode
                          }, (err, result) => {
                            if(err) {
                              return callback(err);
                            }
                            const relativeHead = heads[iNode].indexOf(result);
                            if(relativeHead === -1) {
                              tableHeadData[offsetHead + counterHead]
                                .push('unk');
                              return callback();
                            }
                            // NOTE: this shows that heads are moving
                            // tableHeadData[offsetHead + counterHead].push(
                            //   relativeHead.toString());
                            tableHeadData[offsetHead + counterHead].push(
                              (heads[iNode].length - 1 -
                                relativeHead).toString());
                            callback();
                          }), callback);
                    }
                  }, callback);
                }, callback)]
            }, callback)]
          }, err => {
            if(err) {
              return callback(err);
            }
            // console.log('Heads', heads);
            // console.log('TTTTTTt', tableHeadData);
            if(blessedEnabled) {
              table.setData(tableData);
              tableHead.setData(tableHeadData);
            }
            const summary = blessedSummary(tableData);
            // console.log('Summary', JSON.stringify(summary, null, 2));
            for(const node in summary) {
              const n = summary[node];
              if(!blockMap[n.blockHeight.toString()]) {
                blockMap[n.blockHeight.toString()] = n['block events'];
              } else if(
                blockMap[n.blockHeight.toString()] !== n['block events']) {
                if(blessedEnabled) {
                  screen.destroy();
                }
                console.log('EVENT MISMATCH at', n.blockHeight);
                console.log(
                  blockMap[n.blockHeight.toString()], ' !== ',
                  n['block events']);
                // DO OTHER LOGGING
                console.log('Summary',
                  JSON.stringify(blessedSummary(tableData), null, 2));
                throw new Error('EVENT MISMATCH');
              }
            }
            if(blessedEnabled) {
              screen.render();
            }
            callback();
          });
        }, err => {
          if(blessedEnabled) {
            screen.destroy();
          }
          console.log(
            'Summary', JSON.stringify(blessedSummary(tableData), null, 2));
          done(err);
        });
      });
    }); // end one block
    describe.skip('More Blocks', () => {
      it('should add an event and achieve consensus', function(done) {
        console.log('ALPHA COLL',
          nodes.alpha.storage.events.collection.collectionName);
        this.timeout(120000);
        const eventTemplate = mockData.events.alpha;
        async.auto({
          addEvent: callback => nodes.beta.consensus._events.add(
            helpers.createEventBasic({eventTemplate}), nodes.beta, callback),
          // this will merge event on peer[1] and transmit to peer[0]
          worker1: ['addEvent', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, callback)],
          count1: ['worker1', (results, callback) =>
            async.each(nodes, (ledgerNode, callback) => {
              ledgerNode.storage.events.collection.find({})
                .toArray((err, result) => {
                  assertNoError(err);
                  result.should.have.length(21);
                  callback();
                });
            }, callback)],
          // this should merge events from beta and create a new block
          worker2: ['count1', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.alpha}, err => {
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
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
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
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                console.log('AFTER WORKER4 -----------------------------');
                assertNoError(err);
                callback(err);
              })],
          test2: ['worker4', (results, callback) =>
            nodes.beta.storage.blocks.getLatest((err, result) => {
              assertNoError(err);
              result.eventBlock.block.blockHeight.should.equal(1);
              callback();
            })],
          // // add another event on beta
          addEvent2: ['test2', (results, callback) =>
            nodes.beta.consensus._events.add(
              helpers.createEventBasic({eventTemplate}), nodes.beta, callback)],
          // this iteration only transmit that merge event that beta created
          // after the alpha merge event
          worker5: ['addEvent2', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                console.log('AFTER WORKER4 -----------------------------');
                callback(err);
              })],
          worker6: ['worker5', (results, callback) =>
            callbackify(consensusApi._worker._run)(
              {ledgerNode: nodes.beta}, err => {
                assertNoError(err);
                console.log('AFTER WORKER5 -----------------------------');
                callback(err);
              })],
        }, done);
      });
    }); // end block 1
  });
});

function blessedSummary(tableData) {
  const sum = {};
  // skip 0 because that's `label` header
  for(let i = 1; i < tableData[0].length; ++i) {
    sum[tableData[0][i]] = {};
  }
  // now start with row 1, because row 0 is the column headers
  for(let i = 1; i < tableData.length; ++i) {
    const rowLabel = tableData[i][0];
    for(let n = 1; n < tableData[0].length; ++n) {
      sum[tableData[0][n]][rowLabel] = tableData[i][n];
    }
  }
  return sum;
}

function _workerCycle({consensusApi, nodes, series = false}, callback) {
  const func = series ? async.eachSeries : async.each;
  func(nodes, (ledgerNode, callback) =>
    callbackify(consensusApi._worker._run)({ledgerNode}, callback), callback);
}
