/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const gossipCycle = require('./gossip-cycle');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

describe.skip('Worker - _gossipWith', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let aggregateHistory;
  let cacheKey;
  let consensusApi;
  let genesisMergeHash;
  let getRecentHistory;
  let mergeBranches;
  let testEventId;
  let EventWriter;
  const nodeCount = 4;
  // NOTE: alpha is assigned manually
  const nodeLabels = ['beta', 'gamma', 'delta', 'epsilon'];
  const nodes = {};
  const peers = {};
  beforeEach(function(done) {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      flush: callback => cache.client.flushall(callback),
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedgerNode.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          getRecentHistory = consensusApi._worker._events.getRecentHistory;
          mergeBranches = consensusApi._worker._events.mergeBranches;
          aggregateHistory = consensusApi._worker._events.aggregateHistory;
          cacheKey = consensusApi._worker._cacheKey;
          EventWriter = consensusApi._worker.EventWriter;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.alpha = result;
          callback();
        })],
      genesisBlock: ['ledgerNode', (results, callback) =>
        nodes.alpha.blocks.getGenesis((err, result) => {
          if(err) {
            return callback(err);
          }
          callback(null, result.genesisBlock.block);
        })],
      createNodes: ['genesisBlock', (results, callback) => {
        async.times(nodeCount - 1, (i, callback) => brLedgerNode.add(null, {
          genesisBlock: results.genesisBlock,
        }, (err, ledgerNode) => {
          if(err) {
            return callback(err);
          }
          nodes[nodeLabels[i]] = ledgerNode;
          callback();
        }), callback);
      }],
      getPeer: ['createNodes', (results, callback) =>
        async.eachOf(nodes, (ledgerNode, i, callback) =>
          consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
            peers[i] = result.id;
            callback();
          }), callback)],
      genesisMerge: ['consensusPlugin', 'getPeer', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          creatorId: peers.alpha, ledgerNode: nodes.alpha
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMergeHash = result.eventHash;
          callback();
        });
      }],
    }, done);
  });
  /*
    gossip wih ledgerNode from nodes.beta, there is no merge event on
    ledgerNode beyond the genesis merge event, so the gossip should complete
    without an error.  There is also nothing to be sent.
  */
  it('completes without an error when nothing to be received', done => {
    async.auto({
      gossipWith: callback => consensusApi._worker._gossipWith({
        ledgerNode: nodes.beta, peerId: peers.alpha
      }, err => {
        assertNoError(err);
        callback();
      })
    }, done);
  });
  /*
    gossip wih ledgerNode from nodes.beta. There is a regular event and a
    merge event on ledgerNode to be gossiped.
  */
  it.only('properly gossips one regular event and one merge event', done => {
    const eventTemplate = mockData.events.alpha;
    async.auto({
      addEvent: callback => helpers.addEventAndMerge({
        consensusApi, creatorId: peers.alpha, eventTemplate,
        ledgerNode: nodes.alpha
      }, callback),
      gossipWith: ['addEvent', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: nodes.beta, peerId: peers.alpha}, err => {
            assertNoError(err);
            callback();
          })],
      testCache: ['gossipWith', (results, callback) => {
        // the events from alpha should now be present in the cache on beta
        const ledgerNodeId = nodes.beta.id;
        const regularEventHash = results.addEvent.regularHashes[0];
        const regularEventKey = cacheKey.event(
          {eventHash: regularEventHash, ledgerNodeId});
        const mergeHash = results.addEvent.mergeHash;
        const mergeHashKey = cacheKey.event(
          {eventHash: mergeHash, ledgerNodeId});
        const hashKeys = [regularEventKey, mergeHashKey];
        // const hashKeys = results.addEvent.allHashes.map(eventHash =>
        //   cacheKey.event({eventHash, ledgerNodeId}));
        cache.client.multi()
          .lrange(cacheKey.eventQueue(ledgerNodeId), 0, 100)
          .exists(hashKeys)
          .exec((err, result) => {
            assertNoError(err);
            const eventQueue = result[0];
            eventQueue.should.have.length(2);
            // ensure that events are in the proper order
            eventQueue[0].should.equal(regularEventKey);
            eventQueue[1].should.equal(mergeHashKey);
            const existsCount = result[1];
            existsCount.should.equal(2);
            callback();
          });
      }],
      writer: ['testCache', (results, callback) => {
        const ew = new EventWriter({ledgerNode: nodes.beta}, callback);
        ew.start();
        ew.stop(callback);
      }],
      testMongo: ['writer', (results, callback) => {
        nodes.beta.storage.events.exists([
          results.addEvent.regularHashes[0],
          results.addEvent.mergeHash
        ], (err, result) => {
          assertNoError(err);
          result.should.be.true;
          callback();
        });
      }]
    }, done);
  });
  /*
    gossip wih ledgerNode from nodes.beta. There is a regular event and a
    merge event on ledgerNode to be gossiped. There is a regular event and a
    merge event from a fictitious node as well. There is nothing to be sent from
    nodes.beta.
  */
  it('properly gossips two regular events and two merge events', done => {
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
    async.auto({
      addEvent: callback => nodes.alpha.events.add(testEvent, callback),
      remoteEvents: ['addEvent', (results, callback) => helpers.addRemoteEvents(
        {consensusApi, ledgerNode: nodes.alpha, mockData}, callback)],
      writer: ['remoteEvents', (results, callback) => {
        const eventWriter = new EventWriter({ledgerNode: nodes.alpha});
        eventWriter.start();
        eventWriter.stop(callback);
      }],
      history: ['addEvent', 'writer', (results, callback) =>
        getRecentHistory(
          {creatorId: peers.alpha, ledgerNode: nodes.alpha}, callback)],
      mergeBranches: ['history', (results, callback) => mergeBranches(
        {history: results.history, ledgerNode: nodes.alpha}, callback)],
      gossipWith: ['mergeBranches', (results, callback) =>
        consensusApi._worker._gossipWith(
          {ledgerNode: nodes.beta, peerId: peers.alpha}, err => {
            assertNoError(err);
            callback();
          })],
      writerBeta: ['gossipWith', (results, callback) => {
        const ew = new EventWriter({ledgerNode: nodes.beta}, callback);
        ew.start();
        ew.stop(callback);
      }],
      test: ['writerBeta', (results, callback) => {
        // the events from ledgerNode should now be present on nodes.beta
        nodes.beta.storage.events.exists([
          // results.remoteEvents.merge,
          // results.remoteEvents.regular,
          results.addEvent.meta.eventHash,
          results.mergeBranches.meta.eventHash
        ], (err, result) => {
          assertNoError(err);
          result.should.be.true;
          callback();
        });
      }]
    }, done);
  });
  /*
    beta gossips with alpha, gamma gossips with alpha, beta gossips with gamma.
    Afterwards, all nodes have the same events.
  */
  it('properly gossips among three nodes', done => {
    const eventTemplate = mockData.events.alpha;
    const testNodes =
      {alpha: nodes.alpha, beta: nodes.beta, gamma: nodes.gamma};
    async.auto({
      addEvent: callback => helpers.addEventMultiNode(
        {consensusApi, eventTemplate, nodes: testNodes, peers}, callback),
      writeAll1: ['addEvent', (results, callback) =>
        async.each(testNodes, (ledgerNode, callback) =>
          _commitCache(ledgerNode, callback), callback)],
      gossipWith: ['writeAll1', (results, callback) => async.series([
        // beta to alpha
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.beta, peerId: peers.alpha}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.beta, callback),
        // gamma to alpha
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.gamma, peerId: peers.alpha}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.gamma, callback),
        // gamma to beta
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.gamma, peerId: peers.beta}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.gamma, callback),
        // beta to gamma
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.beta, peerId: peers.gamma}, (err, result) => {
            assertNoError(err);
            result.creatorHeads.heads[peers.alpha].eventHash
              .should.equal(results.addEvent.alpha.mergeHash);
            // this is head that beta is sending to gamma for itself
            result.creatorHeads.heads[peers.beta].eventHash
              .should.equal(results.addEvent.beta.mergeHash);
            // beta must send genesisMergeHash as head
            result.creatorHeads.heads[peers.gamma].eventHash
              .should.equal(genesisMergeHash);
            callback();
          }),
        callback => _commitCache(nodes.beta, callback),
        // alpha to beta
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.alpha, peerId: peers.beta}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.alpha, callback),
        // alpha to gamma
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.alpha, peerId: peers.gamma}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.alpha, callback),
      ], callback)],
      count: ['gossipWith', (results, callback) => {
        async.eachOfSeries(testNodes, (ledgerNode, i, callback) => {
          ledgerNode.storage.events.collection.count({}, (err, result) => {
            assertNoError(err);
            result.should.equal(8);
            callback();
          });
        }, callback);
      }],
      test: ['count', (results, callback) => {
        // all nodes should have the same events
        async.eachSeries(testNodes, (node, callback) =>
          node.storage.events.exists([
            ...results.addEvent.mergeHash,
            ...results.addEvent.regularHash
          ], (err, result) => {
            assertNoError(err);
            result.should.be.true;
            callback();
          }), callback);
      }]
    }, done);
  });
  it.skip('properly gossips among three nodes II', done => {
    const eventTemplate = mockData.events.alpha;
    const testNodes =
      {alpha: nodes.alpha, beta: nodes.beta, gamma: nodes.gamma};
    // map to track generations of merge events
    const generations = {
      alpha: [genesisMergeHash],
      beta: [genesisMergeHash],
      gamma: [genesisMergeHash]
    };
    async.auto({
      addEvent: callback => helpers.addEventMultiNode({
        consensusApi, eventTemplate, nodes: testNodes, peers
      }, (err, result) => {
        generations.alpha.push(result.alpha.mergeHash);
        generations.beta.push(result.beta.mergeHash);
        generations.gamma.push(result.gamma.mergeHash);
        callback();
      }),
      writeAll1: ['addEvent', (results, callback) =>
        async.each(testNodes, (ledgerNode, callback) =>
          _commitCache(ledgerNode, callback), callback)],
      gossipWith: ['writeAll1', (results, callback) => async.series([
        // beta to alpha
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.beta, peerId: peers.alpha}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.beta, callback),
        // beta to gamma
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.beta, peerId: peers.gamma}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.beta, callback),
        callback => helpers.addEventAndMerge({
          consensusApi, creatorId: peers.beta, eventTemplate,
          ledgerNode: nodes.beta
        }, (err, result) => {
          assertNoError(err);
          generations.beta.push(result.mergeHash);
          callback();
        }),
        // alpha to beta
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.alpha, peerId: peers.beta}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.alpha, callback),
        callback => helpers.addEventAndMerge({
          consensusApi, creatorId: peers.alpha, eventTemplate,
          ledgerNode: nodes.alpha
        }, (err, result) => {
          assertNoError(err);
          generations.alpha.push(result.mergeHash);
          helpers.report({nodes, peers});
          callback();
        }),
        // gamma to alpha
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.gamma, peerId: peers.alpha}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.gamma, callback),
        // gamma M2
        callback => helpers.addEventAndMerge({
          consensusApi, creatorId: peers.gamma, eventTemplate,
          ledgerNode: nodes.gamma
        }, (err, result) => {
          assertNoError(err);
          generations.gamma.push(result.mergeHash);
          callback();
        }),
        callback => {
          const eventMap = {};
          async.eachOfSeries(testNodes, (ledgerNode, i, callback) => {
            ledgerNode.storage.events.collection.count({}, (err, result) => {
              assertNoError(err);
              eventMap[i] = result;
              callback();
            });
          }, err => {
            assertNoError(err);
            eventMap.alpha.should.equal(12);
            eventMap.beta.should.equal(10);
            // FIXME: 10?
            eventMap.gamma.should.equal(10);
            callback();
          });
        },
        // beta to gamma (fails here)
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.beta, peerId: peers.gamma}, (err, result) => {
            assertNoError(err);
            // these are heads beta is sending to gamma
            result.creatorHeads.heads[peers.alpha].eventHash
              .should.equal(generations.alpha[1]);
            // this is head that beta is sending to gamma for itself
            result.creatorHeads.heads[peers.beta].eventHash
              .should.equal(generations.beta[2]);
            result.creatorHeads.heads[peers.gamma].eventHash
              .should.equal(generations.gamma[1]);
            callback();
          }),
        callback => _commitCache(nodes.beta, callback),
        // alpha to gamma
        callback => consensusApi._worker._gossipWith(
          {ledgerNode: nodes.alpha, peerId: peers.gamma}, err => {
            assertNoError(err);
            callback();
          }),
        callback => _commitCache(nodes.alpha, callback),
      ], callback)],
      count: ['gossipWith', (results, callback) => {
        const eventMap = {};
        async.eachOfSeries(testNodes, (ledgerNode, i, callback) => {
          ledgerNode.storage.events.collection.count({}, (err, result) => {
            assertNoError(err);
            eventMap[i] = result;
            callback();
          });
        }, err => {
          assertNoError(err);
          // console.log('GENERATIONS', JSON.stringify(generations, null, 2));
          // console.log('TTTTTTT', eventMap);
          eventMap.alpha.should.equal(14);
          // FIXME: 14?
          eventMap.beta.should.equal(12);
          // FIXME: 14?
          eventMap.gamma.should.equal(10);
          helpers.report({nodes, peers});
          callback();
        });
      }],
      // test: ['count', (results, callback) => {
      //   // all nodes should have the same events
      //   async.eachSeries(testNodes, (node, callback) =>
      //     node.storage.events.exists([
      //       ...results.addEvent.mergeHash,
      //       ...results.addEvent.regularHash
      //     ], (err, result) => {
      //       assertNoError(err);
      //       result.should.be.true;
      //       callback();
      //     }), callback);
      // }]
    }, done);
  });
  it.skip('performs gossip-cycle alpha 100 times', function(done) {
    this.timeout(120000);
    const eventTemplate = mockData.events.alpha;
    let previousResult;
    async.timesSeries(100, (i, callback) => {
      gossipCycle.alpha(
        {consensusApi, eventTemplate, nodes, peers, previousResult},
        (err, result) => {
          if(err) {
            return callback(err);
          }
          previousResult = result;
          callback();
        });
    }, err => {
      if(err) {
        return done(err);
      }
      done();
    });
  }); // end cycle alpha
  it.skip('performs gossip cycle beta 100 times', function(done) {
    this.timeout(120000);
    const eventTemplate = mockData.events.alpha;
    let previousResult;
    async.timesSeries(100, (i, callback) => {
      gossipCycle.beta(
        {consensusApi, eventTemplate, nodes, peers, previousResult},
        (err, result) => {
          if(err) {
            return callback(err);
          }
          previousResult = result;
          callback();
        });
    }, err => {
      if(err) {
        return done(err);
      }
      done();
    });
  }); // end cycle beta
  it.skip('performs gossip cycle gamma 100 times', function(done) {
    this.timeout(120000);
    const eventTemplate = mockData.events.alpha;
    let previousResult;
    async.timesSeries(100, (i, callback) => {
      gossipCycle.gamma(
        {consensusApi, eventTemplate, nodes, peers, previousResult},
        (err, result) => {
          if(err) {
            return callback(err);
          }
          previousResult = result;
          callback();
        });
    }, err => {
      if(err) {
        return done(err);
      }
      done();
    });
  }); // end cycle gamma
  it.skip('performs gossip cycle delta 100 times', function(done) {
    this.timeout(120000);
    const eventTemplate = mockData.events.alpha;
    let previousResult;
    async.timesSeries(100, (i, callback) => {
      gossipCycle.delta(
        {consensusApi, eventTemplate, nodes, peers, previousResult},
        (err, result) => {
          if(err) {
            return callback(err);
          }
          previousResult = result;
          callback();
        });
    }, err => {
      if(err) {
        return done(err);
      }
      done();
    });
  }); // end cycle delta

  function _commitCache(ledgerNode, callback) {
    const ew = new EventWriter({ledgerNode});
    ew.start();
    ew.stop(callback);
  }
});
