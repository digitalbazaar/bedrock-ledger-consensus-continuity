/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const uuid = require('uuid/v4');
const util = require('util');

const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

describe.only('Election API _findMergeEventProof', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let genesisMerge;
  let eventHash;
  let testEventId;
  const nodes = {};
  const peers = {};
  beforeEach(done => {
    const configEvent = mockData.events.config;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedgerNode.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {configEvent}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.alpha = result;
          callback(null, result);
        })],
      genesisMerge: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          eventsCollection: nodes.alpha.storage.events.collection
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMerge = result;
          callback();
        });
      }],
      genesisBlock: ['ledgerNode', (results, callback) =>
        nodes.alpha.blocks.getGenesis((err, result) => {
          if(err) {
            return callback(err);
          }
          callback(null, result.genesisBlock.block);
        })],
      nodeBeta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.beta = result;
          callback(null, result);
        })],
      nodeGamma: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.gamma = result;
          callback(null, result);
        })],
      nodeDelta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.delta = result;
          callback(null, result);
        })],
      // nodeEpsilon: ['genesisBlock', (results, callback) => brLedgerNode.add(
      //   null, {genesisBlock: results.genesisBlock}, (err, result) => {
      //     if(err) {
      //       return callback(err);
      //     }
      //     nodes.epsilon = result;
      //     callback(null, result);
      //   })],
      creator: ['nodeDelta', (results, callback) =>
        async.eachOf(nodes, (n, i, callback) =>
          consensusApi._worker._voters.get(n.id, (err, result) => {
            if(err) {
              return callback(err);
            }
            peers[i] = result.id;
            callback();
          }), callback)]
    }, done);
  });
  it('does something', done => {
    const getRecentHistory = consensusApi._worker._events.getRecentHistory;
    const _getElectorBranches =
      consensusApi._worker._election._getElectorBranches;
    const _findMergeEventProof =
      consensusApi._worker._election._findMergeEventProof
    async.auto({
      regularEvent: callback => async.each(nodes, (n, callback) => {
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEventId = 'https://example.com/events/' + uuid();
        testEvent.input[0].id = testEventId;
        n.events.add(testEvent, callback);
      }, callback),
      merge1: ['regularEvent', (results, callback) =>
        _mergeOn({nodes}, callback)],
      cp1: ['merge1', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge1.alpha,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp2: ['merge1', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge1.delta,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      merge2: ['cp1', 'cp2', (results, callback) => {
        const {beta, gamma} = nodes;
        _mergeOn({nodes: {beta, gamma}}, callback);
      }],
      cp3: ['merge2', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge2.beta,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp4: ['merge2', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge2.gamma,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      merge3: ['cp3', 'cp4', (results, callback) => {
        const {beta, gamma} = nodes;
        _mergeOn({nodes: {beta, gamma}}, callback);
      }],
      cp5: ['merge3', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge3.beta,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp6: ['merge3', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge3.gamma,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      merge4: ['cp5', 'cp6', (results, callback) => {
        const {alpha, delta} = nodes;
        _mergeOn({nodes: {alpha, delta}}, callback);
      }],
      cp7: ['merge4', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge4.alpha,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp8: ['merge4', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge4.delta,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      merge5: ['cp7', 'cp8', (results, callback) => {
        const {beta, gamma} = nodes;
        _mergeOn({nodes: {beta, gamma}}, callback);
      }],
      cp9: ['merge5', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge5.beta,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp10: ['merge5', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge5.gamma,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      merge6: ['cp9', 'cp10', (results, callback) => {
        const {beta, gamma} = nodes;
        _mergeOn({nodes: {beta, gamma}}, callback);
      }],
      // start steps 13 and 14 in prose
      cp11: ['merge6', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge6.beta,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp12: ['merge6', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge6.gamma,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      merge7: ['cp11', 'cp12', (results, callback) => {
        const {beta, gamma} = nodes;
        _mergeOn({nodes: {beta, gamma}}, callback);
      }],
      // steps 15 and 16
      cp13: ['merge7', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge7.beta,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp14: ['merge7', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge7.gamma,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      merge8: ['cp13', 'cp14', (results, callback) => {
        const {alpha, delta} = nodes;
        _mergeOn({nodes: {alpha, delta}}, callback);
      }],
      // steps 17 and 18
      cp15: ['merge8', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge8.alpha,
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp16: ['merge8', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge8.delta,
        from: nodes.delta,
        to: nodes.gamma
      }, callback)],
      merge9: ['cp15', 'cp16', (results, callback) => {
        const {beta, gamma} = nodes;
        _mergeOn({nodes: {beta, gamma}}, callback);
      }],
      // steps 19 and 20
      cp17: ['merge9', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge9.beta,
        from: nodes.beta,
        to: nodes.gamma
      }, callback)],
      cp18: ['merge9', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge9.gamma,
        from: nodes.gamma,
        to: nodes.beta
      }, callback)],
      merge10: ['cp17', 'cp18', (results, callback) => {
        const {beta, gamma} = nodes;
        _mergeOn({nodes: {beta, gamma}}, callback);
      }],
      // steps 21 and 22
      cp19: ['merge10', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge10.beta,
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      cp20: ['merge10', (results, callback) => _copyFromMerge({
        mergeEvent: results.merge10.gamma,
        from: nodes.gamma,
        to: nodes.delta
      }, callback)],
      merge11: ['cp19', 'cp20', (results, callback) => {
        const {alpha, delta} = nodes;
        _mergeOn({nodes: {alpha, delta}}, callback);
      }],
      testAlpha: ['merge11', (results, callback) => {
        // all peers are electors
        const electors = Object.values(peers);
        const ledgerNode = nodes.alpha;
        console.log('EEELLLLL', electors);
        async.auto({
          history: callback =>
            getRecentHistory({ledgerNode}, callback),
          branches: ['history', (results, callback) => {
            const branches = _getElectorBranches(
              {
                event: results.history
                  .eventMap[results.history.localBranchHead],
                electors
              });
            callback(null, branches);
          }],
          proof: ['branches', (results, callback) => {
            const proof = _findMergeEventProof({
              electors,
              ledgerNode,
              tail: results.branches.tail
            });
            console.log('PROOF', util.inspect(proof));
            callback();
          }]
        }, callback);
      }]
    }, done);
  });
});

function _mergeOn({nodes}, callback) {
  // FIXME: get mergeBranches by some other reference
  const mergeBranches = consensusApi._worker._events.mergeBranches;
  const events = {};
  async.eachOf(nodes, (n, i, callback) =>
    mergeBranches({ledgerNode: n}, (err, result) => {
      events[i] = result;
      callback(err);
    }), err => callback(err, events));
}

function _copyFromMerge({from, mergeEvent, to}, callback) {
  const treeHash = mergeEvent.event.treeHash;
  const eventHash = mergeEvent.meta.eventHash;
  _copyEvents({eventHash, from, to, treeHash}, callback);
}

// FIXME: make this a helper
function _copyEvents({eventHash, from, to, treeHash}, callback) {
  async.auto({
    events: callback => {
      const collection = from.storage.events.collection;
      // FIXME: use a more efficient query, the commented aggregate function
      // is evidently missing some events.
      collection.find({
        'meta.consensus': {$exists: false}
      }).sort({$natural: 1}).toArray(callback);
      // collection.aggregate([
      //   {$match: {eventHash}},
      //   {
      //     $graphLookup: {
      //       from: collection.s.name,
      //       startWith: '$eventHash',
      //       connectFromField: "event.parentHash",
      //       connectToField: "eventHash",
      //       as: "_parents",
      //       restrictSearchWithMatch: {
      //         eventHash: {$ne: treeHash},
      //         'meta.consensus': {$exists: false}
      //       }
      //     },
      //   },
      //   {$unwind: '$_parents'},
      //   {$replaceRoot: {newRoot: '$_parents'}},
      //   // the order of events is unpredictable without this sort, and we
      //   // must ensure that events are added in chronological order
      //   {$sort: {'meta.created': 1}}
      // ], callback);
    },
    add: ['events', (results, callback) => {
      async.eachSeries(results.events, (e, callback) => {
        to.events.add(e.event, {continuity2017: {peer: true}}, err => {
          // FIXME: only ignore dup error
          // ignore errors
          callback();
        });
      }, callback);
    }]
  }, callback);
}
