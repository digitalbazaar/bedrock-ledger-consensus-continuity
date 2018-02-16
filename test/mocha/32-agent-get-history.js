/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
// let request = require('request');
// request = request.defaults({json: true, strictSSL: false});
const uuid = require('uuid/v4');

// FIXME: these tests will need to supply `creatorHeads` in the request
describe.skip('Consensus Agent - Get History API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let genesisMerge;
  let getHistory;
  let getRecentHistory;
  let ledgerNode;
  let ledgerNodeBeta;
  let mergeBranches;
  let peerId;
  let eventHash;
  let testEventId;
  beforeEach(done => {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedgerNode.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          getHistory = consensusApi._worker._client.getHistory;
          getRecentHistory = consensusApi._worker._events.getRecentHistory;
          mergeBranches = consensusApi._worker._events.mergeBranches;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNode = result;
          callback(null, result);
        })],
      creator: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          if(err) {
            return callback(err);
          }
          peerId = result.id;
          callback(null, result);
        });
      }],
      genesisMerge: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          ledgerNode: ledgerNode.id,
          eventsCollection: ledgerNode.storage.events.collection,
          creatorId: peerId
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMerge = result;
          callback();
        });
      }],
      addEvent: ['ledgerNode', (results, callback) =>
        ledgerNode.consensus._events.add(
          testEvent, ledgerNode, (err, result) => {
          eventHash = result.meta.eventHash;
          callback();
        })],
      genesisBlock: ['ledgerNode', (results, callback) =>
        ledgerNode.blocks.getGenesis((err, result) => {
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
          ledgerNodeBeta = result;
          callback(null, result);
        })]
    }, done);
  });
  it('should return an empty array', done => {
    async.auto({
      history: callback => getHistory(
        {peerId, treeHash: genesisMerge}, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('object');
          should.exist(result.history);
          result.history.should.have.length(0);
          callback();
        })
    }, done);
  });
  it('should return one local merge event', done => {
    async.auto({
      // merge the local event added in `before`
      history1: callback => getRecentHistory({ledgerNode}, callback),
      mergeBranches: ['history1', (results, callback) => mergeBranches(
        {history: results.history1, ledgerNode}, callback)],
      history2: ['mergeBranches', (results, callback) => getHistory(
        {peerId, treeHash: genesisMerge}, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('object');
          result.history.should.be.an('array');
          result.history.should.have.length(1);
          result.history.should.have.same.members([
            results.mergeBranches.meta.eventHash]);
          callback();
        })]
    }, done);
  });

  /* history includes:
       one local event
       one local merge event
       one remote event
       one remote merge event
  */
  it('returns a local merge and a remote merge event ', done => {
    async.auto({
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode, mockData}, callback),
      history1: ['remoteEvents', (results, callback) =>
        getRecentHistory({ledgerNode}, callback)],
      mergeBranches: ['history1', (results, callback) =>
        mergeBranches({history: results.history1, ledgerNode}, callback)],
      history2: ['mergeBranches', (results, callback) => getHistory(
        {peerId, treeHash: genesisMerge}, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('object');
          result.history.should.be.an('array');
          result.history.should.have.length(2);
          result.history.should.have.same.members([
            results.remoteEvents.merge,
            results.mergeBranches.meta.eventHash
          ]);
          callback();
        })]
    }, done);
  });

  /* history includes:
       one local event
       one local merge event
       two remote events
       two remote merge events
  */
  it('returns a local merge and two remote merge event ', done => {
    const getHistory = consensusApi._worker._client.getHistory;
    async.auto({
      remoteEvents: callback => async.times(2, (i, callback) =>
        helpers.addRemoteEvents(
          {consensusApi, ledgerNode, mockData}, callback), callback),
      history1: ['remoteEvents', (results, callback) =>
        getRecentHistory({ledgerNode}, callback)],
      mergeBranches: ['history1', (results, callback) =>
        mergeBranches({history: results.history1, ledgerNode}, callback)],
      history2: ['mergeBranches', (results, callback) => getHistory(
        {peerId, treeHash: genesisMerge}, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('object');
          result.history.should.be.an('array');
          result.history.should.have.length(3);
          result.history.should.have.same.members([
            ...results.remoteEvents.map(e => e.merge),
            results.mergeBranches.meta.eventHash
          ]);
          callback();
        })]
    }, done);
  });

  /* history includes:
       one local regular event
       one local merge event
       one merge event on ledgerNodeBeta
       one regular event on fictitious node (added via ledgerNodeBeta)
       one merge event on fictitious node (added via ledgerNodeBeta)
  */
  /* in this example there are two actual ledger nodes:
     ledgerNode, ledgerNodeBeta.  A regular event and a merge event from a
     fictitious third node are added at ledgerNodeBeta.  Those events are
     merged at ledgerNodeBeta.
  */
  // expected result from getHistory is three merge events
  //   one from ledgerNode
  //   one from ledgerNodeBeta
  //   one from fictitious node
  it('returns merge events from three different nodes', done => {
    async.auto({
      remoteEventsBeta: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode: ledgerNodeBeta, mockData}, callback),
      history1: ['remoteEventsBeta', (results, callback) =>
        getRecentHistory({ledgerNode: ledgerNodeBeta}, callback)],
      mergeBranchesBeta: ['history1', (results, callback) => mergeBranches(
        {history: results.history1, ledgerNode: ledgerNodeBeta}, callback)],
      fromBeta: ['mergeBranchesBeta', (results, callback) => {
        const treeHash = results.mergeBranchesBeta.event.treeHash;
        const eventHash = results.mergeBranchesBeta.meta.eventHash;
        // copy the mergeEvent on ledgerNodeBeta and its ancestors to ledgerNode
        _copyEvents(
          {eventHash, from: ledgerNodeBeta, to: ledgerNode, treeHash},
          callback);
      }],
      history2: ['fromBeta', (results, callback) =>
        getRecentHistory({ledgerNode}, callback)],
      mergeBranches: ['history2', (results, callback) =>
        mergeBranches({history: results.history2, ledgerNode}, callback)],
      history3: ['mergeBranches', (results, callback) => getHistory(
        {peerId, treeHash: genesisMerge}, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('object');
          result.history.should.be.an('array');
          result.history.should.have.length(3);
          result.history.should.have.same.members([
            results.remoteEventsBeta.merge,
            results.mergeBranchesBeta.meta.eventHash,
            results.mergeBranches.meta.eventHash,
          ]);
          callback();
        })]
    }, done);
  });
});

// FIXME: use helpers
function _copyEvents({eventHash, from, to, treeHash}, callback) {
  async.auto({
    // events: callback => async.map(events, (e, callback) =>
    //   from.events.get(e, callback), callback),
    events: callback => {
      const collection = from.storage.events.collection;
      collection.aggregate([
        {$match: {eventHash}},
        {
          $graphLookup: {
            from: collection.s.name,
            startWith: '$eventHash',
            connectFromField: "event.parentHash",
            connectToField: "eventHash",
            as: "_parents",
            restrictSearchWithMatch: {eventHash: {$ne: treeHash}}
          },
        },
        {$unwind: '$_parents'},
        {$replaceRoot: {newRoot: '$_parents'}},
        // the order of events is unpredictable without this sort, and we
        // must ensure that events are added in chronological order
        {$sort: {'meta.created': 1}}
      ], callback);
    },
    add: ['events', (results, callback) => {
      async.eachSeries(results.events, (e, callback) =>
        to.consensus._events.add(
          e.event, to, {continuity2017: {peer: true}}, callback), callback);
    }]
  }, callback);
}
