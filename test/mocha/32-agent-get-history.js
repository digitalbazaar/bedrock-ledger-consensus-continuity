/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
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

describe.only('Consensus Agent - Get History API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let genesisMerge;
  let ledgerNode;
  let ledgerNodeBeta;
  let creatorId;
  let eventHash;
  let testEventId;
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
          ledgerNode = result;
          callback(null, result);
        })],
      creator: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          if(err) {
            return callback(err);
          }
          creatorId = result.id;
          callback(null, result);
        });
      }],
      genesisMerge: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          eventsCollection: ledgerNode.storage.events.collection,
          creator: creatorId
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMerge = result;
          callback();
        });
      }],
      addEvent: ['ledgerNode', (results, callback) => ledgerNode.events.add(
        testEvent, (err, result) => {
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
    const getHistory = consensusApi._worker._client.getHistory;
    async.auto({
      history: callback => getHistory(
        genesisMerge, creatorId, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(0);
          callback();
        })
    }, done);
  });
  it('should return one local merge event', done => {
    const getHistory = consensusApi._worker._client.getHistory;
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    async.auto({
      // merge the local event added in `before`
      mergeBranches: callback => mergeBranches(ledgerNode, callback),
      history: ['mergeBranches', (results, callback) => getHistory(
        genesisMerge, creatorId, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(1);
          result.should.have.same.members([
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
    const getHistory = consensusApi._worker._client.getHistory;
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    async.auto({
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode, mockData}, callback),
      mergeBranches: ['remoteEvents', (results, callback) =>
        mergeBranches(ledgerNode, callback)],
      history: ['mergeBranches', (results, callback) => getHistory(
        genesisMerge, creatorId, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(2);
          result.should.have.same.members([
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
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    async.auto({
      remoteEvents: callback => async.times(2, (i, callback) =>
        helpers.addRemoteEvents(
          {consensusApi, ledgerNode, mockData}, callback), callback),
      mergeBranches: ['remoteEvents', (results, callback) =>
        mergeBranches(ledgerNode, callback)],
      history: ['mergeBranches', (results, callback) => getHistory(
        genesisMerge, creatorId, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(3);
          result.should.have.same.members([
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
  it('returns merge events from thee different nodes', done => {
    const getHistory = consensusApi._worker._client.getHistory;
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    // console.log('alphaeventscollection', ledgerNode.storage.events.collection.s.name);
    // console.log('betaeventscollection', ledgerNodeBeta.storage.events.collection.s.name);
    async.auto({
      remoteEventsBeta: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode: ledgerNodeBeta, mockData}, callback),
      mergeBranchesBeta: ['remoteEventsBeta', (results, callback) =>
        mergeBranches(ledgerNodeBeta, callback)],
      fromBeta: ['mergeBranchesBeta', (results, callback) => {
        const treeHash = results.mergeBranchesBeta.event.treeHash;
        const eventHash = results.mergeBranchesBeta.meta.eventHash;
        // copy the mergeEvent on ledgerNodeBeta and its ancestors to ledgerNode
        _copyEvents(
          {eventHash, from: ledgerNodeBeta, to: ledgerNode, treeHash},
          callback);
      }],
      mergeBranches: ['fromBeta', (results, callback) =>
        mergeBranches(ledgerNode, callback)],
      history: ['mergeBranches', (results, callback) => getHistory(
        genesisMerge, creatorId, (err, result) => {
          assertNoError(err);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(3);
          result.should.have.same.members([
            results.remoteEventsBeta.merge,
            results.mergeBranchesBeta.meta.eventHash,
            results.mergeBranches.meta.eventHash,
          ]);
          callback();
        })]
    }, done);
  });
});

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
      async.eachSeries(results.events, (e, callback) => to.events.add(
        e.event, {continuity2017: {peer: true}}, callback), callback);
    }]
  }, callback);
}
