/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const async = require('async');
const uuid = require('uuid/v4');
const util = require('util');

const helpers = require('./helpers');
const mockData = require('./mock.data');

describe('Election API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
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
        brLedger.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedger.add(
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNode = result;
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          voterId = result.id;
          callback();
        });
      }],
      addEvent: ['ledgerNode', (results, callback) =>
        ledgerNode.consensus._events.add(
          testEvent, ledgerNode, (err, result) => {
          if(err) {
            return callback(err);
          }
          eventHash = result.meta.eventHash;
          callback();
        })]
    }, done);
  });

  describe('_findMergeEventProof', () => {
    it('just works', done => {
      const electors = [
        voterId,
        mockData.exampleIdentity
      ];
      const eventTemplate = mockData.events.alpha;
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      async.auto({
        events: callback => helpers.createEvent(
          {eventTemplate, eventNum: 1, consensus: false, hash: false},
          callback),
        localEvents: ['events', (results, callback) => async.map(
          results.events, (e, callback) => ledgerNode.consensus._events.add(
            e.event, ledgerNode,
            (err, result) => callback(err, result.meta.eventHash)),
          callback)],
        remoteEvents: callback => helpers.addRemoteEvents(
          {consensusApi, count: 1, ledgerNode, mockData}, callback),
        history1: ['localEvents', 'remoteEvents', (results, callback) =>
          getRecentHistory({ledgerNode}, callback)],
        mergeBranches: ['history1', (results, callback) => {
          mergeBranches({history: results.history1, ledgerNode}, callback);
        }],
        history2: ['mergeBranches', (results, callback) =>
          getRecentHistory({ledgerNode}, callback)],
        branches: ['history2', (results, callback) => {
          const branches = consensusApi._worker._election._getElectorBranches(
            {
              history: results.history2,
              electors
            });
          callback(null, branches);
        }],
        proof: ['branches', (results, callback) => {
          const proof = consensusApi._worker._election._findMergeEventProof({
            ledgerNode,
            history: results.history2,
            tails: results.branches,
            electors
          });
          // FIXME: add assertions
          //console.log('PROOF', util.inspect(proof));
          callback();
        }]
      }, done);
    });
  }); // end _findMergeEventProof
});
