/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const brTest = require('bedrock-test');
const async = require('async');
const request = brTest.require('request');
const sinon = require('sinon');
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
    const configEvent = mockData.events.config;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
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
        null, {configEvent}, (err, result) => {
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
      addEvent: ['ledgerNode', (results, callback) => ledgerNode.events.add(
        testEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          eventHash = result.meta.eventHash;
          callback();
        })]
    }, done);
  });

  // FIXME: this is being preserved for the manually created history objects
  describe.skip('_getElectorBranches', () => {
    it('produces heads and tails for two electors', done => {
      /*const history = {
        // merge event
        eventHash: '1',
        meta: {continuity2017: {creator: 'a'}},
        _parents: [{
          // local event
          eventHash: '2',
          _parents: [{
            // merge event
            eventHash: '3',
            meta: {continuity2017: {creator: 'a'}},
            _parents: [{
              // merge event
              eventHash: '4',
              meta: {continuity2017: {creator: 'b'}},
              _parents: [{
                // merge event
                eventHash: 'root',
                meta: {continuity2017: {creator: 'a'}}
              }]
            }, {
              // merge event
              eventHash: '5',
              meta: {continuity2017: {creator: 'c'}},
              _parents: [{
                // merge event
                eventHash: 'root',
                meta: {continuity2017: {creator: 'a'}}
              }]
            }]
          }]
        }]
      };*/

  /*
      const history = {
        // merge event
        eventHash: '1',
        meta: {continuity2017: {creator: 'a'}},
        _parents: [{
          // local event
          eventHash: '2',
          _parents: [{
            // merge event
            eventHash: '3',
            meta: {continuity2017: {creator: 'a'}},
            _parents: [{
              // merge event
              eventHash: '4',
              meta: {continuity2017: {creator: 'b'}}
            }, {
              // merge event
              eventHash: '5',
              meta: {continuity2017: {creator: 'c'}}
            }]
          }]
        }]
      };*/

  /* byzantine 'c' because c-merge-2 isn't rooted in c-merge-1
      const history = {
        eventHash: 'a-merge-3',
        meta: {continuity2017: {creator: 'a'}},
        _parents: [{
          eventHash: 'a-local-2',
          _parents: [{
            eventHash: 'a-merge-2',
            meta: {continuity2017: {creator: 'a'}},
            _parents: [{
              eventHash: 'a-local-1',
              _parents: [{
                eventHash: 'a-merge-1',
                _parents: [{
                  eventHash: 'b-merge-1',
                  meta: {continuity2017: {creator: 'b'}}
                }, {
                  eventHash: 'c-merge-1',
                  meta: {continuity2017: {creator: 'c'}}
                }]
              }]
            }, {
              eventHash: 'a-merge-1',
              _parents: [{
                eventHash: 'b-merge-1',
                meta: {continuity2017: {creator: 'b'}}
              }, {
                eventHash: 'c-merge-1',
                meta: {continuity2017: {creator: 'c'}}
              }]
            }, {
              eventHash: 'b-merge-2',
              meta: {continuity2017: {creator: 'b'}},
              _parents: [{
                eventHash: 'b-merge-1',
                meta: {continuity2017: {creator: 'b'}}
              }]
            }, {
              eventHash: 'c-merge-2',
              meta: {continuity2017: {creator: 'c'}}
            }]
          }]
        }]
      };*/

      const history = {
        eventHash: 'a-merge-4',
        meta: {continuity2017: {creator: 'a'}},
        _parents: [{
          eventHash: 'b-merge-3',
          meta: {continuity2017: {creator: 'b'}},
          _parents: [{
            eventHash: 'b-merge-2',
            meta: {continuity2017: {creator: 'b'}},
            _parents: [{
              eventHash: 'b-merge-1',
              meta: {continuity2017: {creator: 'b'}}
            }]
          }]
        }, {
          eventHash: 'c-merge-3',
          meta: {continuity2017: {creator: 'c'}},
          _parents: [{
            eventHash: 'c-merge-2',
            meta: {continuity2017: {creator: 'c'}},
            _parents: [{
              eventHash: 'c-merge-1',
              meta: {continuity2017: {creator: 'c'}}
            }]
          }]
        }, {
          eventHash: 'a-merge-3',
          meta: {continuity2017: {creator: 'a'}},
          _parents: [{
            eventHash: 'a-local-2',
            _parents: [{
              eventHash: 'a-merge-2',
              meta: {continuity2017: {creator: 'a'}},
              _parents: [{
                eventHash: 'a-local-1',
                _parents: [{
                  eventHash: 'a-merge-1',
                  _parents: [{
                    eventHash: 'b-merge-1',
                    meta: {continuity2017: {creator: 'b'}}
                  }, {
                    eventHash: 'c-merge-1',
                    meta: {continuity2017: {creator: 'c'}}
                  }]
                }]
              }, {
                eventHash: 'a-merge-1',
                _parents: [{
                  eventHash: 'b-merge-1',
                  meta: {continuity2017: {creator: 'b'}}
                }, {
                  eventHash: 'c-merge-1',
                  meta: {continuity2017: {creator: 'c'}}
                }]
              }, {
                eventHash: 'b-merge-2',
                meta: {continuity2017: {creator: 'b'}},
                _parents: [{
                  eventHash: 'b-merge-1',
                  meta: {continuity2017: {creator: 'b'}}
                }]
              }, {
                eventHash: 'c-merge-2',
                meta: {continuity2017: {creator: 'c'}},
                _parents: [{
                  eventHash: 'c-merge-1',
                  meta: {continuity2017: {creator: 'c'}}
                }]
              }]
            }]
          }]
        }]
      };
      // const electors = ['a', 'b', 'c', 'd'];
      const electors = [
        voterId,
        mockData.exampleIdentity
      ];
      const eventTemplate = mockData.events.alpha;
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      async.auto({
        localEvent: callback => helpers.addEvent(
          {eventTemplate, ledgerNode}, callback),
        remoteEvents: callback => helpers.addRemoteEvents(
          {consensusApi, count: 1, ledgerNode, mockData}, callback),
        history1: ['remoteEvents', 'localEvent', (results, callback) =>
          getRecentHistory({ledgerNode}, callback)],
        mergeBranches: ['history1', , (results, callback) => {
          mergeBranches({history: results.history1, ledgerNode}, callback);
        }],
        history2: ['mergeBranches', (results, callback) =>
          getRecentHistory({ledgerNode}, callback)],
        test: ['history2', (results, callback) => {
          // const start = Date.now();
          // const branches = consensusApi._worker._election
          //   ._getElectorBranches({event: history, electors});
          const branches = consensusApi._worker._election._getElectorBranches(
            {
              history: results.history2,
              electors
            });
          branches.should.be.an('object');
          console.log('BBBBBBB', branches);
          Object.keys(branches).should.have.same.members(electors);
          callback();
        }]
      }, done);
    });
  }); // end _getElectorBranches

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
          results.events, (e, callback) => ledgerNode.events.add(
            e.event, (err, result) => callback(err, result.meta.eventHash)),
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
          console.log('PROOF', util.inspect(proof));
          callback();
        }]
      }, done);
    });
  }); // end _findMergeEventProof
});
