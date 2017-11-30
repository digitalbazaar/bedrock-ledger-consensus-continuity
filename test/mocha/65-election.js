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

  it.only('_getElectorBranches', done => {
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
      events: callback => helpers.createEvent(
        {eventTemplate, eventNum: 1, consensus: false, hash: false},
        callback),
      localEvents: ['events', (results, callback) => async.map(
        results.events, (e, callback) => ledgerNode.events.add(
          e.event, (err, result) => callback(err, result.meta.eventHash)),
        callback)],
      // 5 remote merge events from the same creator chained together
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, count: 1, ledgerNode, mockData}, callback),
      mergeBranches: ['localEvents', 'remoteEvents', (results, callback) => {
        // return callback();
        mergeBranches({ledgerNode}, callback);
      }],
      history: ['mergeBranches', (results, callback) =>
        getRecentHistory({ledgerNode}, callback)],
      test: ['history', (results, callback) => {
        const start = Date.now();
        // FIXME: passing in the localBranchHead Event here, correct?
        const branches = consensusApi._worker._election._getElectorBranches(
          {
            event: results.history.eventMap[results.history.localBranchHead],
            // events: results.history.eventMap,
            electors
          });
        // const branches = consensusApi._worker._election._getElectorBranches(
        //   {event: history, electors});
        const end = Date.now();
        console.log('time', (end - start) + ' ms');
        console.log('head', util.inspect(branches.head, {depth: 20}));
        console.log('tail', util.inspect(branches.tail, {depth: 20}));
        console.log('ZZZZZZ', Object.keys(branches.tail));
        // console.log('tail.a', util.inspect(branches.tail.a, {depth: 20}));
        // console.log('tail.b', util.inspect(branches.tail.b, {depth: 20}));
        // console.log('tail.c', util.inspect(branches.tail.c, {depth: 20}));
        callback();
      }]
    }, done);

  });

  describe('_getManifest', () => {
    before(() => {
      sinon.stub(request, 'get').callsFake((options, callback) => {
        const mockUrl = options.url.substring(voterId.length);
        if(!mockData.sinon[mockUrl]) {
          return callback(null, {
            statusCode: 404,
            body: {error: 'Manifest not found.'}
          });
        }
        callback(null, {
          statusCode: 200,
          body: mockData.sinon[mockUrl]
        });
      });

    });
    after(() => {
      request.get.restore();
    });
    it('gets a manifest out of local storage', done => {
      async.auto({
        createManifest: callback => consensusApi._worker._election
          ._createEventManifest(ledgerNode, 1, callback),
        getManifest: ['createManifest', (results, callback) => {
          const manifestHash = results.createManifest.id;
          consensusApi._election._getManifest(
            ledgerNode, voterId, manifestHash, 'Events', (err, result) => {
              assertNoError(err);
              result.should.deep.equal(results.createManifest);
              callback();
            });
        }]
      }, done);
    });
    it('returns NotFoundError on unknown manifestHash', done => {
      async.auto({
        getManifest: callback => {
          const manifestHash =
            'ni:///sha-256;-D0-PH-X_NVlNPeTwY9jjtlaH-4HOhQHVmzH-CT6rYI';
          consensusApi._election._getManifest(
            ledgerNode, voterId, manifestHash, 'Events', (err, result) => {
              should.exist(err);
              err.name.should.equal('NotFoundError');
              err.details.httpStatusCode.should.equal(404);
              should.not.exist(result);
              callback();
            });
        }
      }, done);
    });
    // FIXME: should the event associated with this test be signed?
    it('gets a remote manifest', done => {
      async.auto({
        getManifest: callback => {
          const manifestHash = mockData.manifests.sinonAlpha.id;
          consensusApi._election._getManifest(
            ledgerNode, voterId, manifestHash, 'Events', (err, result) => {
              assertNoError(err);
              result.should.be.an('object');
              result.should.deep.equal(mockData.manifests.sinonAlpha);
              callback();
            });
        }
      }, done);
    });
  });
});
