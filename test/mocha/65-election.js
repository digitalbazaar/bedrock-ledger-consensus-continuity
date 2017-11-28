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

describe.skip('Election API', () => {
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

  it('_getElectorBranches', () => {
    /*const history = {
      // merge event
      eventHash: '1',
      creator: 'a',
      _parents: [{
        // local event
        eventHash: '2',
        _parents: [{
          // merge event
          eventHash: '3',
          creator: 'a',
          _parents: [{
            // merge event
            eventHash: '4',
            creator: 'b',
            _parents: [{
              // merge event
              eventHash: 'root',
              creator: 'a'
            }]
          }, {
            // merge event
            eventHash: '5',
            creator: 'c',
            _parents: [{
              // merge event
              eventHash: 'root',
              creator: 'a'
            }]
          }]
        }]
      }]
    };*/

/*
    const history = {
      // merge event
      eventHash: '1',
      creator: 'a',
      _parents: [{
        // local event
        eventHash: '2',
        _parents: [{
          // merge event
          eventHash: '3',
          creator: 'a',
          _parents: [{
            // merge event
            eventHash: '4',
            creator: 'b'
          }, {
            // merge event
            eventHash: '5',
            creator: 'c'
          }]
        }]
      }]
    };*/

/* byzantine 'c' because c-merge-2 isn't rooted in c-merge-1
    const history = {
      eventHash: 'a-merge-3',
      creator: 'a',
      _parents: [{
        eventHash: 'a-local-2',
        _parents: [{
          eventHash: 'a-merge-2',
          creator: 'a',
          _parents: [{
            eventHash: 'a-local-1',
            _parents: [{
              eventHash: 'a-merge-1',
              _parents: [{
                eventHash: 'b-merge-1',
                creator: 'b'
              }, {
                eventHash: 'c-merge-1',
                creator: 'c'
              }]
            }]
          }, {
            eventHash: 'a-merge-1',
            _parents: [{
              eventHash: 'b-merge-1',
              creator: 'b'
            }, {
              eventHash: 'c-merge-1',
              creator: 'c'
            }]
          }, {
            eventHash: 'b-merge-2',
            creator: 'b',
            _parents: [{
              eventHash: 'b-merge-1',
              creator: 'b'
            }]
          }, {
            eventHash: 'c-merge-2',
            creator: 'c'
          }]
        }]
      }]
    };*/

    const history = {
      eventHash: 'a-merge-4',
      creator: 'a',
      _parents: [{
        eventHash: 'b-merge-3',
        creator: 'b',
        _parents: [{
          eventHash: 'b-merge-2',
          creator: 'b',
          _parents: [{
            eventHash: 'b-merge-1',
            creator: 'b'
          }]
        }]
      }, {
        eventHash: 'c-merge-3',
        creator: 'c',
        _parents: [{
          eventHash: 'c-merge-2',
          creator: 'c',
          _parents: [{
            eventHash: 'c-merge-1',
            creator: 'c'
          }]
        }]
      }, {
        eventHash: 'a-merge-3',
        creator: 'a',
        _parents: [{
          eventHash: 'a-local-2',
          _parents: [{
            eventHash: 'a-merge-2',
            creator: 'a',
            _parents: [{
              eventHash: 'a-local-1',
              _parents: [{
                eventHash: 'a-merge-1',
                _parents: [{
                  eventHash: 'b-merge-1',
                  creator: 'b'
                }, {
                  eventHash: 'c-merge-1',
                  creator: 'c'
                }]
              }]
            }, {
              eventHash: 'a-merge-1',
              _parents: [{
                eventHash: 'b-merge-1',
                creator: 'b'
              }, {
                eventHash: 'c-merge-1',
                creator: 'c'
              }]
            }, {
              eventHash: 'b-merge-2',
              creator: 'b',
              _parents: [{
                eventHash: 'b-merge-1',
                creator: 'b'
              }]
            }, {
              eventHash: 'c-merge-2',
              creator: 'c',
              _parents: [{
                eventHash: 'c-merge-1',
                creator: 'c'
              }]
            }]
          }]
        }]
      }]
    };
    const electors = ['a', 'b', 'c', 'd'];
const start = Date.now();
    const branches = consensusApi._worker._election._getElectorBranches(
      {event: history, electors});
const end = Date.now();
console.log('time', (end - start) + ' ms');

    //console.log('head', util.inspect(branches.head, {depth: 20}));
    //console.log('tail', util.inspect(branches.tail, {depth: 20}));
    //console.log('tail.a', util.inspect(branches.tail.a, {depth: 20}));
    //console.log('tail.b', util.inspect(branches.tail.b, {depth: 20}));
    //console.log('tail.c', util.inspect(branches.tail.c, {depth: 20}));
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
