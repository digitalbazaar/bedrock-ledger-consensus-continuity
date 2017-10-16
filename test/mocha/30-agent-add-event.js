/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
let request = require('request');
request = request.defaults({json: true, strictSSL: false});
const uuid = require('uuid/v4');

describe('Consensus Agent - Add Event API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  beforeEach(done => {
    const configEvent = mockData.events.config;
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
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          voterId = result.id;
          callback();
        });
      }]
    }, done);
  });
  it('should add an event', done => {
    const testUrl = voterId + '/events';
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEvent.input[0].id = 'https://example.com/events/' + uuid();
    // FIXME: should the event be signed?
    async.auto({
      hash: callback => brLedgerNode.consensus._hasher(testEvent, callback),
      addEvent: ['hash', (results, callback) =>
        request.post({
          url: testUrl,
          json: {eventHash: results.hash, event: testEvent}
        }, (err, res) => {
          assertNoError(err);
          res.statusCode.should.equal(201);
          should.exist(res.headers.location);
          callback();
        })]
    }, done);
  });
});
