/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
// let request = require('request');
// request = request.defaults({json: true, strictSSL: false});
const uuid = require('uuid/v4');

describe.skip('Consensus Agent - Get History API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let genesisMerge;
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
          callback(null, result);
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          if(err) {
            return callback(err);
          }
          voterId = result.id;
          callback();
        });
      }],
      genesisMerge: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        const collection = ledgerNode.storage.events.collection;
        consensusApi._worker._events._getLocalBranchHead(
          collection, (err, result) => {
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
        })]
    }, done);
  });
  it('should return an empty array', done => {
    const getHistory = consensusApi._worker._client.getHistory;
    async.auto({
      history: callback => getHistory(genesisMerge, voterId, (err, result) => {
        assertNoError(err);
        should.exist(result);
        result.should.be.an('array');
        result.should.have.length(0);
        callback();
      })
    }, done);
  });
  it('should return something', done => {
    const getHistory = consensusApi._worker._client.getHistory;
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    async.auto({
      // merge the local event added in `before`
      mergeBranches: callback => mergeBranches(ledgerNode, callback),
      history: ['mergeBranches', (results, callback) => getHistory(
        genesisMerge, voterId, (err, result) => {
          assertNoError(err);
          console.log('GGGGGGGGG', genesisMerge);
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(0);
          callback();
        })]
    }, done);
  });
});
