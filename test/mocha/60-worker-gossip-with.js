/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

describe('Worker - _gossipWith', () => {
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
          eventHash = result.meta.eventHash;
          callback();
        })]
    }, done);
  });
  it('should complete without an error', done => {
    async.auto({
      previousBlockHash: callback => ledgerNode.storage.blocks.getLatest(
        (err, block) => callback(
          err, err ? null : block.eventBlock.meta.blockHash)),
      gossipWith: ['previousBlockHash', (results, callback) =>
        consensusApi._worker._gossipWith(
          ledgerNode, {id: voterId}, results.previousBlockHash,
          1, {id: voterId}, err => {
            should.not.exist(err);
            callback();
          })]
    }, done);
  });
});
