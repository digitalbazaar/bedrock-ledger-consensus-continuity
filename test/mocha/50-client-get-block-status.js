/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
/* globals should */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

describe('Consensus Client - getBlockStatus API', () => {
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
  it('blockHeight 1 status == consensus after consensus', done => {
    async.auto({
      get: callback => consensusApi._worker._client.getBlockStatus(
        0, voterId, (err, result) => {
          should.not.exist(err);
          should.exist(result);
          result.should.be.an('object');
          result.blockHeight.should.equal(0);
          result.consensusPhase.should.equal('consensus');
          result.election.should.be.an('array');
          result.election.should.have.length(2);
          callback();
        })
    }, done);
  });
  it('should get a status for blockHeight = 1 during gossip', done => {
    async.auto({
      get: callback => consensusApi._worker._client.getBlockStatus(
        1, voterId, (err, result) => {
          should.not.exist(err);
          should.exist(result);
          result.should.be.an('object');
          result.blockHeight.should.equal(1);
          result.consensusPhase.should.equal('gossip');
          result.eventHash.should.be.an('array');
          result.eventHash.should.have.length(1);
          result.eventHash.should.have.members([eventHash]);
          callback();
        })
    }, done);
  });
  it('NotFoundError for blockHeight = 2 while block 1 is gossip', done => {
    async.auto({
      get: callback => consensusApi._worker._client.getBlockStatus(
        2, voterId, (err, result) => {
          should.exist(err);
          should.not.exist(result);
          err.name.should.equal('NotFoundError');
          err.details.httpStatusCode.should.equal(404);
          callback();
        })
    }, done);
  });
  it('blockHeight 1 status == consensus after consensus', done => {
    async.auto({
      runWorker: callback =>
        consensusApi._worker._run(ledgerNode, err => callback(err)),
      get: ['runWorker', (results, callback) =>
        consensusApi._worker._client.getBlockStatus(
          1, voterId, (err, result) => {
            should.not.exist(err);
            should.exist(result);
            result.should.be.an('object');
            result.blockHeight.should.equal(1);
            result.consensusPhase.should.equal('consensus');
            result.election.should.be.an('array');
            result.election.should.have.length(2);
            callback();
          })]
    }, done);
  });
  it('NotFoundError for blockHeight = 100', done => {
    async.auto({
      get: callback => consensusApi._worker._client.getBlockStatus(
        100, voterId, (err, result) => {
          should.exist(err);
          should.not.exist(result);
          err.name.should.equal('NotFoundError');
          err.details.httpStatusCode.should.equal(404);
          callback();
        })
    }, done);
  });
});
