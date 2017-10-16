/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const brLedger = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
let request = require('request');
request = request.defaults({json: true, strictSSL: false});

describe('Consensus Agent API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  let latestBlockHeight;
  beforeEach(done => {
    const configEvent = mockData.events.config;
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
      getLatest: ['ledgerNode', (results, callback) => {
        ledgerNode.blocks.getLatest((err, result) => {
          if(err) {
            return callback(err);
          }
          latestBlockHeight = result.eventBlock.block.blockHeight;
          callback();
        });
      }],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          voterId = result.id;
          callback();
        });
      }]
    }, done);
  });

  it('get block status', done => {
    const newBlockHeight = latestBlockHeight + 1;
    const testUrl = voterId + '/blocks/' + newBlockHeight + '/status';
    request.get(testUrl, (err, res) => {
      should.not.exist(err);
      should.exist(res.body);
      res.body.should.be.an('object');
      const result = res.body;
      result.blockHeight.should.equal(newBlockHeight);
      result.consensusPhase.should.equal('gossip');
      result.eventHash.should.be.an('array');
      result.eventHash.should.have.length(0);
      done();
    });
  });
  it('should have one event to gossip', done => {
    const newBlockHeight = latestBlockHeight + 1;
    const testUrl = voterId + '/blocks/' + newBlockHeight + '/status';
    async.auto({
      addEvent: callback => ledgerNode.events.add(
        mockData.events.alpha, callback),
      blockStatus: ['addEvent', (results, callback) =>
        request.get(testUrl, (err, res) => {
          should.not.exist(err);
          should.exist(res.body);
          res.body.should.be.an('object');
          const result = res.body;
          result.blockHeight.should.equal(newBlockHeight);
          result.consensusPhase.should.equal('gossip');
          result.eventHash.should.be.an('array');
          result.eventHash.should.have.length(1);
          callback();
        })
      ]
    }, done);
  });
});
