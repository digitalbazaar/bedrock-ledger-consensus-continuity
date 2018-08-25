/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
// const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
let request = require('request');
request = request.defaults({json: true, strictSSL: false});
// const uuid = require('uuid/v4');

describe('Consensus Agent - Get Event API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let eventHash;
  let ledgerNode;
  let ledgerNodeId;
  let operationId;
  let voterId;
  beforeEach(done => {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        helpers.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {ledgerConfiguration}, (err, result) => {
          if(err) {
            return callback(err);
          }
          ledgerNode = result;
          ledgerNodeId = ledgerNode.id;
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._voters.get({ledgerNodeId}, (err, result) => {
          voterId = result.id;
          ledgerNode.creatorId = result.id;
          callback();
        });
      }],
      addEvent: ['getVoter', (results, callback) => {
        const eventTemplate = mockData.events.alpha;
        const opTemplate = mockData.operations.alpha;
        helpers.addEvent(
          {eventTemplate, ledgerNode, opTemplate}, (err, result) => {
            if(err) {
              return callback(err);
            }
            eventHash = Object.keys(result)[0];
            operationId = result[eventHash].operations[0].operation.record.id;
            callback();
          });
      }]
    }, done);
  });
  it('should get an event', done => {
    const testUrl = voterId + '/events?id=' + eventHash;
    async.auto({
      get: callback => request.get(testUrl, (err, res) => {
        should.not.exist(err);
        res.statusCode.should.equal(200);
        const result = res.body;
        should.exist(result.operation);
        result.operation.should.be.an('array');
        result.operation.should.have.length(1);
        should.exist(result.operation[0].record);
        result.operation[0].record.id.should.equal(operationId);
        callback();
      })
    }, done);
  });
});
