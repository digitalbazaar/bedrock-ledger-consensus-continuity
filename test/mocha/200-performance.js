/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const {util: {uuid}} = bedrock;

describe.skip('Performance - Consensus Client - getBlockStatus API', () => {
  before(async function() {
    await helpers.prepareDatabase(mockData);
  });

  const eventNum = 2000;
  const passNum = 10;
  const opNum = 500;
  let consensusApi;
  let ledgerNode;
  let voterId;
  let testEventId;
  before(done => {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
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
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._voters.get(ledgerNode.id, (err, result) => {
          voterId = result.id;
          callback();
        });
      }]
    }, done);
  });
  describe('Preparation', () => {
    it(`adds ${eventNum} events`, function(done) {
      this.timeout(120000);
      async.auto({
        create: callback => helpers.createEvent(
          {consensus: false, eventNum, eventTemplate: mockData.events.alpha},
          callback),
        add: ['create', (results, callback) => async.eachLimit(
          results.create, 100, (e, callback) => ledgerNode.storage.events.add(
            e.event, e.meta, callback), callback)]
      }, err => {
        assertNoError(err);
        done();
      });
    });
  });
  describe('get block status', () => {
    it(`gets block status ${opNum} times`, function(done) {
      this.timeout(120000);
      runPasses({
        func: consensusApi._client.getBlockStatus,
        blockHeight: 1,
        voterId,
        opNum,
        passNum
      }, done);
    });
  });
});

function runPasses({
  func, blockHeight, passNum, opNum, voterId, concurrency = 100
}, callback) {
  const passes = [];
  async.timesSeries(passNum, (i, callback) => {
    const start = Date.now();
    async.timesLimit(
      opNum, concurrency,
      (i, callback) => func.call(null, blockHeight, voterId, callback), err => {
        const stop = Date.now();
        assertNoError(err);
        passes.push(Math.round(opNum / (stop - start) * 1000));
        callback();
      });
  }, err => {
    assertNoError(err);
    console.log('ops/sec passes', passes);
    console.log('average ops/sec', helpers.average(passes));
    callback();
  });
}
