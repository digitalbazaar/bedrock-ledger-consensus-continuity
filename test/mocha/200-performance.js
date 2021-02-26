/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const {callbackify} = require('util');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const {util: {uuid}} = bedrock;

describe.skip('Performance - Consensus Client - getBlockStatus API', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });

  const eventNum = 2000;
  const passNum = 10;
  const opNum = 500;
  let consensusApi;
  let ledgerNode;
  let peerId;
  let testEventId;
  before(async function() {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.operation[0].record.id = testEventId;
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const consensusPlugin = await helpers.use('Continuity2017');
    consensusApi = consensusPlugin.api;
    ledgerNode = await brLedgerNode.add(null, {ledgerConfiguration});
    peerId = await consensusApi._localPeers.getPeerId(
      {ledgerNodeId: ledgerNode.id});
  });
  describe('Preparation', () => {
    it(`adds ${eventNum} events`, function(done) {
      this.timeout(120000);
      async.auto({
        create: callback => callbackify(helpers.createEvent)(
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
        peerId,
        opNum,
        passNum
      }, done);
    });
  });
});

function runPasses({
  func, blockHeight, passNum, opNum, peerId, concurrency = 100
}, callback) {
  const passes = [];
  async.timesSeries(passNum, (i, callback) => {
    const start = Date.now();
    async.timesLimit(
      opNum, concurrency,
      (i, callback) => func.call(null, blockHeight, peerId, callback), err => {
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
