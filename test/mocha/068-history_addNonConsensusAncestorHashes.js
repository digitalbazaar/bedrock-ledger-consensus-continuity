/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const {callbackify} = require('util');

const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

describe('History API _addNonConsensusAncestorHashes', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });
  let genesisMerge;
  let Worker;
  const nodes = {};
  const peers = {};
  const witnesses = new Set();
  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    await helpers.flushCache();
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const plugin = await helpers.use('Continuity2017');
    consensusApi = plugin.api;
    Worker = consensusApi._worker.Worker;
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    const peerId = await consensusApi._localPeers.getPeerId(
      {ledgerNodeId: nodes.alpha.id});
    nodes.alpha.peerId = peerId;
    const {genesisBlock: _genesisBlock} = await nodes.alpha.blocks.getGenesis();
    const genesisBlock = _genesisBlock.block;
    nodes.beta = await brLedgerNode.add(null, {genesisBlock});
    nodes.gamma = await brLedgerNode.add(null, {genesisBlock});
    nodes.delta = await brLedgerNode.add(null, {genesisBlock});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      // attach worker to the node to emulate a work session used by `helpers`
      ledgerNode.worker = new Worker({session: {ledgerNode}});
      await ledgerNode.worker.init();
      const {id: ledgerNodeId} = ledgerNode;
      ledgerNode.peerId = await consensusApi._localPeers.getPeerId(
        {ledgerNodeId});
      peers[key] = ledgerNode.peerId;
      witnesses.add(ledgerNode.peerId);
      ledgerNode.worker.consensusState.witnesses = witnesses;
    }
    genesisMerge = nodes.alpha.worker.head.eventHash;
  });
  it('gets no events', async () => {
    // the genesisMerge already has consensus
    const getAncestors = consensusApi._history._addNonConsensusAncestorHashes;
    const hashes = {mergeEventHashes: [], parentHashes: [genesisMerge]};
    const result = await getAncestors({ledgerNode: nodes.alpha, hashes});
    should.exist(result);
    result.should.be.an('array');
    result.should.have.length(0);
  });
  it('gets two events', done => {
    const getAncestors = consensusApi._history._addNonConsensusAncestorHashes;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode, witnesses, opTemplate}, callback),
      test: ['event1', async results => {
        const hashes = {
          mergeEventHashes: [results.event1.mergeHash],
          parentHashes: results.event1.merge.event.parentHash
        };
        try {
          const result = await getAncestors({hashes, ledgerNode});
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(2);
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });
  // FIXME: skipped due to test requiring an update to wait for the previous
  // events to achieve consensus before they can be merged
  it.skip('gets four events', done => {
    const getAncestors = consensusApi._history._addNonConsensusAncestorHashes;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode, opTemplate}, callback),
      event2: ['event1', (results, callback) =>
        callbackify(helpers.addEventAndMerge)(
          {eventTemplate, ledgerNode, opTemplate}, callback)],
      test: ['event2', async results => {
        const hashes = {
          mergeEventHashes: [results.event2.mergeHash],
          parentHashes: [
            ...results.event1.merge.event.parentHash,
            ...results.event2.merge.event.parentHash
          ]
        };
        try {
          const result = await getAncestors({hashes, ledgerNode});
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });
  it('gets 4 events involving 2 nodes', done => {
    const getAncestors = consensusApi._history._addNonConsensusAncestorHashes;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode, opTemplate}, callback),
      cp1: ['event1', (results, callback) => callbackify(helpers.copyAndMerge)({
        from: 'alpha', nodes, to: 'beta'}, callback)],
      cp2: ['cp1', (results, callback) => callbackify(helpers.copyAndMerge)({
        from: 'beta', nodes, to: 'alpha'}, callback)],
      test: ['cp2', async results => {
        const hashes = {
          mergeEventHashes: [
            results.cp1.meta.eventHash,
            results.cp2.meta.eventHash
          ],
          parentHashes: _.uniq([
            ...results.cp1.event.parentHash,
            ...results.cp2.event.parentHash,
          ])
        };
        try {
          const result = await getAncestors({hashes, ledgerNode});
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });

  // FIXME: this test likely needs to be removed, the returned data structure
  // no longer matches the assertions
  it.skip('gets 4 events without duplicates', done => {
    const getAncestors = consensusApi._history._addNonConsensusAncestorHashes;
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {eventTemplate, ledgerNode}, callback),
      cp1: ['event1', (results, callback) => callbackify(helpers.copyAndMerge)({
        from: nodes.alpha,
        to: nodes.beta
      }, callback)],
      cp2: ['cp1', (results, callback) => callbackify(helpers.copyAndMerge)({
        from: nodes.beta,
        to: nodes.alpha
      }, callback)],
      test: ['cp2', async results => {
        try {
          const result = await getAncestors({
            ledgerNode,
            eventHash: [results.cp1.meta.eventHash, results.cp2.meta.eventHash]
          });
          should.exist(result);
          result.should.be.an('array');
          result.should.have.length(4);
          result.forEach(e => {
            e.event.should.be.an('array');
            e.event.should.have.length(1);
          });
        } catch(e) {
          assertNoError(e);
        }
      }]
    }, done);
  });
});
