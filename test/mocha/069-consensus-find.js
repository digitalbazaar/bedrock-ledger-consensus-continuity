/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');
const async = require('async');
const {callbackify} = require('util');

const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

/* eslint-disable no-unused-vars */
describe('Consensus API find', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });
  let genesisBlock;
  let Worker;
  const nodes = {};
  const peers = {};
  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    await helpers.flushCache();
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    const plugin = await helpers.use('Continuity2017');
    consensusApi = plugin.api;
    Worker = consensusApi._worker.Worker;
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    const {genesisBlock: _genesisBlock} = await nodes.alpha.blocks.getGenesis();
    genesisBlock = _genesisBlock.block;
    nodes.beta = await brLedgerNode.add(null, {genesisBlock});
    nodes.gamma = await brLedgerNode.add(null, {genesisBlock});
    nodes.delta = await brLedgerNode.add(null, {genesisBlock});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      // attach worker to the node to emulate a work session used by `helpers`
      ledgerNode.worker = new Worker({session: {ledgerNode}});
      await ledgerNode.worker.init();
      const {id: ledgerNodeId} = ledgerNode;
      const peerId = await consensusApi._localPeers.getPeerId({ledgerNodeId});
      peers[key] = peerId;
      ledgerNode.peerId = peerId;
      helpers.peersReverse[peerId] = key;
    }
    // NOTE: if nodeEpsilon is enabled, be sure to add to `creator` deps
    // nodeEpsilon: ['genesisBlock', (results, callback) => brLedgerNode.add(
    //   null, {genesisBlock: results.genesisBlock}, (err, result) => {
    //     if(err) {
    //       return callback(err);
    //     }
    //     nodes.epsilon = result;
    //     callback(null, result);
    //   })],
  });
  it('single node consensus', done => {
    // the genesisMerge already has consensus
    const ledgerNode = nodes.alpha;
    const {peerId} = ledgerNode;
    ledgerNode.worker.consensusState.witnesses = [peers.alpha];
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      consensus: ['event1', (results, callback) => {
        ledgerNode.worker._findConsensus().then(result => {
          should.exist(result);
          result.consensusProofHash.should.have.length(1);
          result.consensusProofHash[0].should.equal(
            results.event1.mergeHash);
          result.eventHash.should.have.length(2);
          result.eventHash.should.have.same.members([
            Object.keys(results.event1.regular)[0],
            results.event1.merge.meta.eventHash
          ]);
          callback();
        }, callback);
      }]
    }, done);
  });
  it('properly does not reach consensus with four witnesses', done => {
    const ledgerNode = nodes.alpha;
    const {peerId} = ledgerNode;
    const witnesses = [peers.alpha, peers.beta, peers.gamma, peers.delta];
    ledgerNode.worker.consensusState.witnesses = witnesses;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      event1: callback => callbackify(helpers.addEventAndMerge)(
        {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
      consensus: ['event1', (results, callback) => {
        ledgerNode.worker._findConsensus().then(result => {
          should.exist(result);
          result.consensus.should.equal(false);
          callback();
        }, callback);
      }]
    }, done);
  });
  it.skip('add regular event with no merge before findConsensus', done => {
    const findConsensus = callbackify(consensusApi._worker._findConsensus);
    const ledgerNode = nodes.alpha;
    const witnesses = Object.values(peers);
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    async.auto({
      build: callback => callbackify(helpers.buildHistory)(
        {historyId: 'alpha', mockData, nodes}, callback),
      history: ['build', (results, callback) => {
        callback(null, ledgerNode.worker.getRecentHistory());
      }],
      event: ['history', (results, callback) => callbackify(helpers.addEvent)(
        {ledgerNode, eventTemplate, opTemplate}, callback)],
      consensus: ['event', (results, callback) => {
        findConsensus({
          witnesses, ledgerNode, history: results.history
        }, (err, result) => {
          const {copyMergeHashes, copyMergeHashesIndex, regularEvent} =
            results.build;
          assertNoError(err);
          result.eventHash.should.have.length(8);
          result.eventHash.should.have.same.members([
            ...regularEvent.regularHash,
            ...regularEvent.mergeHash
          ]);
          result.consensusProofHash.should.have.length(10);
          result.consensusProofHash.should.have.same.members([
            copyMergeHashes.cpa,
            copyMergeHashes.cpb,
            copyMergeHashes.cp1,
            copyMergeHashes.cp2,
            copyMergeHashes.cp3,
            copyMergeHashes.cp4,
            copyMergeHashes.cp5,
            copyMergeHashes.cp6,
            copyMergeHashes.cp7,
            copyMergeHashes.cp8
          ]);
          // createReport({
          //   copyMergeHashes,
          //   copyMergeHashesIndex,
          //   consensusProofHash: result.consensusProofHash,
          // });
          callback();
        });
      }]
    }, done);
  });
});

function createReport(
  {copyMergeHashes, copyMergeHashesIndex, consensusProofHash}) {
  console.log(
    'CONSENSUS REPORT',
    consensusProofHash.map(c => copyMergeHashesIndex[c]));
}
