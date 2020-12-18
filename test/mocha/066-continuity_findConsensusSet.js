/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');

let consensusApi;

/* eslint-disable no-unused-vars */
describe('Continuity API _findConsensusSet', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });
  let _findConsensusSet;
  let _getTails;
  let genesisBlock;
  let Worker;
  const nodes = {};
  const peers = {};
  beforeEach(async function() {
    this.timeout(120000);
    await helpers.flushCache();
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    ({api: consensusApi} = await helpers.use('Continuity2017'));
    _getTails = consensusApi._consensus._continuity._getTails;
    _findConsensusSet = consensusApi._consensus._continuity._findConsensusSet;
    Worker = consensusApi._worker.Worker;

    // add genesis node
    const ledgerConfiguration = mockData.ledgerConfiguration;
    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});
    const {id: creatorId} = await consensusApi._peers.get(
      {ledgerNodeId: nodes.alpha.id});

    // get genesis block
    ({genesisBlock: {block: genesisBlock}} =
      await nodes.alpha.blocks.getGenesis());

    // add other nodes
    [nodes.beta, nodes.gamma, nodes.delta] = await Promise.all([
      brLedgerNode.add(null, {genesisBlock}),
      brLedgerNode.add(null, {genesisBlock}),
      brLedgerNode.add(null, {genesisBlock})
    ]);

    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const {id: ledgerNodeId} = ledgerNode;
      // attach worker to the node to emulate a work session used by `helpers`
      ledgerNode.worker = new Worker({session: {ledgerNode}});
      await ledgerNode.worker._init();
      const {id} = await consensusApi._peers.get({ledgerNodeId});
      peers[key] = id;
      ledgerNode.creatorId = id;
      helpers.peersReverse[id] = key;
    }
  });

  it('ledger history alpha', async function() {
    const report = {};
    // NOTE: for ledger history alpha, all nodes should have the same view
    // all peers are witnesses
    const witnesses = Object.values(peers);
    const build = await helpers.buildHistory(
      {historyId: 'alpha', mockData, nodes, witnesses});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const history = ledgerNode.worker.getRecentHistory();
      const {tails, witnessTails} = _getTails({history, witnesses});
      const result = _findConsensusSet({
        ledgerNode, history, tails, witnessTails, witnesses
      });
      // try {
      //   report[i] = createReport({
      //     result,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[i] = 'NONE';
      // }
      const allXs = result.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = result.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members(build.regularEvent.mergeHash);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  it('ledger history beta', async function() {
    const report = {};
    // NOTE: for ledger history beta, all nodes should have the same view
    // all peers are witnesses
    const witnesses = Object.values(peers);
    const build = await helpers.buildHistory(
      {historyId: 'beta', mockData, nodes, witnesses});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const history = ledgerNode.worker.getRecentHistory();
      const {tails, witnessTails} = _getTails({history, witnesses});
      const result = _findConsensusSet({
        ledgerNode, history, tails, witnessTails, witnesses
      });
      // try {
      //   report[i] = createReport({
      //     result,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[i] = 'NONE';
      // }
      const allXs = result.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = result.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members(build.regularEvent.mergeHash);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  it('ledger history gamma', async function() {
    const report = {};
    // NOTE: for ledger history gamma, all nodes should have the same view
    // all peers are witnesses
    const witnesses = Object.values(peers);
    const build = await helpers.buildHistory(
      {historyId: 'gamma', mockData, nodes, witnesses});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const history = ledgerNode.worker.getRecentHistory();
      const {tails, witnessTails} = _getTails({history, witnesses});
      const result = _findConsensusSet({
        ledgerNode, history, tails, witnessTails, witnesses
      });
      // try {
      //   report[i] = createReport({
      //     result,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[i] = 'NONE';
      // }
      const allXs = result.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = result.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members(build.regularEvent.mergeHash);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  // involves 4 witness nodes and one non-witness
  it('ledger history delta produces same as alpha result', async function() {
    this.timeout(120000);
    const report = {};
    // all peers except epsilon are witnesses (epsilon not added yet)
    const witnesses = Object.values(peers);

    try {
      // add node epsilon for this test and remove it afterwards
      nodes.epsilon = await brLedgerNode.add(null, {genesisBlock});
      // attach worker to the node to emulate a work session used by `helpers`
      nodes.epsilon.worker = new Worker({session: {ledgerNode: nodes.epsilon}});
      await nodes.epsilon.worker._init();
      const {id} = await consensusApi._peers.get(
        {ledgerNodeId: nodes.epsilon.id});
      peers.epsilon = id;
      nodes.epsilon.creatorId = id;
      helpers.peersReverse[id] = 'epsilon';

      const build = await helpers.buildHistory(
        {historyId: 'delta', mockData, nodes, witnesses});

      for(const key in nodes) {
        const ledgerNode = nodes[key];
        const history = ledgerNode.worker.getRecentHistory();
        const {tails, witnessTails} = _getTails({history, witnesses});
        const result = _findConsensusSet({
          ledgerNode, history, tails, witnessTails, witnesses
        });
        // try {
        //   report[i] = createReport({
        //     result,
        //     copyMergeHashes: build.copyMergeHashes,
        //     copyMergeHashesIndex: build.copyMergeHashesIndex});
        // } catch(e) {
        //   report[i] = 'NONE';
        // }
        const allXs = result.consensus.map(p => p.x.eventHash);
        allXs.should.have.length(4);
        const mergeHashes = [
          build.regularEvent.alpha.mergeHash,
          build.regularEvent.beta.mergeHash,
          build.regularEvent.gamma.mergeHash,
          build.regularEvent.delta.mergeHash
          // exclude epsilon (non-witness)
        ];
        allXs.should.have.same.members(mergeHashes);
        const allYs = result.consensus.map(p => p.y.eventHash);
        allYs.should.have.length(4);
        allYs.should.have.same.members(mergeHashes);
      }
      // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
    } finally {
      // clean up epsilon
      if(peers.epsilon) {
        delete helpers.peersReverse[peers.epsilon];
        delete peers.epsilon;
        delete nodes.epsilon;
      }
    }
  });
  it('ledger history epsilon', async function() {
    const report = {};
    // all peers are witnesses (epsilon is not a peer anymore here and
    // the peer name is only coincidentally the same as the history name)
    const witnesses = Object.values(peers);
    const build = await helpers.buildHistory(
      {historyId: 'epsilon', mockData, nodes, witnesses});
    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const history = ledgerNode.worker.getRecentHistory();
      const {tails, witnessTails} = _getTails({history, witnesses});
      const result = _findConsensusSet({
        ledgerNode, history, tails, witnessTails, witnesses
      });
      // try {
      //   report[i] = createReport({
      //     result,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[i] = 'NONE';
      // }
      const allXs = result.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = result.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members(build.regularEvent.mergeHash);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  // add regular event on alpha before running findConsensusSet on alpha
  it('add regular local event before getting consensus', async function() {
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    // all peers are witnesses
    const witnesses = Object.values(peers);
    const build = await helpers.buildHistory(
      {historyId: 'alpha', mockData, nodes, witnesses});
    const event = await helpers.addEvent(
      {ledgerNode, eventTemplate, opTemplate});

    for(const key in nodes) {
      const ledgerNode = nodes[key];
      const history = ledgerNode.worker.getRecentHistory();
      const {tails, witnessTails} = _getTails({history, witnesses});
      const result = _findConsensusSet({
        ledgerNode, history, tails, witnessTails, witnesses
      });
      // try {
      //   report[i] = createReport({
      //     result,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[i] = 'NONE';
      // }
      const allXs = result.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = result.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members(build.regularEvent.mergeHash);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
});

function createReport({result, copyMergeHashes, copyMergeHashesIndex}) {
  const allXs = result.consensus.map(p => p.x.eventHash);
  const allYs = result.consensus.map(p => p.y.eventHash);
  const yCandidates = result.yCandidates.map(c => c.eventHash);
  // console.log('COPYHASHES', JSON.stringify(copyMergeHashes, null, 2));
  console.log('XXXXXXXXX', allXs);
  console.log('YYYYYYYYY', allYs);
  console.log('YCANDIDATE', yCandidates);
  const xIndex = allXs.map(x => copyMergeHashesIndex[x]);
  const yIndex = allYs.map(y => copyMergeHashesIndex[y]);
  const yCandidateIndex = yCandidates.map(c => copyMergeHashesIndex[c]);
  console.log('REPORTED Xs', xIndex);
  console.log('REPORTED Ys', yIndex);
  console.log('REPORTED yCandidates', yCandidateIndex);
  return {xIndex, yIndex, yCandidateIndex};
}
