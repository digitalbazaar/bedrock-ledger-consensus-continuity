/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
// const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
// const util = require('util');
// const uuid = require('uuid/v4');

let consensusApi;

describe.only('Election API _findMergeEventProof', () => {
  before(async () => helpers.prepareDatabase(mockData));

  let _findMergeEventProof;
  let _getElectorBranches;
  let genesisBlock;
  let getRecentHistory;
  let EventWriter;
  const nodes = {};
  const peers = {};

  beforeEach(async function() {
    this.timeout(120000);
    const ledgerConfiguration = mockData.ledgerConfiguration;
    const flush = helpers.flushCache();
    const clean = helpers.removeCollections(['ledger', 'ledgerNode']);
    await Promise.all([flush, clean]);

    consensusApi = helpers.use('Continuity2017').api;
    getRecentHistory = consensusApi._events.getRecentHistory;
    _getElectorBranches = consensusApi._election._getElectorBranches;
    _findMergeEventProof = consensusApi._election._findMergeEventProof;
    EventWriter = consensusApi._worker.EventWriter;

    nodes.alpha = await brLedgerNode.add(null, {ledgerConfiguration});

    // const {id: creatorId} = await consensusApi._voters.get({
    //   ledgerNodeId: nodes.alpha.id
    // });
    // const genesisMerge = consensusApi._events.getHead({
    //   creatorId:
    //   ledgerNode: nodes.alpha
    // });
    genesisBlock = (await nodes.alpha.blocks.getGenesis()).genesisBlock.block;

    [nodes.beta, nodes.gamma, nodes.delta] = await Promise.all([
      brLedgerNode.add(null, {genesisBlock}),
      brLedgerNode.add(null, {genesisBlock}),
      brLedgerNode.add(null, {genesisBlock}),
      // NOTE: if nodes.epsilon is enabled, be sure to add above
      //brLedgerNode.add(null, {genesisBlock})
    ]);

    return Promise.all(Object.keys(nodes).map(async name => {
      // setup creator
      const ledgerNode = nodes[name];
      const {id: ledgerNodeId} = ledgerNode;
      // attach eventWriter to the node
      ledgerNode.eventWriter = new EventWriter(
        {immediate: true, ledgerNode});
      const voter = await consensusApi._voters.get({ledgerNodeId});
      peers[name] = voter.id;
      ledgerNode.creatorId = voter.id;
      helpers.peersReverse[voter.id] = name;
    }));
  });
  it('ledger history alpha', async () => {
    const report = {};
    const build = await helpers.buildHistory({
      consensusApi, historyId: 'alpha', mockData, nodes
    });
    // NOTE: for ledger history alpha, all nodes should have the same view
    // all peers are electors
    const electors = _.values(peers).map(p => ({id: p}));
    for(const name of Object.keys(nodes)) {
      const ledgerNode = nodes[name];
      const history = await getRecentHistory({
        creatorId: ledgerNode.creatorId,
        ledgerNode, excludeLocalRegularEvents: true
      });
      const branches = _getElectorBranches({
        history,
        electors
      });
      const proof = _findMergeEventProof({
        ledgerNode,
        history,
        tails: branches,
        electors
      });
      // try {
      //   report[name] = proofReport({
      //     proof,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[name] = 'NO PROOF';
      // }
      const allXs = proof.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = proof.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members([
        build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
        build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
      ]);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  it('ledger history beta', async () => {
    const report = {};
    const build = await helpers.buildHistory({
      consensusApi, historyId: 'beta', mockData, nodes
    });
    // all peers are electors
    const electors = _.values(peers).map(p => ({id: p}));
    for(const name of Object.keys(nodes)) {
      const ledgerNode = nodes[name];
      const history = await getRecentHistory({
        creatorId: ledgerNode.creatorId,
        ledgerNode, excludeLocalRegularEvents: true
      });
      const branches = _getElectorBranches({
        history,
        electors
      });
      const proof = _findMergeEventProof({
        ledgerNode,
        history,
        tails: branches,
        electors
      });
      // try {
      //   report[name] = proofReport({
      //     proof,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[name] = 'NO PROOF';
      // }
      const allXs = proof.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = proof.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members([
        build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
        build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
      ]);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  it('ledger history gamma', async () => {
    const report = {};
    const build = await helpers.buildHistory({
      consensusApi, historyId: 'gamma', mockData, nodes
    });
    // all peers are electors
    const electors = _.values(peers).map(p => ({id: p}));
    for(const name of Object.keys(nodes)) {
      const ledgerNode = nodes[name];
      const history = await getRecentHistory({
        creatorId: ledgerNode.creatorId,
        ledgerNode, excludeLocalRegularEvents: true
      });
      const branches = _getElectorBranches({
        history,
        electors
      });
      const proof = _findMergeEventProof({
        ledgerNode,
        history,
        tails: branches,
        electors
      });
      // try {
      //   report[name] = proofReport({
      //     proof,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[name] = 'NO PROOF';
      // }
      const allXs = proof.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = proof.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members([
        build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
        build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
      ]);
    }
    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  // involves 4 elector nodes and one non-elector
  it('ledger history delta produces same as alpha result', async () => {
    const report = {};
    // add node epsilon for this test and remove it afterwards
    nodes.epsilon = await brLedgerNode.add(null, {genesisBlock});
    nodes.epsilon.eventWriter = new EventWriter({
      immediate: true, ledgerNode: nodes.epsilon
    });
    const voter = await consensusApi._voters.get({
      ledgerNodeId: nodes.epsilon.id
    });
    peers.epsilon = voter.id;
    nodes.epsilon.creatorId = voter.id;
    helpers.peersReverse[voter.id] = 'epsilon';

    const build = await helpers.buildHistory({
      consensusApi, historyId: 'delta', mockData, nodes
    });
    // NOTE: for ledger history alpha, all nodes should have the same view
    // all peers are electors
    const electors = _.values(peers).map(p => ({id: p}));
    for(const name of Object.keys(nodes)) {
      const ledgerNode = nodes[name];
      const history = await getRecentHistory({
        creatorId: ledgerNode.creatorId,
        ledgerNode, excludeLocalRegularEvents: true
      });
      const branches = _getElectorBranches({
        history,
        electors
      });
      const proof = _findMergeEventProof({
        ledgerNode,
        history,
        tails: branches,
        electors
      });
      // try {
      //   report[name] = proofReport({
      //     proof,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[name] = 'NO PROOF';
      // }
      const allXs = proof.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      const mergeHashes = [
        build.regularEvent.alpha.mergeHash,
        build.regularEvent.beta.mergeHash,
        build.regularEvent.gamma.mergeHash,
        build.regularEvent.delta.mergeHash
        // exclude epsilon (non-elector)
      ];
      allXs.should.have.same.members(mergeHashes);
      const allYs = proof.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members([
        build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
        build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
      ]);
    }

    // cleanup
    delete nodes.epsilon;

    // console.log('FINAL REPORT', JSON.stringify(report, null, 2));
  });
  // FIXME: enable test
  it('ledger history epsilon', async () => {
    const report = {};
    const build = await helpers.buildHistory({
      consensusApi, historyId: 'epsilon', mockData, nodes
    });
    // NOTE: for ledger history alpha, all nodes should have the same view
    // all peers are electors
    const electors = _.values(peers).map(p => ({id: p}));
    for(const name of Object.keys(nodes)) {
      const ledgerNode = nodes[name];
      const history = await getRecentHistory({
        creatorId: ledgerNode.creatorId,
        ledgerNode, excludeLocalRegularEvents: true
      });
      const branches = _getElectorBranches({
        history,
        electors
      });
      const proof = _findMergeEventProof({
        ledgerNode,
        history,
        tails: branches,
        electors
      });
      // try {
      //   report[name] = proofReport({
      //     proof,
      //     copyMergeHashes: build.copyMergeHashes,
      //     copyMergeHashesIndex: build.copyMergeHashesIndex});
      // } catch(e) {
      //   report[name] = 'NO PROOF';
      // }
      const allXs = proof.consensus.map(p => p.x.eventHash);
      allXs.should.have.length(4);
      allXs.should.have.same.members(build.regularEvent.mergeHash);
      const allYs = proof.consensus.map(p => p.y.eventHash);
      allYs.should.have.length(4);
      allYs.should.have.same.members([
        build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
        build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
      ]);
    }
    // console.log('REPORT', JSON.stringify(report, null, 2));
  });
  // add regular event on alpha before running findMergeEventProof on alpha
  it('add regular local event before getting proof', async () => {
    const ledgerNode = nodes.alpha;
    const eventTemplate = mockData.events.alpha;
    const opTemplate = mockData.operations.alpha;
    const build = await helpers.buildHistory({
      consensusApi, historyId: 'alpha', mockData, nodes
    });
    await helpers.addEvent({ledgerNode, eventTemplate, opTemplate});
    // NOTE: for ledger history alpha, all nodes should have the same view
    // all peers are electors
    const electors = _.values(peers).map(p => ({id: p}));
    const history = await getRecentHistory({
      creatorId: nodes.alpha.creatorId,
      ledgerNode, excludeLocalRegularEvents: true
    });
    const branches = _getElectorBranches({
      history,
      electors
    });
    const proof = _findMergeEventProof({
      ledgerNode,
      history,
      tails: branches,
      electors
    });
    // proofReport({
    //   proof,
    //   copyMergeHashes: build.copyMergeHashes,
    //   copyMergeHashesIndex: build.copyMergeHashesIndex});
    const allXs = proof.consensus.map(p => p.x.eventHash);
    allXs.should.have.length(4);
    allXs.should.have.same.members(build.regularEvent.mergeHash);
    const allYs = proof.consensus.map(p => p.y.eventHash);
    allYs.should.have.length(4);
    allYs.should.have.same.members([
      build.copyMergeHashes.cp5, build.copyMergeHashes.cp6,
      build.copyMergeHashes.cp7, build.copyMergeHashes.cp8
    ]);
  });
});

function proofReport({proof, copyMergeHashes, copyMergeHashesIndex}) {
  const allXs = proof.consensus.map(p => p.x.eventHash);
  const allYs = proof.consensus.map(p => p.y.eventHash);
  const yCandidates = proof.yCandidates.map(c => c.eventHash);
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
