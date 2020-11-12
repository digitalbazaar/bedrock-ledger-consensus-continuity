/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const Simulator = require('../tools/Simulator');

async function mergeStrategy() {
  // pick nodes and events used for merging
  const peers = await this.getPeers();
  const idx = Math.round(Math.random() * (peers.length - 1));
  const peer = peers[idx]; // select random peer

  const peerHead = await peer.getHead();

  // merge event into history
  const events = [{nodeId: peer.nodeId, eventHash: peerHead}];
  return this.merge({events});
}

async function gossipStrategy() {
  // run peer selector algorithm
  const peers = await this.getPeers();
  const idx = Math.round(Math.random() * (peers.length - 1));
  const peer = peers[idx]; // select random peer

  // get different histories
  const nodeHistory = await this.getHistory();
  const peerHistory = await peer.getHistory();

  // get different events
  const nodeEvents = nodeHistory.events.map(({eventHash}) => eventHash);
  const peerEvents = peerHistory.events.map(({eventHash}) => eventHash);

  // diff histories
  const diff = _.difference(peerEvents, nodeEvents);

  // download events from peer
  const downloadedEvents = await peer.getEvents({events: diff});

  // add events to node
  await this.addEvents({events: downloadedEvents});

  return {peer: peer.nodeId, events: downloadedEvents.length};
}

async function pipelineFn() {
  // const count = (((this.witnesses.size - 1) / 3 * 2) + 1);
  // const count = (this.witnesses.size - 1) / 3 * 2;
  const count = 1;

  for(let i = 0; i < count; i++) {
    // await this.run({type: 'merge', fn: mergeStrategy});
    await this.run({type: 'gossip', fn: gossipStrategy});
  }
  await this.run({type: 'merge', fn: mergeStrategy});
  const consensusResults = await this.run({type: 'consensus'});
  if(consensusResults.consensus) {
    console.log(`Found Consensus - Node ${this.nodeId}`);
  }
}

async function load({user, witnessCount}) {
  // FIXME: Get from params
  const id = 'simulation-01';
  const creator = user;

  const simulator = new Simulator({
    id, creator, witnessCount, pipeline: pipelineFn
  });

  const results = await simulator.start();
  console.log(results);
  const {graph} = simulator;

  const ledgerNodeId = '1';

  const input = {
    ledgerNodeId,
    history: graph.getHistory({nodeId: ledgerNodeId}),
    electors: graph.getElectors(),
    recoveryElectors: [],
    mode: 'first'
  };

  const display = {
    title: 'Simulation 01',
    nodeOrder: ['0', '1', '2', '3']
  };

  return {input, display};
}

module.exports = {load};
