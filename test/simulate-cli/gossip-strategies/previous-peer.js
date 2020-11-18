/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');

module.exports.run = async function() {
  // run peer selector algorithm
  const peers = await this.getPeers();

  // always talk to the peer directly before you in line
  const localNodeId = parseInt(this.nodeId, 10);
  let peerId;
  if(localNodeId === 0) {
    peerId = this.nodes.size - 1;
  } else {
    peerId = localNodeId - 1;
  }

  const peer = peers.find(p => p.nodeId === peerId.toString());

  if(!peer) {
    console.log('EXITING NO GOSSIP PEER');
    process.exit();
  }

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

  const gossips = this.activityLog.filter(({type}) => type === 'gossip');
  const downloadedEventsTotal = gossips.reduce((acc, curr) => {
    acc += curr.details.events;
    return acc;
  }, 0);

  console.log('downloadedEvents:', downloadedEvents.length);
  console.log(
    'downloadedEventsTotal:', downloadedEventsTotal + downloadedEvents.length);

  return {peer: peer.nodeId, events: downloadedEvents.length};
};
