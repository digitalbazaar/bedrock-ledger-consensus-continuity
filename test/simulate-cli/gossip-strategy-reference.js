/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');

module.exports.run = async function() {
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
