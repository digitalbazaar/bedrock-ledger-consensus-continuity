/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');

module.exports.run = async function({}) {
  let totalDownloadedEvents = 0;
  const gossipPeers = new Array(3);
  const gossipSessions = [];

  // ensure gossip iteration is being tracked
  if(this.gossipCounter === undefined) {
    this.gossipCounter = Math.floor(Math.random() * this.witnesses.size);
  } else {
    this.gossipCounter += 1;
  }
  // skip this node in witness selection
  if((this.gossipCounter % this.witnesses.size) === parseInt(this.nodeId, 10)) {
    this.gossipCounter += 1;
  }

  // generate Map of peers that have notified this peer
  const peerNotifications = new Map();
  this.nodes.forEach(node => {
    if(node.nodeId === this.nodeId) {
      // skip if the node is the current node
      return;
    }
    const notified = (2 / this.witnesses.size) < Math.random();
    if(notified) {
      peerNotifications.set(node.nodeId, node);
    }
  });

  // select the peers to gossip with
  const roundRobinWitness = this.witnesses.get(
    (this.gossipCounter % this.witnesses.size).toString());
  peerNotifications.delete(roundRobinWitness.nodeId);
  const randomPeer1 = _selectAndRemoveRandomPeer({peers: peerNotifications});
  const randomPeer2 = _selectAndRemoveRandomPeer({peers: peerNotifications});
  gossipPeers.push(randomPeer1);
  gossipPeers.push(randomPeer2);
  gossipPeers.push(roundRobinWitness);

  // gossip with selected peers that have notified this peer
  for(const peer of gossipPeers) {
    if(peer) {
      totalDownloadedEvents += await _gossipNodeEvents(this, peer);
      gossipSessions.push({peer: peer.nodeId, events: totalDownloadedEvents});
    }
  }

  return gossipSessions;
};

function _selectAndRemoveRandomPeer({peers}) {
  let selection = undefined;
  const allPeerIds = Array.from(peers.keys());
  if(allPeerIds.length > 0) {
    const randomId = allPeerIds[Math.floor(Math.random() * allPeerIds.length)];
    selection = peers.get(randomId);
    peers.delete(randomId);
  }

  return selection;
}

async function _gossipNodeEvents(node, peer) {
  // get different histories
  const nodeHistory = await node.getHistory();
  const peerHistory = await peer.getHistory();

  // get different events
  const nodeEvents = nodeHistory.events.map(({eventHash}) => eventHash);
  const peerEvents = peerHistory.events.map(({eventHash}) => eventHash);

  // diff histories
  const diff = _.difference(peerEvents, nodeEvents);

  // download events from peer
  const downloadedEvents = await peer.getEvents({events: diff});

  // add events to node
  await node.addEvents({events: downloadedEvents});

  const gossips = node.activityLog.filter(({type}) => type === 'gossip');
  const downloadedEventsTotal = gossips.reduce((acc, curr) => {
    Object.values(curr.details).forEach(event => {
      if(typeof event === 'object') {
        acc += event.events;
      }
    });
    return acc;
  }, 0);

  const totalEvents = downloadedEventsTotal + downloadedEvents.length;
  console.log(`  gossip ${node.nodeId} <- ${peer.nodeId}:` +
    ` ${downloadedEvents.length}/${totalEvents}`);

  return downloadedEvents.length;
}
