/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');

module.exports.run = async function() {
  let totalDownloadedEvents = 0;
  const gossipPeers = new Array(3);
  const gossipSessions = [];

  // generate array of peers that have notified this peer
  const peerDeltas = new Array();
  for(const peer of this.nodes.values()) {
    if(peer.nodeId === this.nodeId) {
      // skip if the node is the current node
      continue;
    }
    const notified = (2 / this.nodes.size) < Math.random();
    if(notified) {
      const delta = await _getDelta(this, peer);
      if(delta > 0) {
        peerDeltas.push({delta, peer});
      }
    }
  }

  // sort notitifcations by max delta
  peerDeltas.sort((a, b) => b.delta - a.delta);

  // select the peers to gossip with, ensuring at least 1 witness
  const maxDeltaWitness =
    _selectAndRemoveMaxDelta({peerDeltas, witness: true});
  const maxDeltaPeer1 = _selectAndRemoveMaxDelta({peerDeltas});
  const maxDeltaPeer2 = _selectAndRemoveMaxDelta({peerDeltas});
  gossipPeers.push(maxDeltaPeer1);
  gossipPeers.push(maxDeltaPeer2);
  gossipPeers.push(maxDeltaWitness);

  // gossip with selected peers that have notified this peer
  for(const peer of gossipPeers) {
    if(peer) {
      totalDownloadedEvents += await _gossipNodeEvents(this, peer);
      gossipSessions.push({peer: peer.nodeId, events: totalDownloadedEvents});
    }
  }

  return gossipSessions;
};

function _selectAndRemoveMaxDelta({peerDeltas, witness}) {
  let selection = undefined;

  // try to select a witness first if requested
  if(witness) {
    for(let i = 0; i < peerDeltas.length; i++) {
      if(peerDeltas[i].peer.isWitness) {
        selection = peerDeltas[i].peer;
        delete peerDeltas[i];
        break;
      }
    }
  }

  // attempt to select the greatest information delta
  if(selection === undefined) {
    for(let i = 0; i < peerDeltas.length; i++) {
      if(peerDeltas[i] !== undefined) {
        selection = peerDeltas[i].peer;
        delete peerDeltas[i];
        break;
      }
    }
  }

  return selection;
}

async function _getDelta(node, peer) {
  // get different histories
  const nodeHistory = await node.getHistory();
  const peerHistory = await peer.getHistory();

  // get different events
  const nodeEvents = nodeHistory.events.map(({eventHash}) => eventHash);
  const peerEvents = peerHistory.events.map(({eventHash}) => eventHash);

  // diff histories
  const diff = _.difference(peerEvents, nodeEvents);

  return diff.length;
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
