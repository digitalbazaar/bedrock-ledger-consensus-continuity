/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');

module.exports.run = async function() {
  let totalDownloadedEvents = 0;
  const gossipSessions = [];

  // generate array of peers that have notified this peer
  const notificationPeers = await _getNotificationPeers(this);

  // initialize the peer delta list
  const peerDeltas =
    notificationPeers.map(peer => new Object({delta: 0, peer}));
  // select the peers to gossip with, ensuring at least 1 witness
  let needWitness = true;
  // try to gossip with one witness and two other peers that maximize info
  let i = 0;
  do {
    // gossip with select peer that has notified this peer
    await _recalculatePeerDeltas(this, peerDeltas);
    const peer = _selectAndRemoveMaxDelta({peerDeltas, witness: needWitness});
    if(peer) {
      totalDownloadedEvents += await _gossipNodeEvents(this, peer);
      gossipSessions.push({peer: peer.nodeId, events: totalDownloadedEvents});
    }

    needWitness = false;
    i++;
  } while(i < 3);

  return gossipSessions;
};

async function _getNotificationPeers(node) {
  const peers = [];

  for(const peer of node.nodes.values()) {
    if(peer.nodeId === node.nodeId) {
      // skip if the node is the current node
      continue;
    }
    const notified = (2 / node.nodes.size) < Math.random();
    if(notified) {
      peers.push(peer);
    }
  }

  return peers;
}

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

  // attempt to select the greatest information delta if no selection yet
  if(selection === undefined) {
    for(let i = 0; i < peerDeltas.length; i++) {
      if(peerDeltas[i] !== undefined) {
        if(peerDeltas[i].delta > 0) {
          selection = peerDeltas[i].peer;
          delete peerDeltas[i];
          break;
        } else {
          delete peerDeltas[i];
        }
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

async function _recalculatePeerDeltas(node, peerDeltas) {
  for(const peerDelta of peerDeltas) {
    if(peerDelta) {
      peerDelta.delta = await _getDelta(node, peerDelta.peer);
    }
  }

  // sort notifications by max delta
  peerDeltas.sort((a, b) => b.delta - a.delta);
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
  let logMessage = `  gossip ${node.nodeId} <- ${peer.nodeId}`;
  if(peer.isWitness) {
    logMessage += ' (witness)';
  }
  logMessage += `: ${downloadedEvents.length}/${totalEvents}`;
  console.log(logMessage);

  return downloadedEvents.length;
}
