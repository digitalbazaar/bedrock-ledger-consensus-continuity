/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function() {
  // pick nodes and events used for merging
  const peers = await this.getLocalPeers();
  const idx = Math.round(Math.random() * (peers.length - 1));
  const peer = peers[idx]; // select random peer

  const peerHead = await this.getLocalPeerHead(peer);

  // merge event into history
  const events = [{nodeId: peer.nodeId, eventHash: peerHead}];
  this.merge({events});

  for(let i = 0; i < 5; ++i) {
    this.merge();
  }
};
