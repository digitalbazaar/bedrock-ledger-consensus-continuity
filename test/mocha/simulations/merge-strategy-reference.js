/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function() {
  // pick nodes and events used for merging
  const peers = await this.getPeers();
  const idx = Math.round(Math.random() * (peers.length - 1));
  const peer = peers[idx]; // select random peer

  const peerHead = await peer.getHead();

  // merge event into history
  const events = [{nodeId: peer.nodeId, eventHash: peerHead}];
  return this.merge({events});
};
