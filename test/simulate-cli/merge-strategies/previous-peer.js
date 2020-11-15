/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function() {
  // pick nodes and events used for merging
  const peers = await this.getLocalPeers();

  // always merge on the peer directly before you in line
  const localNodeId = parseInt(this.nodeId, 10);
  let peerId;
  if(localNodeId === 0) {
    peerId = this.witnesses.size - 1;
  } else {
    peerId = localNodeId - 1;
  }

  const peer = peers.find(p => p.nodeId === peerId.toString());

  if(!peer) {
    console.log('EXITING NO MERGE PEER');
    process.exit();
  }

  const peerHead = await this.getLocalPeerHead(peer);

  // merge event into history
  const events = [{nodeId: peer.nodeId, eventHash: peerHead}];
  return this.merge({events});
};
