/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function() {
  let events = [];

  // operation batch is full (10% chance)
  const operationBatchFull = Math.random() < 0.1;
  // merge timeout reached (20% chance)
  const mergeTimeout = Math.random() < 0.2;

  // create a merge event if the operation batch is full or on a merge timeout
  if(operationBatchFull || mergeTimeout) {
    const peers = await this.getLocalPeers();
    const idx = Math.round(Math.random() * (peers.length - 1));
    const peer = peers[idx]; // select random peer
    const peerHead = await this.getLocalPeerHead(peer);

    // merge event into history
    // FIXME: Is this realistic?
    // FIXME: Do we want to merge with as many as possible here?
    events.push({nodeId: peer.nodeId, eventHash: peerHead});

    return this.merge({events});
  }

  if(this.isWitness) {
    const f = (this.witnesses.size - 1) / 3;
    const minWitnessEvents = 2 * f;
    const otherWitnesses = Array.from(this.witnesses.keys())
      .filter(witness => witness !== this.nodeId);
    const otherWitnessEvents = [];

    console.log({f, minWitnessEvents, otherWitnesses});

    // try to find a minimum of required other witness events to merge
    for(let i = 0;
      otherWitnessEvents.length < minWitnessEvents &&
      i < this.outstandingMergeEvents.length; i++) {

      const dbEvent = this.outstandingMergeEvents[i];
      const creator = dbEvent.meta.continuity2017.creator;

      if(otherWitnesses.includes(creator)) {
        otherWitnessEvents.push(
          {nodeId: creator, eventHash: dbEvent.eventHash});
        otherWitnesses.splice(otherWitnesses.indexOf(creator), 1);
      }
    }

    // merge if we see exactly min witness events
    if(otherWitnessEvents.length === minWitnessEvents) {
      events = otherWitnessEvents;
    }
  }

  return this.merge({events});
};
