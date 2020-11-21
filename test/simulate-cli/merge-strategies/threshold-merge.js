/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function({witnessThreshold, peerThreshold}) {
  let events = [];
  const f = (this.witnesses.size - 1) / 3;

  // operation batch is full (10% chance)
  const operationBatchFull = Math.random() < 0.1;
  // merge timeout reached (20% chance)
  const mergeTimeout = Math.random() < 0.2;

  if(operationBatchFull || mergeTimeout) {
    // any node should try to merge if operation batch is full or a merge
    // timeout occurs and a minimum threshold of witnesses are seen
    const minWitnessEvents = _getMinWitnessEvents(f, peerThreshold);
    const witnesses = new Map();
    const localWitnessPeers = await this.getLocalWitnessPeers();
    localWitnessPeers.forEach(witness => {
      witnesses.set(witness.nodeId, witness);
    });

    events = await _getWitnessEvents({node: this, witnesses, minWitnessEvents});
  } else if(this.isWitness) {
    // witness should merge if minimum threshold of other witness events seen
    const minWitnessEvents = _getMinWitnessEvents(f, witnessThreshold);
    const witnesses = new Map();
    // remove current witness from list of available merge witnesses
    const localWitnessPeers = await this.getLocalWitnessPeers();
    localWitnessPeers.forEach(witness => {
      witnesses.set(witness.nodeId, witness);
    });

    events = await _getWitnessEvents({node: this, witnesses, minWitnessEvents});
  }

  if(events.length > 0) {
    this.merge({events});
  }

  return;
};

function _getMinWitnessEvents(f, threshold) {
  let minWitnessEvents = 0;

  if(threshold === '2f') {
    minWitnessEvents = 2 * f;
  } else if(threshold === 'f') {
    minWitnessEvents = f;
  } else if(threshold === '1') {
    minWitnessEvents = 1;
  } else {
    console.log('error: theshold-merge - unsupported witness threshold:',
      threshold);
    process.exit(1);
  }

  return minWitnessEvents;
}

async function _getWitnessEvents({node, witnesses, minWitnessEvents}) {
  const witnessEvents = [];
  const witnessIds = Array.from(witnesses.keys());
  _shuffleArray(witnessIds);

  // try to find a minimum of required other witness events to merge
  for(const witnessId of witnessIds) {
    if(witnessEvents.length >= minWitnessEvents) {
      // bail early if minWitnessEvents found
      break;
    }
    // see if the witness' head can be found
    try {
      const witness = witnesses.get(witnessId);
      const witnessHead = await node.getLocalPeerHead(witness);
      witnessEvents.push({nodeId: witnessId, eventHash: witnessHead});
    } catch(err) {
      // ignore inability to get local peer heads
    }
  }

  // return the events if we hit the minimum witness threshold
  if(witnessEvents.length === minWitnessEvents) {
    return witnessEvents;
  }

  return [];
}

function _shuffleArray(array) {
  for(let i = array.length - 1; i > 0; i--) {
    const j = Math.floor(Math.random() * (i + 1));
    [array[i], array[j]] = [array[j], array[i]];
  }
}
