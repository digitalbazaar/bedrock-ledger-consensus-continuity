/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function({
  witnessTargetThreshold, witnessMinimumThreshold,
  peerTargetThreshold, peerMinimumThreshold}) {
  const f = (this.witnesses.size - 1) / 3;
  const witnesses = new Map();
  let events = [];
  const localWitnessPeers = await this.getLocalWitnessPeers();
  // operation batch is full (10% chance)
  const operationBatchFull = Math.random() < 0.1;
  // merge timeout reached (20% chance)
  const mergeTimeout = Math.random() < 0.2;

  // build the witness map
  localWitnessPeers.forEach(witness => {
    witnesses.set(witness.nodeId, witness);
  });

  // set the target/minimum thresholds depending on if witness or not
  let targetThreshold;
  let minimumThreshold;
  if(this.isWitness) {
    targetThreshold = _witnessFormulaToNumber(f, witnessTargetThreshold);
    minimumThreshold = _witnessFormulaToNumber(f, witnessMinimumThreshold);
  } else {
    targetThreshold = _witnessFormulaToNumber(f, peerTargetThreshold);
    minimumThreshold = _witnessFormulaToNumber(f, peerMinimumThreshold);
  }

  // get the witness events to merge with
  events = await _getWitnessEvents({
    node: this, witnesses, targetThreshold, minimumThreshold});

  if((operationBatchFull || mergeTimeout) &&
    events.length >= minimumThreshold) {
    // merge if the operation batch is full, merge timeout is reached, and
    // the minimum threshold for witness events is met
    this.merge({events});
  } else if(events.length >= targetThreshold) {
    // merge if the target threshold for witness events is met
    this.merge({events});
  }

  return;
};

function _witnessFormulaToNumber(f, threshold) {
  let thresholdWitnessEvents = 0;

  if(threshold === '2f') {
    thresholdWitnessEvents = 2 * f;
  } else if(threshold === 'f') {
    thresholdWitnessEvents = f;
  } else if(threshold === '1') {
    thresholdWitnessEvents = 1;
  } else if(threshold === '0') {
    thresholdWitnessEvents = 0;
  } else {
    console.log('error: theshold-merge - unsupported witness threshold:',
      threshold);
    process.exit(1);
  }

  return thresholdWitnessEvents;
}

async function _getWitnessEvents({
  node, witnesses, targetThreshold, minimumThreshold}) {
  const witnessEvents = [];
  const witnessIds = Array.from(witnesses.keys());
  _shuffleArray(witnessIds);

  // try to find a minimum of required other witness events to merge
  for(const witnessId of witnessIds) {
    if(witnessEvents.length >= targetThreshold) {
      // bail early if targetThreshold found
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
  if(witnessEvents.length >= minimumThreshold) {
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
