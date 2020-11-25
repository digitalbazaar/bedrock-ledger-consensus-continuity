/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function({
  witnessTargetThreshold, witnessMinimumThreshold,
  peerMinimumThreshold, operationReadyChance = 0.2,
  byzantineWitnesses = '0'
}) {
  const f = (this.witnesses.size - 1) / 3;
  const witnesses = new Map();
  let events = [];
  const byzantineWitnessCount = _formulaToNumber(f, byzantineWitnesses);
  const localWitnessPeers = await this.getLocalWitnessPeers();

  // allowed to merge a pending operation if you meet a certain threshold;
  // nodes are only supposed to do so when their operation batch is full
  // or a merge timeout has been reached with a pending operation.
  const operationReady = Math.random() < operationReadyChance;

  // build the witness map
  localWitnessPeers.forEach(witness => {
    witnesses.set(witness.nodeId, witness);
  });

  // set the target/minimum thresholds depending on if witness or not
  let targetThreshold;
  let minimumThreshold;
  if(this.isWitness) {
    targetThreshold = _formulaToNumber(f, witnessTargetThreshold);
    minimumThreshold = _formulaToNumber(f, witnessMinimumThreshold);
  } else {
    minimumThreshold = _formulaToNumber(f, peerMinimumThreshold);
    targetThreshold = minimumThreshold;
  }

  // get the witness events to merge with
  events = await _getWitnessEvents({
    node: this, witnesses, targetThreshold, minimumThreshold});

  if(operationReady && events.length >= minimumThreshold) {
    // if this is a byzantine witness, create an empty merge, you jerk
    if(parseInt(this.nodeId) < byzantineWitnessCount) {
      events = [];
    }
    // merge if the operation batch is full, merge timeout is reached, and
    // the minimum threshold for witness events is met
    this.merge({events});
  } else if(this.isWitness && events.length >= targetThreshold) {
    // merge if the target threshold for witness events is met
    this.merge({events});
  }

  return;
};

function _formulaToNumber(f, threshold) {
  let number = 0;

  if(threshold === '2f') {
    number = 2 * f;
  } else if(threshold === 'f') {
    number = f;
  } else if(threshold === '1') {
    number = 1;
  } else if(threshold === '0') {
    number = 0;
  } else {
    console.log('error: theshold-merge - unsupported witness threshold:',
      threshold);
    process.exit(1);
  }

  return number;
}

async function _getWitnessEvents({
  node, witnesses, targetThreshold, minimumThreshold}) {
  const witnessEvents = [];
  const witnessIds = Array.from(witnesses.keys());
  _shuffleArray(witnessIds);

  // TODO: Implement merge caps
  // For witnessess: when a witness makes a non-empty event, they do at least 1
  // witness event, at least one non-witness, and then fill with 1 witness event
  // (simulation only) and then fill with non-witnesses to reach the cap of 16 -
  // 1(self)
  //
  // for 13 node network - do self, then 1 for another witness, 1 for another
  // non-witness, then 13 remain ... so use next 6 slots for witnesses (if you
  // have them) and then up to 7 more non-witnesses if you have them... then if
  // you're still under the cap, add witnesses

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
