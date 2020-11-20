/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

module.exports.run = async function({witnessThreshold, peerThreshold}) {
  let events = [];
  console.log({witnessThreshold, peerThreshold});
  const outstandingMergeEvents = this.outstandingMergeEvents;
  const f = (this.witnesses.size - 1) / 3;

  // operation batch is full (10% chance)
  const operationBatchFull = Math.random() < 0.1;
  // merge timeout reached (20% chance)
  const mergeTimeout = Math.random() < 0.2;

  // create a merge event if the operation batch is full or on a merge timeout
  if(operationBatchFull || mergeTimeout) {
    let minWitnessEvents;
    const witnesses = Array.from(this.witnesses.keys());

    if(peerThreshold === '2f') {
      minWitnessEvents = 2 * f;
    } else if(peerThreshold === 'f+1') {
      minWitnessEvents = f + 1;
    } else if(peerThreshold === '1') {
      minWitnessEvents = 1;
    } else {
      console.log('error: theshold-merge - unsupported peerThreshold:',
        peerThreshold);
      process.exit(1);
    }

    events = getWitnessEvents({
      witnesses, minWitnessEvents, outstandingMergeEvents});

    return this.merge({events});
  }

  if(this.isWitness) {
    const f = (this.witnesses.size - 1) / 3;
    let minWitnessEvents;
    const otherWitnesses = Array.from(this.witnesses.keys())
      .filter(witness => witness !== this.nodeId);

    if(witnessThreshold === '2f') {
      minWitnessEvents = 2 * f;
    } else if(witnessThreshold === 'f+1') {
      minWitnessEvents = f + 1;
    } else if(witnessThreshold === 'f+1') {
      minWitnessEvents = f + 1;
    } else {
      console.log('error: theshold-merge - unsupported witnessThreshold:',
        witnessThreshold);
      process.exit(1);
    }

    console.log({f, minWitnessEvents, otherWitnesses});

    events = getWitnessEvents({
      witnesses: otherWitnesses, minWitnessEvents, outstandingMergeEvents});
  }

  return this.merge({events});
};

function getWitnessEvents({
  witnesses, minWitnessEvents, outstandingMergeEvents}) {
  let events = [];
  const witnessEvents = [];
  const availableWitnesses = Array.from(witnesses);

  // try to find a minimum of required other witness events to merge
  for(let i = 0;
    witnessEvents.length < minWitnessEvents &&
    i < outstandingMergeEvents.length; i++) {

    const dbEvent = outstandingMergeEvents[i];
    const creator = dbEvent.meta.continuity2017.creator;

    if(availableWitnesses.includes(creator)) {
      witnessEvents.push(
        {nodeId: creator, eventHash: dbEvent.eventHash});
      availableWitnesses.splice(availableWitnesses.indexOf(creator), 1);
    }
  }

  // merge if we see exactly min witness events
  if(witnessEvents.length === minWitnessEvents) {
    events = witnessEvents;
  }

  return events;
}
