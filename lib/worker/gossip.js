/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _client = require('../client');
const _peers = require('../peers');
const _peerEvents = require('../peerEvents');
const bedrock = require('bedrock');
const logger = require('../logger');

exports.runGossipCycle = async ({worker, needsGossip}) => {
  // always run the gossip cycle at least once, but continue running it
  // until we contact at least one other peer
  let mergeEventsReceived = 0;
  const {peerSelector, consensusState: {blockHeight, priorityPeers}} = worker;
  const isPriorityPeer = priorityPeers.includes(worker.localPeerId);
  let busy = false;
  let contacted = false;

  /* Retry peer selector up to a maximum of 10 times; this limit was selected
  because we want it to be:
  1. High enough to detect that a significant sampling of the possible peers to
    communicate with has yielded nothing but busy peers.
  2. Not so high that we hard spin for too long. */
  let retries = 10;
  do {
    // get a set of peers to pull from with during this cycle
    const peers = await peerSelector.selectPullPeers();
    if(peers.length === 0) {
      // no peers to communicate with
      break;
    }
    for(const peer of peers) {
      // gossip with `peer`
      const {mergeEventsReceived: received, cursor, success} =
        await _gw({worker, peer});
      if(success) {
        contacted = true;
        mergeEventsReceived += received;
        // if a greater `requiredBlockHeight` is required, break to run
        // consensus to produce more blocks
        if(cursor && cursor.requiredBlockHeight > blockHeight) {
          break;
        }
      }
    }
    // if peer is a priority peer, always break after contacting at least
    // one peer
    if(contacted && isPriorityPeer) {
      break;
    }
    retries--;
  } while(!worker.halt() && retries > 0 &&
    (!contacted || (needsGossip && mergeEventsReceived === 0)));

  // if no other peers were contacted and we're not a priority peer, then
  // indicate that the network is too busy (and we need to cooldown)
  if(!contacted && !isPriorityPeer) {
    busy = true;
  }

  return {mergeEventsReceived, busy};
};

exports.sendNotification = async ({localPeerId, peerSelector} = {}) => {
  // notify all selected peers in parallel
  const peers = await peerSelector.selectNotifyPeers();
  const promises = [];
  for(const peer of peers) {
    promises.push(_notifyPeer({localPeerId, peer}));
  }
  try {
    await Promise.all(promises);
  } catch(e) {
    // ignore errors here, they are logged in `_notifyPeer`
  }
};

async function _gw({worker, peer}) {
  let mergeEventsReceived = 0;

  // get needed events from server
  let result;
  try {
    result = await _getNeeded({worker, peer});
  } catch(error) {
    // do not throw; an error communicating with a peer does not prevent a
    // critical change in state, so it should not terminate the work session
    logger.debug(
      'A non-critical error occurred during gossip', {peer: peer.id, error});

    // record connection/timeout error as non-fatal
    // FIXME: ensure that all client errors properly report as data errors
    // and not just network errors if it's clear that the remote peer is
    // not speaking (or violating) protocol
    const fatal = error.name === 'DataError';
    await peer.fail({error, fatal});
    return {mergeEventsReceived, cursor: null, success: false};
  }

  // download was successful; now process what was downloaded...
  const {events, neededHashes} = result;
  if(events.length > 0) {
    // try to add the batch of events received (this will validate the events
    // and the operations therein)
    const batchResult = await _peerEvents.addBatch(
      {worker, events, neededHashes, remotePeerId: peer.id});
    const {valid, error, fatal} = batchResult;
    if(!valid) {
      // record gossip session as a failure due to invalid events; this is
      // non-critical to our work session, but a fatal error for the
      // remote peer
      logger.debug(
        'A non-critical error occurred during gossip batch processing.',
        {peer: peer.id, error});
      await peer.fail({error, fatal});
      return {mergeEventsReceived, cursor: null};
    }
    // update merge events received
    ({mergeEventsReceived} = batchResult);

    // if the `batchResult` indicates that events were "withheld", then do not
    // store the cursor, keep the last one
    const {withheld} = batchResult;
    if(withheld) {
      // preserve previous gossip cursor
      const {cursor} = peer.getStatus();
      result.cursor = cursor;
    }
  }

  // add sample peers
  const {ledgerNode} = worker;
  const {samplePeers} = result;
  await Promise.all(samplePeers.map(remotePeer =>
    _peers.optionallyAddPeer({ledgerNode, remotePeer})));

  // consider gossip session a success
  // store `cursor` that indicates what to request in subsequent call
  const {cursor} = result;
  await peer.success({mergeEventsReceived, cursor});
  return {mergeEventsReceived, cursor, success: true};
}

async function _getNeeded({worker, peer}) {
  const {localPeerId, ledgerNode} = worker;

  // communicate the latest block height and any creator heads beyond it to
  // the peer
  // FIXME: fix access of private `_peer`
  const remotePeer = {id: peer._peer.id, url: peer._peer.url};
  logger.verbose('Start _getNeeded', {remotePeer});
  const startTime = Date.now();
  try {
    // get non-consensus peer heads and latest `blockHeight` to send to server
    // as well as current peer status; set `peerLimit` to the maximum batch
    // size
    const {'ledger-consensus-continuity': {gossip}} = bedrock.config;
    const peerLimit = gossip.maxEvents;
    const peerHeadsMap = await worker._getNonConsensusPeerHeads(
      {countPerPeer: 2, peerLimit});
    const {blockHeight: basisBlockHeight} = worker.consensusState;
    let localEventNumber;

    // ensure that the peer status `cursor` information is used
    const peerStatus = peer.getStatus();
    if(peerStatus.cursor) {
      // use cursor's `localEventNumber` if present and non-zero
      if(peerStatus.cursor.localEventNumber) {
        ({localEventNumber} = peerStatus.cursor);
      }
    }

    const result = await _client.getHistory({
      basisBlockHeight, localPeerId, peerHeadsMap, remotePeer,
      localEventNumber
    });

    const {batch, cursor, samplePeers = []} = result;
    if(batch.eventHash.length === 0) {
      // peer has nothing to share
      return {events: [], neededHashes: [], cursor, samplePeers};
    }

    // check to see what's needed from the peer by diffing with the cache
    // and local storage
    const eventHashes = batch.eventHash;
    const neededHashes = await _diff({worker, eventHashes});
    if(neededHashes.length === 0) {
      // we already have what we need from other peers, so no need to download
      // any events that were announced... but there may be more if the history
      // was truncated; we must also ensure we return `cursor` which indicates
      // information to use in the next request by the client... we MUST keep
      // track of this so subsequent calls can use it to prevent the client
      // from getting stuck if more events are required from the server to make
      // the next block
      return {events: [], neededHashes, cursor, samplePeers};
    }

    const timer = new _cache.Timer();
    timer.start({
      name: 'eventsDownloadDurationMs',
      ledgerNodeId: ledgerNode.id
    });
    const events = await _client.getEvents(
      {eventHashes: neededHashes, remotePeer});
    timer.stop();

    return {events, neededHashes, cursor, samplePeers};
  } catch(e) {
    throw e;
  } finally {
    logger.verbose('End _getNeeded', {duration: Date.now() - startTime});
  }
}

async function _diff({worker, eventHashes}) {
  // first check worker state
  let notFound = worker._difference({eventHashes});
  if(notFound.length === 0) {
    return [];
  }
  // ...of the events not found in the worker state, return those that
  // are also not in storage (mongo), i.e. we haven't stored them at all
  const {ledgerNode} = worker;
  notFound = await ledgerNode.storage.events.difference(notFound);
  if(notFound.length === 0) {
    return [];
  }
  // perserve the order of the hashes as they are DAG-sorted
  const needSet = new Set(notFound);
  const result = [];
  for(const eventHash of eventHashes) {
    if(needSet.has(eventHash)) {
      result.push(eventHash);
    }
  }
  return result;
}

async function _notifyPeer({localPeerId, peer}) {
  // FIXME: fix access of private `_peer`
  const remotePeer = {id: peer._peer.id, url: peer._peer.url};
  try {
    await _client.notifyPeer({localPeerId, remotePeer});
  } catch(error) {
    logger.debug(
      'A non-critical notification error occurred', {peer: peer.id, error});
  }
}
