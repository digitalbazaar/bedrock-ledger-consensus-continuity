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
  // as long as gossip is needed and no merge events have been received
  // FIXME: rearrange this so we don't hard spin when we need gossip, but
  // rather we wait for the cooldown period that can be interrupted by
  // a notification
  let contacted = false;
  let mergeEventsReceived = 0;
  const {blockHeight, priorityPeers} = worker.consensusState;
  const priorityPeerSet = new Set(priorityPeers);
  const {peerSelector} = worker;
  const busyPriorityPeers = new Set();
  let busy = false;
  // FIXME: do we need to do retries and track `busy` here anymore? are
  // their sufficient blockers for creating new merge events other than
  // basing this on busy detection during gossip, i.e., merge height,
  // witness thresholds, and so on?

  /* Retry peer selector up to a maximum of 10 times; this limit was selected
  because we want it to be:
  1. High enough to detect that either the priority peers are too busy or
    that a significant sampling of the possible peers to communicate with
    has yielded nothing but busy peers.
  2. Not so high that we hard spin for too long. */
  let retries = 10;
  do {
    // if all priority peers are all busy, indicate not to merge and break;
    // note that if the local peer is a priority peer, this will never happen
    // and should not block merging
    if(busyPriorityPeers.size === priorityPeerSet.size) {
      busy = true;
      break;
    }

    // get a set of peers to pull from with during this cycle
    const peers = await peerSelector.selectPullPeers();
    if(peers.length === 0) {
      // no peers to communicate with
      break;
    }
    for(const peer of peers) {
      // FIXME: the peer selector will handle `recommended`, this should
      // be removed entirely -- do we even need the priority peer busy check
      // or can we rely on the general busy check and witness merge thresholds
      // to prevent bad scenarios?

      // if a priority peer is not recommended, add them to the busy set,
      // otherwise remove them
      const recommended = await peer.isRecommended();
      if(priorityPeerSet.has(peer.id)) {
        if(!recommended) {
          busyPriorityPeers.add(peer.id);
        } else {
          busyPriorityPeers.delete(peer.id);
        }
      }

      // do not gossip with a peer that is not recommended
      if(!recommended) {
        continue;
      }

      // gossip with `peer`
      const {mergeEventsReceived: received, cursor} = await _gw({worker, peer});
      mergeEventsReceived += received;
      contacted = true;

      // if a greater `requiredBlockHeight` is required, break to run consensus
      // to produce more blocks
      if(cursor && cursor.requiredBlockHeight > blockHeight) {
        break;
      }
    }
    // FIXME: determine conditions under which it is safe to keep
    // looping here -- or maybe this will be removed entirely with the new
    // gossip strategy anyway
    if(contacted) {
      break;
    }
    retries--;
  } while(!worker.halt() && needsGossip &&
    mergeEventsReceived === 0 && retries > 0);

  // if no other peers were contacted and we're not a priority peer, then
  // indicate that the network is too busy (and we need to cooldown)
  if(!contacted && !priorityPeerSet.has(worker.localPeerId)) {
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

    // preserve previous gossip cursor
    const {cursor} = await peer.getStatus();

    if(error.name !== 'TimeoutError') {
      // record non-timeout errors as non-fatal gossip session failures
      await peer.fail({error, fatal: false});
      return {mergeEventsReceived, cursor};
    }

    // normalize a timeout to a gossip session that produced no events so
    // this peer is preferenced behind other peers
    result = {events: [], neededHashes: [], cursor};
  }

  // at this point, download was successful (or timed out with no events),
  // but we need to process what was downloaded next...
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
      const {cursor} = await peer.getStatus();
      result.cursor = cursor;
    }
  }

  // add sample peers
  const {ledgerNode} = worker;
  const {samplePeers} = result;
  await Promise.all(samplePeers.map(remotePeer =>
    _peers.optionallyAddPeer({ledgerNode, remotePeer})));

  // consider gossip session a success
  const {cursor} = result;
  // store `cursor` that indicates what to request in subsequent call
  await peer.success({mergeEventsReceived, cursor});
  return {mergeEventsReceived, cursor};
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
    const [peerHeadsMap, peerStatus] = await Promise.all([
      worker._getNonConsensusPeerHeads({countPerPeer: 2, peerLimit}),
      peer.getStatus()
    ]);
    const {blockHeight: basisBlockHeight} = worker.consensusState;
    let localEventNumber;

    // ensure that the peer status `cursor` information is used
    if(peerStatus.cursor) {
      // use cursor's `localEventNumber` if present and non-zero
      if(peerStatus.cursor.localEventNumber) {
        ({localEventNumber} = peerStatus.cursor);
      }
    }

    // FIXME:
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
