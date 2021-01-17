/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _client = require('../client');
const _peerEvents = require('../peerEvents');
const bedrock = require('bedrock');
const {config} = bedrock;
const logger = require('../logger');

exports.runGossipCycle = async ({worker, needsGossip}) => {
  // always run the gossip cycle at least once, but continue running it
  // as long as gossip is needed and no merge events have been received
  // FIXME: rearrange this so we don't hard spin when we need gossip, but
  // rather we wait for the cooldown period that can be interrupted by
  // a notification
  let contacted = false;
  let mergeEventsReceived = 0;
  // FIXME: select from more peer candidates than just the witnesses
  const {blockHeight, priorityPeers, witnesses} = worker.consensusState;
  const priorityPeerSet = new Set(priorityPeers);
  const {peerSelector} = worker;
  do {
    // get a set of peers to communicate with during this cycle
    const peers = await peerSelector.selectPeers({peerIds: witnesses});
    if(peers.length === 0) {
      // FIXME: this should not happen
      // no peers to communicate with
      break;
    }
    for(const peer of peers) {
      // FIXME: if we know that the `cursor` received from the peer requires a
      // higher `basisBlockHeight` than we have, then it is pointless to
      // contact the peer before trying to compute consensus even if
      // `needsGossip` is true, we need to account for that below

      // we must try to contact the peer if we need gossip and they are a
      // priority peer
      const mustContact = needsGossip && mergeEventsReceived === 0 &&
        priorityPeerSet.has(peer.id);

      // if we don't have to contact the peer and they are not recommended,
      // then skip communicating with them
      if(!mustContact && !await peer.isRecommended()) {
        continue;
      }

      // gossip with `peer`
      const {mergeEventsReceived: received, cursor} = await _gw({worker, peer});
      mergeEventsReceived += received;
      contacted = true;

      // if a greater `basisBlockHeight` is required, break to run consensus
      // to produce more blocks
      if(cursor && cursor.basisBlockHeight > blockHeight) {
        break;
      }
    }
    // FIXME: determine conditions under which it is safe to keep
    // looping here -- or maybe this will be removed entirely with the new
    // gossip strategy anyway
    if(contacted) {
      break;
    }
  } while(!worker.halt() && needsGossip && mergeEventsReceived === 0);

  return {mergeEventsReceived};
};

exports.sendNotification = async ({
  localPeerId, priorityPeers, peerSelector
} = {}) => {
  let sent = 0;
  let attempts = 0;
  const maxRetries = 10;
  // attempt to send notifications to two distinct peers
  const peers = await peerSelector.selectNotifyPeers({peerIds: priorityPeers});
  while(peers.length > 0 && sent < 2 && attempts < maxRetries) {
    attempts++;
    const peer = peers.shift();
    if(!peer) {
      // either there are no peers, or they are all currently failed out
      break;
    }
    const {id: remotePeerId} = peer;
    try {
      await _client.notifyPeer({localPeerId, remotePeerId});
      // FIXME: need to track success/fail network requests separate from
      // success/fail related to gossip validation, for now, do not reset
      // the peer on a successful notification
      // await peer.success();
      sent++;
    } catch(e) {
      await peer.fail(e);
      peers.push(peer);
    }
  }
};

async function _gw({worker, peer}) {
  let mergeEventsReceived = 0;

  // get needed events from server
  let result;
  try {
    result = await _getNeeded({worker, peer});
  } catch(e) {
    // do not throw; an error communicating with a peer does not prevent a
    // critical change in state, so it should not terminate the work session
    logger.debug(
      'A non-critical error occurred during gossip', {peer, error: e});

    if(e.name !== 'TimeoutError') {
      // record non-timeout errors as gossip session failures
      await peer.fail(e);
      return {mergeEventsReceived, cursor: null};
    }

    // timeout error, preserve previous gossip cursor
    const {cursor} = await peer.getStatus();

    // normalize a timeout to a gossip session that produced no events
    result = {events: [], neededHashes: [], cursor};
  }

  // at this point, download was successful (or timed out with no events),
  // but we need to process what was downloaded next...
  const {events, neededHashes} = result;
  if(events.length > 0) {
    // try to add the batch of events received (this will validate the events
    // and the operations therein)
    const batchResult = await _peerEvents.addBatch(
      {worker, events, neededHashes});
    const {valid, error} = batchResult;
    if(!valid) {
      // record gossip session as a failure due to invalid events
      logger.debug(
        'A non-critical error occurred during gossip batch processing.',
        {peer, error});
      await peer.fail(error);
      return {mergeEventsReceived, cursor: null};
    }
    // update merge events received
    ({mergeEventsReceived} = batchResult);
  }

  // consider gossip session a success
  //let backoff = 0;
  const backoff = 0;
  const {cursor} = result;
  if(!(cursor && cursor.hasMore)) {
    // FIXME: consider removing this backoff entirely, instead only doing
    // cooldown when there are no outstanding operations
    /*
    // no more gossip from the peer, add `coolDownPeriod` to backoff
    const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
    backoff = coolDownPeriod;
    */
  }
  // store `cursor` along with status for what to request in subsequent call
  await peer.success({backoff, cursor});
  return {mergeEventsReceived, cursor};
}

async function _getNeeded({worker, peer}) {
  const {localPeerId, ledgerNode} = worker;

  // communicate the latest block height and any creator heads beyond it to
  // the peer
  const {id: remotePeerId} = peer;
  logger.verbose('Start _getNeeded', {remotePeerId});
  const startTime = Date.now();
  try {
    // get non-consensus peer heads and latest `blockHeight` to send to server
    // as well as current peer status
    const [peerHeadsMap, peerStatus] = await Promise.all([
      // FIXME: make `countPerPeer` and `peerLimit` configurable
      worker._getNonConsensusPeerHeads({countPerPeer: 2, peerLimit: 100}),
      peer.getStatus()
    ]);
    let {blockHeight} = worker.consensusState;
    const basisBlockHeight = blockHeight;
    let blockEventCount;
    let localEventNumber;

    // ensure that the peer status `cursor` information is used
    if(peerStatus.cursor) {
      // only if `peerStatus.cursor.blockHeight` is defined and ahead of the
      // current `blockHeight` should we use the cursor's block information,
      // otherwise this conditional will return `false` and only the local
      // peer's `blockHeight` will be used instead
      if(peerStatus.cursor.blockHeight > blockHeight) {
        ({blockHeight, blockEventCount} = peerStatus.cursor);
      }
      // use cursor's `localEventNumber` if present and non-zero
      if(peerStatus.cursor.localEventNumber) {
        ({localEventNumber} = peerStatus.cursor);
      }
    }

    const result = await _client.getHistory({
      basisBlockHeight, blockHeight, localPeerId,
      peerHeadsMap, remotePeerId, blockEventCount, localEventNumber
    });

    const {cursor, totalEvents} = result;
    if(totalEvents === 0) {
      // peer has nothing to share
      return {events: [], neededHashes: [], cursor};
    }

    // check to see what's needed from the peer by diffing with the cache
    // and local storage
    const eventHashes = [];
    for(const block of result.consensusEvents.blocks) {
      eventHashes.push(...block.eventHash);
    }
    eventHashes.push(...result.nonConsensusEvents.eventHash);
    const neededHashes = await _diff({worker, eventHashes});
    if(neededHashes.length === 0) {
      // we already have what we need from other peers, so no need to download
      // any events that were announced... but there may be more if the history
      // was truncated; we must also ensure we return `cursor` which indicates
      // information to use in the next request by the client... we MUST keep
      // track of this so subsequent calls can use it to prevent the client
      // from getting stuck if more events are required from the server to make
      // the next block
      return {events: [], neededHashes, cursor};
    }

    let events;
    try {
      const timer = new _cache.Timer();
      timer.start({
        name: 'eventsDownloadDurationMs',
        ledgerNodeId: ledgerNode.id
      });
      events = await _client.getEvents(
        {eventHashes: neededHashes, peerId: remotePeerId});
      timer.stop();
    } catch(e) {
      if(e.details && e.details.httpStatusCode === 404) {
        // assume peer has nothing to share, the event hashes it reported
        // are not present; reset its cursor in case it was faulty
        return {events: [], neededHashes: [], cursor: null};
      }
      throw e;
    }
    return {events, neededHashes, cursor};
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
