/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config} = bedrock;
const {default: {LruMemoize}} = require('@digitalbazaar/lru-memoize');

require('./config');

// for limiting adding new peer concurrency
let _currentAddNewPeerJobs = 0;
let MAX_ADD_NEW_PEER_JOBS;

// cache for sampling peers from ledger node's peers collection and checking
// whether notifications should be stored
let PEER_CACHE;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock.init', () => {
  const cfg = config['ledger-consensus-continuity'];
  const {gossip: {notification, peerCache}} = cfg;

  MAX_ADD_NEW_PEER_JOBS = notification.addNewPeerConcurrency;

  PEER_CACHE = new LruMemoize(peerCache);
});

// updates/adds a remote peer that has sent a notification so it can be pulled
// from -- if the notifier is new and the peer hasn't been pulled from since
// the last notification
api.addNotifier = async ({ledgerNode, remotePeer} = {}) => {
  const {id, url} = remotePeer;

  // rely on current cached value for the peer to determine whether or not
  // the peer should be updated based on the given notification
  const {records, recordMap} = await _getCachedPeers({ledgerNode});
  const cachedRecord = recordMap.get(id);

  // only try to add peer if it is not in the cache; if it has been deleted
  // from the database and the cache is not current, then the peer will
  // eventually get added once the cache is updated
  if(!cachedRecord) {
    try {
      const record = await api.optionallyAddPeer({ledgerNode, remotePeer});
      if(record) {
        // update cache
        records.push(record);
        recordMap.set(record.peer.id, record);
      }
    } catch(e) {
      if(e.name !== 'DuplicateError') {
        throw e;
      }
    }
    return;
  }

  /* Only update the remote peer if its URL has changed since the last cache
  update or if the last pull follows the last push time (prior to this current
  notification). This avoids allowing a misbehaving peer to cause our peers
  collection to be updated constantly with URL changes but also ensures that
  we will eventually capture changes to URLs and push notifications that have
  happened since the last pull. */
  const {peer: cachedPeer} = cachedRecord;
  const {status: {lastPullAt, lastPushAt}} = cachedPeer;
  let updateDatabase = false;
  let usePulledAfterPush = false;
  if(cachedPeer.url !== url) {
    // update URL in cache, but only allow database update if the last update
    // was before cache max age
    cachedPeer.url = url;
    cachedRecord.meta.updated = Date.now();
    const {gossip: {peerCache}} = config['ledger-consensus-continuity'];
    const maxUpdated = Date.now() - peerCache.maxAge;
    if(cachedRecord.meta.updated <= maxUpdated) {
      updateDatabase = true;
    }
  } else if(lastPullAt > lastPushAt) {
    // only update database if we've pulled since the last push notification
    updateDatabase = true;
    usePulledAfterPush = true;
  }

  if(updateDatabase) {
    try {
      await ledgerNode.peers.updateLastPushAt({id, url, usePulledAfterPush});
      return;
    } catch(e) {
      // if we get a `NotFoundError`, we ignore it and try to add the peer
      // below *if* it wasn't in the cache; note we could get a not found error
      // because the update is too recent even when the peer exists in the
      // database
      if(e.name !== 'NotFoundError') {
        throw e;
      }
    }
  }
};

// updates/adds a remote peer if too many other peers aren't being concurrently
// added
api.optionallyAddPeer = async ({ledgerNode, remotePeer} = {}) => {
  /* Note: It is possible that the `peerCount` below could increase beyond the
  cap since we are not adding the peer atomically with the collection size
  check. The risk is that an attacker sends notifications for N many new
  peers at once and they all get added. This risk is mitigated by requiring
  all processes that could add a peer this way to be limited by a configurable
  max concurrency factor. The total number of processes multiplied by the
  concurrency indicates the maximum overflow size. When overflowed, any
  unreliable peers will eventually cycle out -- and they should not
  significantly affect existing reliable peers. */

  // if too many other new peers are being added, do not add another one
  if(_currentAddNewPeerJobs >= MAX_ADD_NEW_PEER_JOBS) {
    return;
  }

  _currentAddNewPeerJobs++;

  try {
    /* Note: A maximum of 10 peers can be onboarded concurrently. This number
    was chosen based on it being the lowest power of ten that is acceptable
    (powers of ten being human-friendly for analysis). Getting onboarded only
    means that there will be at least one pull gossip opportunity for that
    peer.

    Peers are onboarded with a reputation of `0`. The first time they are
    pulled from it will either result in success or failure. For failure, the
    peer will be deleted, making room for more peers to onboard. For success,
    the peer will be deleted if the maximum number of peers allowed to be
    stored has been reached, otherwise its reputation will increase to `1`
    causing it to become a persistent peer.

    We must always reserve some space to allow for peers that are less trusted
    or reliable to be onboarded or at least pulled from once, even if they do
    not end up becoming persistent in the peers collection.

    Peers will be cycled in and out of a ledger node's peer collection based on
    how reliable and productive they are over time. Those that are not reliable
    and productive will be removed enabling space for more peers to try their
    hand at getting persisted for longer than a single pull gossip session.

    If the number of available onboarding slots is too large, then we will
    waste too much time pulling from untrusted peers that have a greater risk
    of being unproductive. */
    // count onboarding slots (slots where rep === 0)
    // FIXME: make onboarding slots size configurable?
    const peerCount = await ledgerNode.peers.count({maxReputation: 0});
    if(peerCount >= 10) {
      return;
    }

    // add peer since it was not found and there is space to add it
    const peer = {
      ...remotePeer,
      status: {lastPushAt: Date.now()}
    };
    let record;
    try {
      record = await ledgerNode.peers.add({peer});
    } catch(e) {
      if(e.name !== 'DuplicateError') {
        throw e;
      }
    }
    return record;
  } finally {
    _currentAddNewPeerJobs--;
  }
};

// gets a sample of peers, one with a high reputation and another with
// a low reputation but that does not appear to be failing
api.samplePeers = async ({ledgerNode, vetoPeerId} = {}) => {
  const {records} = await _getCachedPeers({ledgerNode});

  // remove `vetoPeerId` from peers for sampling
  const peers = records.map(r => r.peer).filter(p => p.id !== vetoPeerId);
  if(peers.length === 0) {
    return [];
  }

  // split peers into top/bottom half by reputation
  const middle = Math.floor(peers.length / 2);

  // get a random sample from the top and bottom halves
  const set = new Set();
  set.add(_samplePeers({peers, start: 0, end: middle - 1}));
  set.add(_samplePeers({peers, start: middle}));
  return [...set];
};

async function _getCachedPeers({ledgerNode}) {
  const {id: ledgerNodeId} = ledgerNode;
  return PEER_CACHE.memoize({
    key: ledgerNodeId,
    fn: async () => _getPeers({ledgerNode})
  });
}

async function _getPeers({ledgerNode}) {
  // get full peer records so update field can be checked (`meta=true`)
  // the limit is set at 110 to account for 100 persistent peers and 10
  // that are being onboarded, a total of 110
  const records = await ledgerNode.peers.getAll({
    maxConsecutiveFailures: 0, backoffUntil: Date.now(), limit: 110, meta: true
  });
  const recordMap = new Map();
  for(const record of records) {
    recordMap.set(record.peer.id, record);
  }
  return {records, recordMap};
}

function _samplePeers({peers, start, end = peers.length - 1}) {
  const range = end - start + 1;
  const index = start + Math.floor(Math.random() * range);
  return peers[index];
}
