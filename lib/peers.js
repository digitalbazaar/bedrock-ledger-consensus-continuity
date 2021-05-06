/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config} = bedrock;
const {LruCache} = require('@digitalbazaar/lru-memoize');

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

  PEER_CACHE = new LruCache(peerCache);
});

// updates/adds a remote peer that has sent a notification so it can be pulled
// from -- if the notifier is new and the peer hasn't been pulled from since
// the last notification
api.addNotifier = async ({ledgerNode, remotePeer, localPeerId} = {}) => {
  const {id, url} = remotePeer;

  // rely on current cached value for the peer to determine whether or not
  // the peer should be updated based on the given notification
  const {records, recordMap} = await api.getCached({ledgerNode});
  const cachedRecord = recordMap.get(id);

  // only try to add peer if it is not in the cache; if it has been deleted
  // from the database and the cache is not current, then the peer will
  // eventually get added once the cache is updated
  if(!cachedRecord) {
    try {
      const record = await api.optionallyAdd(
        {ledgerNode, remotePeer, localPeerId, notifier: true});
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
    // only update cache and database if we've pulled since the last push
    // notification
    cachedPeer.status.lastPushAt = Date.now();
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

api.delete = async ({ledgerNode, id} = {}) => {
  try {
    await ledgerNode.peers.remove({id});
  } catch(e) {
    if(e.name !== 'NotFoundError') {
      throw e;
    }
  }
  // update cache
  PEER_CACHE.cache.del(ledgerNode.id);
};

// get all cached peers
api.getCached = async ({ledgerNode}) => {
  const {id: ledgerNodeId} = ledgerNode;
  return PEER_CACHE.memoize({
    key: ledgerNodeId,
    fn: async () => _getPeers({ledgerNode})
  });
};

// updates/adds a remote peer if too many other peers aren't being concurrently
// added
api.optionallyAdd = async ({
  ledgerNode, remotePeer, localPeerId, notifier = false
} = {}) => {
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
    // use cache to minimize database hits
    const {records, recordMap} = await api.getCached({ledgerNode});
    if(recordMap.has(remotePeer.id)) {
      // peer already exists
      return;
    }

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
    let peerCount = 0;
    for(const {peer: {reputation}} of records) {
      if(reputation === 0) {
        peerCount++;
        if(peerCount >= 10) {
          return;
        }
      }
    }

    // determine if the given peer requires a commitment from the local peer
    const commitmentRequired = await _isPeerCommitmentRequired(
      {ledgerNode, peerId: remotePeer.id, localPeerId});

    // if a commitment is required and the peer is not a notifier, do not
    // add it -- as it has not authenticated
    // FIXME: if we do not add the peer then we will continually run the
    // above query until we receive a notification from the peer directly...
    // or until it is onboarded, is that preferable to just adding it?
    if(commitmentRequired && !notifier) {
      return;
    }

    // add peer since it was not found and there is space to add it
    const peer = {
      ...remotePeer,
      commitmentRequired,
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
  const {records} = await api.getCached({ledgerNode});

  // remove `vetoPeerId` and any failing peers for sampling
  const peers = records.map(r => r.peer)
    .filter(p => p.id !== vetoPeerId && p.status.consecutiveFailures === 0);
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

api.update = async ({ledgerNode, peer} = {}) => {
  // update cache
  const {recordMap} = await api.getCached({ledgerNode});
  const {id} = peer;
  const record = recordMap.get(id);
  if(record) {
    // this function assumes the caller has set all defaults in `peer`
    record.peer = peer;
    record.meta.updated = Date.now();
    record.meta.pulledAfterPush =
      peer.status.lastPullAt > peer.status.lastPushAt;
    recordMap.set(id, record);
  }
  // update database
  /* Note: A previous peer update, such as a push notification or URL update
  may be overwritten here, but the remote peer will resend such a notification
  eventually. We allow for overwrites because it is more performant than trying
  to avoid them -- and the system needs to be resilient to such lost updates
  anyway. */
  await ledgerNode.peers.update({peer});
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
// gets a commitment to a peer that has reached consensus that was created
// by a non-replayer
api.getCommitmentToPeer = async ({
  ledgerNode, peerId, explain = false
} = {}) => {
  // FIXME: make this a covered query
  const query = {
    'event.peerCommitment': peerId,
    'meta.continuity2017.type': 'm',
    'meta.continuity2017.replayDetectedBlockHeight': -1,
    'meta.consensus': true
  };
  const {collection} = ledgerNode.storage.events;
  const projection = {
    _id: 0,
    'meta.continuity2017.creator': 1,
    'event.mergeHeight': 1,
    'meta.blockHeight': 1,
    'meta.eventHash': 1
  };
  const cursor = await collection.find(query, {projection}).limit(1);
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.toArray();
};

api.getPeerForCommitment = async ({
  ledgerNode, localPeerId, witnesses = []
} = {}) => {
  const {records} = await api.getCached({ledgerNode});
  // find a peer to commit to
  for(const {peer} of records) {
    if(peer.commitmentRequired !== false) {
      // confirm the peer requires a commitment from the local peer
      if(await _isPeerCommitmentRequired(
        {ledgerNode, peerId: peer.id, localPeerId, witnesses})) {
        return peer;
      }
      // update the peer so it is not checked again
      /* Note: It is conceivable that a peer could have been committed to by
      another peer that subsequently becomes a replayer, thus invalidating a
      commitment that was used to set this flag. This could only prevent a
      new peer from being able to create merge events if no other peers
      committed to it and it failed to create its first merge event before the
      replay was detected. If this unlikely event happens, the new peer will
      need another peer to commit to it, which will only happen if it is not
      on the peer list for another peer. This can be achieved by contacting
      a peer that does not have it on its list or by shutting off the peer for
      a sufficient period of time to cause it to drop off of peer lists -- and
      then restarting it. If this becomes a serious problem on the network, we
      can implement spot checks to this flag for peers that haven't produced
      any events of their own. */
      peer.commitmentRequired = false;
      await ledgerNode.peers.update({peer});
    }
  }
  return null;
};

// FIXME: move to bedrock-ledger-consensus-continuity-storage
api.hasBeenWitness = async ({ledgerNode, peerId, explain = false} = {}) => {
  // FIXME: make this a covered query
  const query = {
    'meta.continuity2017.witness': peerId
  };
  const {collection} = ledgerNode.storage.blocks;
  const projection = {_id: 0, 'meta.continuity2017.witness': 1};
  const cursor = await collection.find(query, {projection}).limit(1);
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.hasNext();
};

async function _getPeers({ledgerNode}) {
  // get full peer records so update field can be checked (`meta=true`)
  // the limit is set at 110 to account for 100 persistent peers and 10
  // that are being onboarded, a total of 110
  const records = await ledgerNode.peers.getAll({
    backoffUntil: Date.now(), limit: 110, meta: true
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

// checks to see if a peer commitment is required for the given peer to enable
// it to submit its own merge events
async function _isPeerCommitmentRequired({
  ledgerNode, peerId, localPeerId, witnesses = []
} = {}) {
  // if peer is presently a witness, then no commitment is required; note that
  // this only applies *at the block height* for the given witnesses, so only
  // pass `witnesses` with care
  if(witnesses.includes(peerId)) {
    return false;
  }
  // optimistically assume it is uncommon to be checking for a witness
  const [wasWitness, hasCommitmentOrHead] = await Promise.all([
    api.hasBeenWitness({ledgerNode, peerId}),
    _peerHasCommitmentOrHead({ledgerNode, peerId, creator: localPeerId})
  ]);
  // a commitment is required for the peer if the peer has never been a witness
  // and there is no other commitment from a non-replayer and the peer has
  // never created its own valid merge event (it has no head)
  return !(wasWitness || hasCommitmentOrHead);
}

// FIXME: move to bedrock-ledger-consensus-continuity-storage
// if `creator` is passed, then non-consensus commitments by that particular
// creator will match, otherwise, only consensus commitments will match
async function _peerHasCommitmentOrHead({
  ledgerNode, peerId, creator, limit = 1, explain = false
} = {}) {
  // FIXME: make this a covered query
  const $or = [{
    'event.peerCommitment': peerId,
    'meta.continuity2017.type': 'm',
    'meta.continuity2017.replayDetectedBlockHeight': -1,
    'meta.consensus': true
  }];
  if(creator) {
    $or.push({
      ...$or[0],
      'meta.consensus': false,
      'meta.continuity2017.creator': creator,
    });
  }
  $or.push({
    'meta.continuity2017.type': 'm',
    'meta.continuity2017.creator': peerId
  });
  const query = {$or};
  const {collection} = ledgerNode.storage.events;
  const projection = {_id: 0, 'meta.continuity2017.type': 1};
  const cursor = await collection.find(query, {projection}).limit(limit);
  if(explain) {
    return cursor.explain('executionStats');
  }
  return cursor.hasNext();
}
