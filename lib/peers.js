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

// cache for sampling peers from ledger node's peers collection
let PEER_SAMPLING_CACHE;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock.init', () => {
  const cfg = config['ledger-consensus-continuity'];
  const {gossip: {notification, peerSamplingCache}} = cfg;

  MAX_ADD_NEW_PEER_JOBS = notification.addNewPeerConcurrency;

  // create cache for sampling peers from a ledger node's peers collection
  PEER_SAMPLING_CACHE = new LruMemoize(peerSamplingCache);
});

// updates/adds a remote peer that has sent a notification so it can be pulled
// from
api.addNotifier = async ({ledgerNode, remotePeer} = {}) => {
  const {id, url} = remotePeer;
  try {
    await ledgerNode.peers.updateLastPushAt({id, url});
    return;
  } catch(e) {
    if(e.name !== 'NotFoundError') {
      throw e;
    }
  }

  await api.optionallyAddPeer({ledgerNode, remotePeer});
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
    try {
      await ledgerNode.peers.add({peer});
    } catch(e) {
      if(e.name !== 'DuplicateError') {
        throw e;
      }
    }
  } finally {
    _currentAddNewPeerJobs--;
  }
};

// gets a sample of peers, one with a high reputation and another with
// a low reputation but that does not appear to be failing
api.samplePeers = async ({ledgerNode} = {}) => {
  const {id: ledgerNodeId} = ledgerNode;
  const peersPromise = PEER_SAMPLING_CACHE.memoize({
    key: ledgerNodeId,
    fn: async () => _getPeers({ledgerNode})
  });

  const peers = await peersPromise;

  // no peers
  if(peers.length === 0) {
    // since no peers were available, shorten max age on peers to ensure
    // peers are refreshed again in a second
    if(PEER_SAMPLING_CACHE.cache.get(ledgerNodeId) === peersPromise) {
      PEER_SAMPLING_CACHE.cache.set(ledgerNodeId, peersPromise, 1000);
    }
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

async function _getPeers({ledgerNode}) {
  // the limit is set at 110 to account for 100 persistent peers and 10
  // that are being onboarded, a total of 110
  return ledgerNode.peers.getAll({
    maxConsecutiveFailures: 0, backoffUntil: Date.now(), limit: 110
  });
}

function _samplePeers({peers, start, end = peers.length - 1}) {
  const range = end - start + 1;
  const index = start + Math.floor(Math.random() * range);
  return peers[index];
}
