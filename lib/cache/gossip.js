/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

/**
 * Record a notification received from another peer. The notification indicates
 * that the peer has new events available for gossip.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param peerId {string} the ID of the sender of the notification.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.addNotification = async ({ledgerNodeId, peerId}) => {
  const key = _cacheKey.gossipNotification(ledgerNodeId);
  // a set is used here because it enforces unique values
  // FIXME: limit size of this set
  return cache.client.multi()
    .sadd(key, peerId)
    .publish(`continuity2017|needsMerge|${ledgerNodeId}`, 'notification')
    .exec();
};

/**
 * Get a single peer ID from the set of peer notifications.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<string>} A peer ID.
 */
exports.getGossipNotification = async ({ledgerNodeId}) => {
  const key = _cacheKey.gossipNotification(ledgerNodeId);
  return cache.client.spop(key);
};

/**
 * Get peer status information.
 *
 * @param creatorId {string} - The ID of the creator/peer.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object>} Peer status information.
 */
exports.getPeerStatus = async ({creatorId, ledgerNodeId}) => {
  const key = _cacheKey.gossipPeerStatus({creatorId, ledgerNodeId});
  return cache.client.hmget(
    key, 'backoff', 'detectedBlockHeight', 'lastContactDate',
    'lastContactResult');
};

/**
 * Get, set, or remove the outstanding gossip notification flag. This flag
 * is used to ensure that outgoing gossip notifications are resulting in
 * gossip sessions being initiated from peers.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param [add=false] {Boolean} - Add the flag.
 * @param [remove=false] {Boolean} - Remove the flag.
 *
 * @returns {Promise<string|null>} An empty string if the flag is set or null.
 */
exports.notifyFlag = async ({ledgerNodeId, remove = false, add = false}) => {
  const key = _cacheKey.gossipNotifyFlag(ledgerNodeId);
  if(remove) {
    return cache.client.del(key);
  }
  if(add) {
    return cache.client.set(key, '');
  }
  return cache.client.get(key);
};

/**
 * Set peer status information.
 *
 * @param backoff {Number} - The backoff period for the peer in ms.
 * @param creatorId {string} - The ID of the creator/peer.
 * @param lastContactDate {Number} - The last contact date in ms elapsed since
 *   the UNIX epoch.
 * @param lastContactResult {string} - The result of the last contact.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setPeerStatus = async ({
  backoff, creatorId, detectedBlockHeight, lastContactDate, lastContactResult,
  ledgerNodeId
}) => {
  const key = _cacheKey.gossipPeerStatus({creatorId, ledgerNodeId});
  return cache.client.multi().hmset(
    key,
    'backoff', backoff,
    'detectedBlockHeight', detectedBlockHeight,
    'lastContactDate', lastContactDate,
    'lastContactResult', lastContactResult)
    // if this key is not updated in 24 hours the peer is not participating
    // in the ledger (no representation in recent blocks). If the peer
    // reappears, a new record will be created
    .expire(key, 86400)
    .exec();
};
