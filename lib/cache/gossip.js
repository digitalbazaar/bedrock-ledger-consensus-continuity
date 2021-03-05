/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cacheKey');

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
 * @param peerId {string} - The ID of the peer.
 * @param ledgerNodeId {string} - The ID of the ledger node.
 *
 * @returns {Promise<Object>} Peer status information.
 */
exports.getPeerStatus = async ({peerId, ledgerNodeId}) => {
  const key = _cacheKey.gossipPeerStatus({peerId, ledgerNodeId});
  const status = await cache.client.get(key);
  if(!status) {
    // no status for peer
    return null;
  }
  return JSON.parse(status);
};

/**
 * Set peer status information.
 *
 * @param ledgerNodeId {string} - The ID of the ledger node.
 * @param peerId {string} - The ID of the peer.
 * @param status {object} - The status of the peer.
 *
 * @returns {Promise} resolves once the operation completes.
 */
exports.setPeerStatus = async ({ledgerNodeId, peerId, status} = {}) => {
  const key = _cacheKey.gossipPeerStatus({peerId, ledgerNodeId});
  return cache.client.multi()
    .set(key, JSON.stringify(status))
    // if this key is not updated in 24 hours the peer is not participating
    // in the ledger (no representation in recent blocks). If the peer
    // reappears, a new record will be created
    .expire(key, 86400)
    .exec();
};
