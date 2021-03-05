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
