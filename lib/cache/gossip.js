/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const cache = require('bedrock-redis');
const _cacheKey = require('./cache-key');

const api = {};
module.exports = api;

/**
 * Called when a node receives a notification from another peer.
 *
 * @param recipientId the ID of the recipient of the notification.
 * @param senderId the ID of the sender of the notification.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.addNotification = async ({ledgerNodeId, senderId}) => {
  const key = _cacheKey.gossipNotification(ledgerNodeId);
  // a set is used here because it only allows unique values
  return cache.client.sadd(key, senderId);
};

api.getGossipNotification = async ({ledgerNodeId}) => {
  const key = _cacheKey.gossipNotification(ledgerNodeId);
  return cache.client.spop(key);
};

api.getPeerStatus = async ({creatorId, ledgerNodeId}) => {
  const key = _cacheKey.gossipPeerStatus({creatorId, ledgerNodeId});
  return cache.client.hmget(
    key, 'backoff', 'lastContactDate', 'lastContactResult');
};

api.gossipBehind = async ({ledgerNodeId, remove = false}) => {
  const key = _cacheKey.gossipBehind(ledgerNodeId);
  if(remove) {
    return cache.client.del(key);
  }
  return cache.client.set(key, '');
};

api.notifyFlag = async ({ledgerNodeId, remove = false, add = false}) => {
  const key = _cacheKey.gossipNotifyFlag(ledgerNodeId);
  if(remove) {
    return cache.client.del(key);
  }
  if(add) {
    return cache.client.set(key, '');
  }
  return cache.client.get(key);
};

api.setPeerStatus = async (
  {backoff, creatorId, lastContactDate, lastContactResult, ledgerNodeId}) => {
  const key = _cacheKey.gossipPeerStatus({creatorId, ledgerNodeId});
  return cache.client.multi().hmset(
    key, 'backoff', backoff, 'lastContactDate', lastContactDate,
    'lastContactResult', lastContactResult)
    // if this key is not updated in 24 hours the peer is not participating
    // in the ledger (no representation in recent blocks). If the peer
    // reappears, a new record will be created
    .expire(key, 86400)
    .exec();
};
