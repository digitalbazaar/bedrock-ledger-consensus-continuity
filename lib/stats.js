/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const logger = require('./logger');
const cache = require('bedrock-redis');
const _cacheKey = require('./cache/cacheKey');

const DEFAULT_KEY_EXPIRATION = 6000;
const VALID_OPERATION_TYPES = new Set(['local', 'peer']);

exports.incrementOpsCounter = (
  {ledgerNodeId, type, count = 1, expires = DEFAULT_KEY_EXPIRATION}) => {
  if(!VALID_OPERATION_TYPES.has(type)) {
    throw new Error(`Unsupported type: "${type}."`);
  }

  let opCountKey;
  if(type === 'local') {
    opCountKey = _cacheKey.opCountLocal(
      {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  } else if(type === 'peer') {
    opCountKey = _cacheKey.opCountPeer(
      {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  }

  const txn = cache.client.multi();
  txn.incrby(opCountKey, count);
  txn.expire(opCountKey, expires);

  // we do not await the promise intentionally
  txn.exec().catch(e => logger.debug(`Non-critical ${type} ops stat error`, {
    error: e
  }));
};
