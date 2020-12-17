/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const _cacheKey = require('./cacheKey');
const cache = require('bedrock-redis');
const logger = require('../logger');

exports.primeAll = async ({ledgerNode}) => {
  logger.debug('Priming the cache...', {ledgerNodeId: ledgerNode.id});
  await exports.primeBlockHeight({ledgerNode});
  logger.debug(
    'Successfully primed block height.', {ledgerNodeId: ledgerNode.id});
  await exports.primeOutstandingMergeEvents({ledgerNode});
  logger.debug(
    'Successfully primed outstanding merge events.',
    {ledgerNodeId: ledgerNode.id});
  logger.debug('Successfully primed the cache.', {ledgerNodeId: ledgerNode.id});
};

exports.primeBlockHeight = async ({ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const blockHeight = await ledgerNode.blocks.getLatestBlockHeight();
  await _blocks.setBlockHeight({blockHeight, ledgerNodeId});
};

exports.primeOutstandingMergeEvents = async ({ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  // get events from mongodb
  const mergeEvents = await exports.getOutstandingMergeEvents({ledgerNode});
  await exports.removeOutstandingMergeEventKeys({ledgerNodeId});
  const txn = cache.client.multi();
  txn.del(outstandingMergeKey);
  // add all outstanding merge events to the cache
  if(mergeEvents.length > 0) {
    const keys = mergeEvents.map(({meta: {eventHash}}) =>
      _cacheKey.outstandingMergeEvent({eventHash, ledgerNodeId}));
    txn.sadd(outstandingMergeKey, keys);
    for(const [index, event] of mergeEvents.entries()) {
      txn.set(keys[index], JSON.stringify(event));
    }
  }
  await txn.exec();
};

exports.getOutstandingMergeEvents = async ({ledgerNode}) =>
  ledgerNode.storage.events.collection.find({
    'meta.continuity2017.type': 'm',
    'meta.consensus': false
  }).project({
    _id: 0,
    'event.parentHash': 1,
    'event.treeHash': 1,
    // FIXME: determine if event.type is actually used, if not eliminate it
    'event.type': 1,
    'meta.eventHash': 1,
    'meta.continuity2017.creator': 1,
  }).toArray();

exports.removeOutstandingMergeEventKeys = async ({ledgerNodeId}) => {
  // this is a sample eventHash used to produce a sample key
  const eventHash = 'zQmRkvkWf3ToMhkzFbWmyfpyFDEUEjbBLWJUj7JwT14D7ex';
  const sampleKey = _cacheKey.outstandingMergeEvent({eventHash, ledgerNodeId});
  const keyPrefix = sampleKey.substr(0, sampleKey.lastIndexOf('|') + 1);
  const eventKeysInCache = await cache.client.keys(`${keyPrefix}*`);
  if(eventKeysInCache.length > 0) {
    await cache.client.del(eventKeysInCache);
  }
};
