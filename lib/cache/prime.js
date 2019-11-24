/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const _cacheKey = require('./cache-key');
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
  await exports.primeChildlessEvents({ledgerNode});
  logger.debug(
    'Successfully primed childless events.', {ledgerNodeId: ledgerNode.id});
  logger.debug('Successfully primed the cache.', {ledgerNodeId: ledgerNode.id});
};

exports.primeBlockHeight = async ({ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const {eventBlock: {block: {blockHeight}}} = await ledgerNode.storage
    .blocks.getLatestSummary();
  await _blocks.setBlockHeight({blockHeight, ledgerNodeId});
};

exports.primeChildlessEvents = async ({ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  const {childless, localChildless} = await exports.getChildlessEvents(
    {ledgerNode});
  const txn = cache.client.multi();
  txn.del(childlessKey);
  txn.del(localChildlessKey);
  if(childless.length > 0) {
    txn.sadd(childlessKey, childless);
  }
  if(localChildless.length > 0) {
    txn.sadd(localChildlessKey, localChildless);
  }
  await txn.exec();
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

exports.getChildlessEvents = async ({ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const {id: creatorId} = await ledgerNode.consensus._voters.get(
    {ledgerNodeId});
  const {eventHash: localHeadHash} = await ledgerNode.consensus.
    _events.getHead({creatorId, ledgerNode, useCache: false});
  const result = await ledgerNode.storage.events.collection.find({
    'meta.consensus': false
  }).project({
    _id: 0,
    'meta.eventHash': 1,
    'meta.continuity2017.creator': 1,
    'event.parentHash': 1,
  }).toArray();
  const eventsMap = new Map();
  for(const e of result) {
    eventsMap.set(e.meta.eventHash, {
      parentHash: e.event.parentHash,
      _creator: e.meta.continuity2017.creator,
      _children: 0,
    });
  }
  // compute the number of children for each event
  for(const [, event] of eventsMap) {
    for(const p of event.parentHash) {
      const parent = eventsMap.get(p);
      if(parent) {
        parent._children++;
      }
    }
  }
  const childless = [];
  const localChildless = [];
  for(const [eventHash, event] of eventsMap) {
    if(event._children === 0 && eventHash !== localHeadHash) {
      childless.push(eventHash);
      if(event._creator === creatorId) {
        localChildless.push(eventHash);
      }
    }
  }
  return {childless, localChildless};
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
