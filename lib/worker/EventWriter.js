/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('../cache/cacheKey');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('../logger');
const {BedrockError} = bedrock.util;

module.exports = class EventWriter {
  constructor({ledgerNode}) {
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.config = config['ledger-consensus-continuity'].writer;
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.storage = ledgerNode.storage;
    this.cacheKey = {
      childless: _cacheKey.childless(this.ledgerNodeId),
      eventQueue: _cacheKey.eventQueue(this.ledgerNodeId),
      eventQueueSet: _cacheKey.eventQueueSet(this.ledgerNodeId),
      outstandingMerge: _cacheKey.outstandingMerge(this.ledgerNodeId),
    };
  }

  async write() {
    return this._clearQueue();
  }

  async _clearQueue() {
    let error;
    let creators = [];
    try {
      const rangeData = await cache.client.lrange(
        this.cacheKey.eventQueue, 0, -1);
      const rawEvents = await this._getEvents({rangeData});
      // not async
      const processedData = this._processEvents({rawEvents});
      ({creators} = await this._storeEvents({processedData, rangeData}));
    } catch(e) {
      error = e;
    }
    if(error && error.name !== 'AbortError') {
      logger.error(`Error in event writer: ${this.ledgerNodeId}`, {error});
    }
    return {creators};
  }

  async _getEvents({rangeData}) {
    if(!rangeData || rangeData.length === 0) {
      throw new BedrockError('Nothing to do.', 'AbortError');
    }
    const result = await cache.client.mget(rangeData);
    // TODO: use fastest looping algo
    // filter out nulls, nulls occur when an event was a duplicate
    const eventsJson = result.filter(r => r !== null);
    if(eventsJson.length === 0) {
      throw new BedrockError('Nothing to do.', 'AbortError');
    }
    // throw if there is a failure here because there is a serious problem
    return eventsJson.map(JSON.parse);
  }

  _processEvents({rawEvents}) {
    const now = Date.now();
    const events = [];
    const eventHashes = new Set();
    const _parentHashes = [];
    const headHashes = new Set();
    const creatorHeads = {};
    const operations = [];
    for(const {event, meta} of rawEvents) {
      const {eventHash} = meta;

      eventHashes.add(eventHash);
      _.defaults(meta, {created: now, updated: now});
      events.push({event, meta});

      if(event.type === 'WebLedgerOperationEvent') {
        operations.push(...event.operationRecords);
        delete event.operationRecords;
      }

      // build a list of treeHashes that are not included in this batch
      if(meta.continuity2017.type === 'm') {
        const {basisBlockHeight, mergeHeight} = event;
        const {creator: creatorId, generation, localAncestorGeneration} =
          meta.continuity2017;
        // capturing the *last* head for each creator
        creatorHeads[creatorId] = {
          eventHash,
          generation,
          basisBlockHeight,
          mergeHeight,
          localAncestorGeneration
        };
        headHashes.add(eventHash);
        _parentHashes.push(...event.parentHash);
      }
    }
    const parentHashes = _.uniq(_parentHashes);
    return {
      creatorHeads, eventHashes, events, headHashes, operations, parentHashes
    };
  }

  async _storeEvents({processedData, rangeData}) {
    const {
      creatorHeads, events, eventHashes, headHashes, operations, parentHashes
    } = processedData;
    if(operations.length !== 0) {
      logger.debug(`Attempting to store ${operations.length} operations.`);
      await this.storage.operations.addMany({operations});
    }
    logger.debug(`Attempting to store ${events.length} events.`);
    // retry on duplicate events until all events have been processed
    const storeEvents = await this.storage.events.addMany({events});
    logger.debug('Successfully stored events and operations.');
    const {ledgerNodeId} = this;
    const eventCountCacheKey = _cacheKey.eventCountPeer({
      ledgerNodeId,
      second: Math.round(Date.now() / 1000)
    });
    const newHeadCreators = new Set();
    const multi = cache.client.multi();
    multi.incrby(eventCountCacheKey, events.length);
    multi.expire(eventCountCacheKey, this.eventsConfig.counter.ttl);
    // remove items from the list
    // ltrim *keeps* items from start to end
    multi.ltrim(this.cacheKey.eventQueue, rangeData.length, -1);
    multi.srem(this.cacheKey.eventQueueSet, Array.from(eventHashes));
    // update heads
    const creators = Object.keys(creatorHeads);
    // contains the current cache keys for all new heads
    const currentKeysForNewHeads = new Set();
    // contains new cache keys for all new heads
    const newHeadKeys = new Set();
    if(creators.length !== 0) {
      const {dupHashes} = storeEvents;
      const dupSet = new Set(dupHashes);
      // used to identify childless events
      const hashFilter = new Set(parentHashes.concat(dupHashes));
      const newHeads = [];
      // update the key that contains a hash of eventHash and generation
      creators.forEach(creatorId => {
        const headKey = _cacheKey.head({creatorId, ledgerNodeId});
        const {
          eventHash, generation, basisBlockHeight, mergeHeight,
          localAncestorGeneration
        } = creatorHeads[creatorId];
        multi.hmset(
          headKey,
          'h', eventHash,
          'g', generation,
          'bh', basisBlockHeight,
          'mh', mergeHeight,
          'la', localAncestorGeneration);
        if(!hashFilter.has(eventHash)) {
          newHeads.push(eventHash);
          newHeadCreators.add(creatorId);
        }
      });
      for(const eventHash of headHashes) {
        if(dupSet.has(eventHash)) {
          continue;
        }
        const currentKeyForNewHead = _cacheKey.event({eventHash, ledgerNodeId});
        const newHeadKey = _cacheKey.outstandingMergeEvent(
          {eventHash, ledgerNodeId});
        multi.rename(currentKeyForNewHead, newHeadKey);
        currentKeysForNewHeads.add(currentKeyForNewHead);
        newHeadKeys.add(newHeadKey);
        const headGenerationKey = _cacheKey.headGeneration(
          {eventHash, ledgerNodeId});
        // these keys are mainly useful during gossip about recent events
        // expire them after an hour
        multi.expire(headGenerationKey, 3600);
      }
      // add new childless heads
      if(newHeads.length !== 0) {
        multi.sadd(this.cacheKey.childless, newHeads);
      }
    }
    // remove head keys so they will not be removed from the cache
    if(newHeadKeys.size !== 0) {
      // head keys have already been renamed, don't attempt to delete again
      _.pull(rangeData, ...currentKeysForNewHeads);
      multi.sadd(this.cacheKey.outstandingMerge, [...newHeadKeys]);
    }
    // TODO: maybe just mark these to expired because they could be used
    // for gossip
    // delete all the regular events from the cache, it's possible that
    // rangeData is empty if the only other items were merge events
    if(rangeData.length !== 0) {
      multi.del(rangeData);
    }
    // remove events with new children that have not yet been merged locally
    if(parentHashes.length !== 0) {
      multi.srem(this.cacheKey.childless, parentHashes);
    }
    // execute update
    await multi.exec();

    // return creators of all new events
    return {creators: [...newHeadCreators]};
  }
};
