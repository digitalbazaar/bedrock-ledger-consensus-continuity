/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _cacheKey = require('../cache/cacheKey');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('../logger');

// PeerEventWriter writes peer events, in bulk, to the database
module.exports = class PeerEventWriter {
  constructor({ledgerNode, peerChildlessMap = new Map()}) {
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.config = config['ledger-consensus-continuity'].writer;
    this.peerChildlessMap = peerChildlessMap;
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.eventMap = new Map();
    this.operationMap = new Map();
    this.storage = ledgerNode.storage;
    this.cacheKey = {
      childless: _cacheKey.childless(this.ledgerNodeId),
      outstandingMerge: _cacheKey.outstandingMerge(this.ledgerNodeId),
    };
  }

  /**
   * Adds an event received from a peer to be written to persistent storage
   * when this writer is flushed.
   *
   * @param event {Object} - The event to cache.
   * @param meta {Object} - The meta data for the event.
   *
   * @returns {Promise} resolves once the operation completes.
   */
  async add({event, meta} = {}) {
    const {ledgerNodeId, operationMap, eventMap} = this;

    // FIXME: consider removing or revising this entirely, we may not need
    // to put anything in redis or it may just be stats (so it should become
    // _cache.stats in that case)... note that if this is not added to redis,
    // then any recently received and validated events will not be available
    // to other peers via gossip until they are written to the database; we
    // might want a model where we push things out to the redis cache here
    // and any event readers will check redis first for events before hitting
    // the database ... and then putting the results into an in-memory LRU
    // cache
    await _cache.events.addPeerEvent({event, meta, ledgerNodeId});

    // shallow copy data to enable modification
    const record = {
      event: {...event},
      meta: {...meta}
    };

    // add operation records to in-memory operation map
    if(record.event.type === 'WebLedgerOperationEvent') {
      for(const opRecord of record.event.operationRecords) {
        operationMap.set(opRecord.meta.operationHash, opRecord);
      }
      delete record.event.operationRecords;
    }

    // add to in-memory event map
    eventMap.set(meta.eventHash, {event, meta});
  }

  async flush() {
    const {eventMap, operationMap} = this;
    if(eventMap.size === 0) {
      // nothing to flush
      return {creators: []};
    }

    let creators = [];
    try {

      // build unique operations and events to be written
      const operations = [...operationMap.values()];
      const events = [...eventMap.values()];
      const now = Date.now();
      for(const {meta} of events) {
        meta.created = meta.updated = now;
      }
      operationMap.clear();
      eventMap.clear();

      // write operations
      if(operations.length !== 0) {
        logger.debug(`Attempting to store ${operations.length} operations.`);
        await this.storage.operations.addMany({operations});
      }
      // write events
      logger.debug(`Attempting to store ${events.length} events.`);
      // this API will automatically retry on duplicate events until all events
      // have been processed
      const {dupHashes} = await this.storage.events.addMany({events});

      // FIXME: old here
      // update heads for all written events
      ({creators} = await this._updateHeads({events, dupHashes}));
    } catch(e) {
      logger.error(`Error in event writer: ${this.ledgerNodeId}`, {error: e});
      throw e;
    }
    return {creators};
  }

  async _updateHeads({events, dupHashes}) {
    const _parentHashes = [];
    const headHashes = new Set();
    const creatorHeads = {};
    for(const {event, meta} of events) {
      const {eventHash} = meta;

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
    const parentHashes = [...new Set(_parentHashes)];

    const {peerChildlessMap, ledgerNodeId} = this;
    const newHeadCreators = new Set();
    const multi = cache.client.multi();
    // update heads
    const creators = Object.keys(creatorHeads);
    // contains the current cache keys for all new heads
    const currentKeysForNewHeads = new Set();
    // contains new cache keys for all new heads
    const newHeadKeys = new Set();
    if(creators.length !== 0) {
      const dupSet = new Set(dupHashes);
      // used to identify childless events
      const hashFilter = new Set(parentHashes.concat(dupHashes));
      const newHeads = [];
      // update the key that contains a hash of eventHash and generation
      for(const creatorId of creators) {
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
          peerChildlessMap.set(eventHash, {
            eventHash,
            generation,
            basisBlockHeight,
            mergeHeight,
            localAncestorGeneration
          });
        }
      }
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
      }
      // add new childless heads
      if(newHeads.length !== 0) {
        multi.sadd(this.cacheKey.childless, newHeads);
      }
    }
    // remove head keys so they will not be removed from the cache
    if(newHeadKeys.size !== 0) {
      // head keys have already been renamed, don't attempt to delete again
      //_.pull(rangeData, ...currentKeysForNewHeads);
      multi.sadd(this.cacheKey.outstandingMerge, [...newHeadKeys]);
    }
    // remove events with new children that have not yet been merged locally
    if(parentHashes.length !== 0) {
      multi.srem(this.cacheKey.childless, parentHashes);
      for(const parentHash of parentHashes) {
        peerChildlessMap.delete(parentHash);
      }
    }
    // execute update
    await multi.exec();

    // return creators of all new events
    return {creators: [...newHeadCreators]};
  }
};
