/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('../cache-key');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const database = require('bedrock-mongodb');
const logger = require('../logger');
const BedrockError = bedrock.util.BedrockError;
const ContinuityAgent = require('./continuity-agent');

module.exports = class EventWriter extends ContinuityAgent {
  constructor({agentName, immediate = false, ledgerNode}) {
    agentName = agentName || 'event-writer';
    super({agentName, ledgerNode});
    this.config = config['ledger-consensus-continuity'].writer;
    this.storage = ledgerNode.storage;
    this.client;
    const ledgerNodeId = ledgerNode.id;
    this.childlessKey = _cacheKey.childless(ledgerNodeId);
    this.eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
    this.eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
    this.outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
    // when in immediate mode, the writer clears the queue on each `start`
    this.immediate = immediate;
  }

  _clearQueue() {
    async.auto({
      range: callback => this.client.lrange(
        this.eventQueueKey, 0, -1, (err, result) => {
          if(err) {
            return callback(err);
          }
          if(!result || result.length === 0) {
            return callback(new BedrockError('Nothing to do.', 'AbortError'));
          }
          return callback(null, result);
        }),
      get: ['range', (results, callback) => this._getEvents(
        {cacheKeys: results.range}, callback)],
      process: ['get', (results, callback) =>
        this._processEvents({rawEvents: results.get}, callback)],
      store: ['process', (results, callback) => this._storeEvents({
        processData: results.process,
        rangeData: results.range,
      }, callback)]
    }, err => this._quit(err));
  }

  _getEvents({cacheKeys}, callback) {
    if(!cacheKeys || cacheKeys.length === 0) {
      return callback(new BedrockError('Nothing to do.', 'AbortError'));
    }
    this.client.mget(cacheKeys, (err, result) => {
      if(err) {
        logger.debug(`mget error: ${err}`);
        return callback(err);
      }
      // TODO: use fastest looping algo
      // filter out nulls, nulls occur when an event was a duplicate
      const eventsJson = result.filter(r => r !== null);
      if(eventsJson.length === 0) {
        return callback(new BedrockError('Nothing to do.', 'AbortError'));
      }
      // throw if there is a failure here because there is a serious problem
      const events = eventsJson.map(e => JSON.parse(e));
      callback(null, events);
    });
  }

  _onQuit() {
    this.client.quit();
  }

  _onStart() {
    this.client = new cache.Client().client;
  }

  _processEvents({rawEvents}, callback) {
    logger.debug('Starting prepare.');
    const now = Date.now();
    const events = [];
    const eventHashes = new Set();
    const _parentHashes = [];
    const headHashes = new Set();
    const creatorHeads = {};
    const operations = [];
    for(let i = 0; i < rawEvents.length; ++i) {
      const {event, meta} = rawEvents[i];
      const eventHash = database.hash(meta.eventHash);

      eventHashes.add(meta.eventHash);
      _.defaults(meta, {created: now, updated: now});
      events.push({event, eventHash, meta});

      if(event.type === 'WebLedgerOperationEvent') {
        for(let i = 0; i < event.operation.length; ++i) {
          const meta = {eventHash, eventOrder: i};
          operations.push({meta, operation: event.operation[i]});
        }
        delete event.operation;
      }

      // build a list of treeHashes that are not included in this batch
      if(meta.continuity2017.type === 'm') {
        const {creator: creatorId, generation} = meta.continuity2017;
        // capturing the *last* head for each creator
        creatorHeads[creatorId] = {eventHash, generation};
        headHashes.add(eventHash);
        _parentHashes.push(...event.parentHash);
      }
    }
    const parentHashes = _.uniq(_parentHashes);
    callback(null, {
      creatorHeads, eventHashes, events, headHashes, operations, parentHashes
    });
  }

  _storeEvents({processData, rangeData}, callback) {
    const {
      creatorHeads, events, eventHashes, headHashes, operations, parentHashes
    } = processData;
    async.auto({
      ops: callback => {
        if(operations.length === 0) {
          return callback();
        }
        this.storage.operations.addMany({operations}, callback);
      },
      store: callback => {
        logger.debug(
          `STORING RECORDS. EVENTSLENGTH ${events.length}`);
        // retry on duplicate events until all events have been processed
        this.storage.events.addMany({events}, callback);
      },
      cache: ['store', (results, callback) => {
        const ledgerNodeId = this.ledgerNode.id;
        const eventCountCacheKey = _cacheKey.eventCountPeer({
          ledgerNodeId,
          second: Math.round(Date.now() / 1000)
        });
        const multi = cache.client.multi();
        multi.incrby(eventCountCacheKey, events.length);
        multi.expire(eventCountCacheKey, 6000);
        // remove items from the list
        // ltrim *keeps* items from start to end
        // subtracting 1 from length to account for `triggerEvent` which
        // has already been removed from the list via blpop
        multi.ltrim(this.eventQueueKey, rangeData.length - 1, -1);
        multi.srem(this.eventQueueSetKey, Array.from(eventHashes));
        // update heads
        const creators = Object.keys(creatorHeads);
        // contains cache keys for all new heads
        const newHeadKeys = new Set();
        if(creators.length !== 0) {
          const {dupHashes} = results.store;
          const dupSet = new Set(dupHashes);
          // used to identify childless events
          const hashFilter = new Set(parentHashes.concat(dupHashes));
          const newHeads = [];
          // update the key that contains a hash of eventHash and generation
          creators.forEach(creatorId => {
            const headKey = _cacheKey.head({creatorId, ledgerNodeId});
            const {eventHash, generation} = creatorHeads[creatorId];
            multi.hmset(headKey, 'h', eventHash, 'g', generation);
            if(!hashFilter.has(eventHash)) {
              newHeads.push(eventHash);
            }
          });
          for(const eventHash of headHashes) {
            if(dupSet.has(eventHash)) {
              continue;
            }
            newHeadKeys.add(_cacheKey.event({eventHash, ledgerNodeId}));
            const headGenerationKey = _cacheKey.headGeneration(
              {eventHash, ledgerNodeId});
            // these keys are mainly useful during gossip about recent events
            // expire them after an hour
            multi.expire(headGenerationKey, 3600);
          }
          // add new childless heads
          if(newHeads.length !== 0) {
            multi.sadd(this.childlessKey, newHeads);
            // new heads, publish to inform ConsensusAgent and other listeners
            multi.publish('continuity2017.event', 'merge');
          }
        }
        // remove head keys so they will not be removed from the cache
        if(newHeadKeys.size !== 0) {
          _.pull(rangeData, ...newHeadKeys);
          multi.sadd(this.outstandingMergeKey, Array.from(newHeadKeys));
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
          multi.srem(this.childlessKey, parentHashes);
        }
        // excute update
        multi.exec(callback);
      }],
    }, callback);
  }

  _workLoop() {
    this.working = true;

    if(this.immediate) {
      return this._clearQueue();
    }

    // timeout every Ns to check for work stoppage
    this.client.blpop(this.eventQueueKey, 1, (err, pResult) => {
      if(err) {
        logger.debug(`Error in BLPOP, ${err}`);
        return this._quit(err);
      }
      if(this.halt || this.quitCalled) {
        if(pResult === null) {
          return this._quit();
        }
        // push the event back on the front of the queue for next cycle
        return this.client.lpush(this.eventQueueKey, pResult[1], err =>
          this._quit(err));
      }
      // start waiting for events again if stop is still false
      if(pResult === null) {
        return this._workLoop();
      }
      const triggerEvent = pResult[1];
      async.auto({
        debounce: callback => setTimeout(callback, this.config.debounce),
        // grab more events for writing
        range: ['debounce', (results, callback) => this.client.lrange(
          this.eventQueueKey, 0, this.config.maxEvents, (err, result) => {
            if(err) {
              return callback(err);
            }
            const queue = [triggerEvent];
            // lrange returns null on no data
            if(result) {
              queue.push(...result);
            }
            callback(null, queue);
          })],
        // get the original record, plus the results from range
        get: ['range', (results, callback) => this._getEvents(
          {cacheKeys: results.range}, callback)],
        process: ['get', (results, callback) =>
          this._processEvents({rawEvents: results.get}, callback)],
        store: ['process', (results, callback) => this._storeEvents({
          processData: results.process,
          rangeData: results.range,
        }, callback)]
      }, err => {
        this.working = false;
        if(err && err.name !== 'AbortError') {
          return this._quit(err);
        }
        // start waiting for another event
        if(this.halt) {
          return this._quit();
        }
        // start listening again
        this._workLoop();
      });
    });
  }
};
