/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _events = require('../events');
const _cacheKey = require('../cache-key');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
const jsonld = bedrock.jsonld;
const logger = require('../logger');
const BedrockError = bedrock.util.BedrockError;
const ContinuityAgent = require('./continuity-agent');

module.exports = class EventWriter extends ContinuityAgent {
  constructor({agentName, immediate = false, ledgerNode}) {
    agentName = agentName || 'event-writer';
    super({agentName, ledgerNode});
    this.storage = ledgerNode.storage;
    this.client;
    this.childlessKey = _cacheKey.childless(ledgerNode.id);
    this.eventQueueKey = _cacheKey.eventQueue(ledgerNode.id);
    this.eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNode.id);
    // when in immediate mode, the writer clears the queue on each `start`
    this.immediate = immediate;
  }

  _calculateGeneration({batchHeadTreeMap, events, headTreeMap}, callback) {
    logger.debug('Starting generation.');
    // treeHashes
    const eventHashes = Array.from(headTreeMap.values());
    _events.getHeads({
      eventHashes, ledgerNode: this.ledgerNode
    }, (err, treeHashGenerationMap) => {
      if(err) {
        return callback(err);
      }
      const headGenerationMap = new Map();
      const creatorHeads = {};

      // assign generations to the heads in this batch
      for(const [eventHash, treeHash] of headTreeMap) {
        const mergeEvent = _.find(events, e => e.eventHash === eventHash);
        const generation = treeHashGenerationMap.get(treeHash);
        const nextGeneration = generation + 1;
        mergeEvent.meta.continuity2017.generation = nextGeneration;
        const creatorId = mergeEvent.meta.continuity2017.creator;
        // capturing eventHash and nextGeneration for each head
        headGenerationMap.set(eventHash, nextGeneration);
        // capturing the *last* head for each creator
        creatorHeads[creatorId] = {eventHash, generation: nextGeneration};
      }
      logger.debug('headTreeMap complete.');
      // Map [eventHash, treeHash]
      for(const [eventHash, treeHash] of batchHeadTreeMap) {
        for(let i = 0; i < events.length; ++i) {
          // first find the treehash
          if(events[i].eventHash === treeHash) {
            let {generation} = events[i].meta.continuity2017;
            if(generation === undefined) {
              // this is the first ever event for this creator
              generation = 1;
              events[i].meta.continuity2017.generation = generation;
            }
            const nextGeneration = generation + 1;
            // now find the child which will come after parent in events
            const child = _.find(events, e =>
              e.eventHash === eventHash, i);
            child.meta.continuity2017.generation = nextGeneration;

            const creatorId = child.meta.continuity2017.creator;

            // capturing eventHash and nextGeneration for each head
            headGenerationMap.set(eventHash, nextGeneration);
            // capturing the *last* head for each creator
            creatorHeads[creatorId] = {
              eventHash, generation: nextGeneration
            };
          }
        }
      }
      logger.debug('Generation Complete');
      // FIXME: remove or formalize
      // events.forEach(e => {
      //   if(jsonld.hasValue(e.event, 'type', 'ContinuityMergeEvent')) {
      //     if(!e.meta.continuity2017.generation) {
      //       console.log('NOGENERATION', JSON.stringify(e, null, 2));
      //       logger.debug('NOGENERATION', {
      //         event: e
      //       });
      //       throw new Error('NOGENERATION');
      //     }
      //   }
      // });
      callback(null, {creatorHeads, headGenerationMap});
    });
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
      // mutates results.prepare.events
      generation: ['process', (results, callback) =>
        this._calculateGeneration(results.process, callback)],
      store: ['generation', (results, callback) => this._storeEvents({
        generationData: results.generation,
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
      // filter out nulls, nulls occur when an event was a duplicate
      const eventsJson = result.filter(r => r !== null);
      if(eventsJson.length === 0) {
        return callback(new BedrockError('Nothing to do.', 'AbortError'));
      }
      let events = null;
      try {
        events = eventsJson.map(e => JSON.parse(e));
      } catch(err) {
        return callback(err);
      }
      logger.debug('JSON.parse successful.', {
        eventsLength: events.length
      });
      if(!events) {
        return callback(new BedrockError('Nothing to do.', 'AbortError'));
      }
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
    const headTreeMap = new Map();
    const batchHeadTreeMap = new Map();
    const parentHashes = [];
    for(let i = 0; i < rawEvents.length; ++i) {
      const {event, meta} = rawEvents[i];
      const {eventHash} = meta;

      eventHashes.add(eventHash);
      _.defaults(meta, {created: now, updated: now});
      events.push({event, eventHash, meta});

      // build a list of treeHashes that are not included in this batch
      if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
        parentHashes.push(...event.parentHash);
        if(!eventHashes.has(event.treeHash)) {
          // no need to check heads that are in `events`
          headTreeMap.set(eventHash, event.treeHash);
        } else {
          batchHeadTreeMap.set(eventHash, event.treeHash);
        }
      }
    }
    callback(null, {
      eventHashes, events, headTreeMap, batchHeadTreeMap, parentHashes
    });
  }

  _storeEvents({generationData, processData, rangeData}, callback) {
    const {events, eventHashes, parentHashes} = processData;
    async.auto({
      store: callback => {
        logger.debug(
          `STORING RECORDS. EVENTSLENGTH ${events.length}`);
        // retry on duplicate events until all events have been processed
        const dupHashes = [];
        async.retry({
          times: Infinity, errorFilter: database.isDuplicateError
        }, callback => this.storage.events.collection.insertMany(
          events, {ordered: true}, err => {
            if(err) {
              if(database.isDuplicateError(err)) {
                // TODO: increment a dup counter here?
                // remove events up to the dup and retry
                dupHashes.push(events[err.index].eventHash);
                events.splice(0, err.index + 1);
                return callback(err);
              }
              return callback(err);
            }
            callback(null, {dupHashes});
          }), callback);
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
        // delete all the events from the cache
        multi.del(rangeData);
        multi.srem(this.eventQueueSetKey, eventHashes);
        multi.srem(this.childlessKey, parentHashes);
        // update heads
        if(generationData) {
          const {creatorHeads, headGenerationMap} = generationData;
          // update the key that contains a hash of eventHash and generation
          Object.keys(creatorHeads).forEach(creatorId => {
            const headKey = _cacheKey.head({creatorId, ledgerNodeId});
            const {eventHash, generation} = creatorHeads[creatorId];
            multi.hmset(headKey, 'h', eventHash, 'g', generation);
          });
          // update the key that contains the generation for a head
          const newHeads = [];
          const {dupHashes} = results.store;
          const hashFilter = new Set(parentHashes.concat(dupHashes));
          for(const [eventHash, generation] of headGenerationMap) {
            const headGenerationKey = _cacheKey.headGeneration(
              {eventHash, ledgerNodeId});
            // these keys are mainly useful during gossip about recent events
            // expire them after an hour
            multi.set(headGenerationKey, generation, 'EX', 3600);
            if(!hashFilter.has(eventHash)) {
              newHeads.push(eventHash);
            }
          }
          // add new childless heads
          if(newHeads.length !== 0) {
            multi.sadd(this.childlessKey, newHeads);
          }
          // new heads, publish to inform ConsensusAgent and other listeners
          multi.publish('continuity2017.event', 'write');
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
        debounce: callback => setTimeout(callback, 250),
        // grab more events for writing
        // FIXME: make the record count and debounce configurable
        range: ['debounce', (results, callback) => this.client.lrange(
          this.eventQueueKey, 0, 1000, (err, result) => {
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
        // mutates results.prepare.events
        generation: ['process', (results, callback) =>
          this._calculateGeneration(results.process, callback)],
        store: ['generation', (results, callback) => this._storeEvents({
          generationData: results.generation,
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
