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
// const BedrockError = bedrock.util.BedrockError;
const ContinuityAgent = require('./continuity-agent');

module.exports = class EventWriter extends ContinuityAgent {
  constructor({agentName, ledgerNode}) {
    agentName = agentName || 'event-writer';
    super({agentName, ledgerNode});
    this.storage = ledgerNode.storage;
    this.client = new cache.Client().client;
    this.eventQueueKey = _cacheKey.eventQueue(ledgerNode.id);
    this.eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNode.id);
  }

  _beforeQuit() {
    this.client.quit();
  }

  _workLoop() {
    this.working = true;
    const ledgerNodeId = this.ledgerNode.id;
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
      async.auto({
        debounce: callback => setTimeout(callback, 250),
        // grab more events for writing
        // FIXME: make the record count configurable
        range: ['debounce', (results, callback) => this.client.lrange(
          this.eventQueueKey, 0, 1000, callback)],
        // get the original record, plus the results from range
        get: ['range', (results, callback) => {
          const queue = [pResult[1]];
          // lrange returns null on no data
          if(results.range) {
            queue.push(...results.range);
          }
          this.client.mget(queue, (err, result) => {
            if(err) {
              logger.debug(`mget error: ${err}`);
              return callback(err);
            }
            // filter out nulls, nulls occur when an event was a duplicate
            const eventsJson = result.filter(r => r !== null);
            if(eventsJson.length === 0) {
              logger.debug('eventsJson empty.');
              // event was a duplicate and has been removed from the cache
              // nothing to do
              return callback();
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
            callback(null, events);
          });
        }],
        prepare: ['get', (results, callback) => {
          logger.debug('Starting prepare.');
          if(!results.get) {
            return callback();
          }
          // FIXME: is setting all to same time going to work?
          const now = Date.now();
          const events = [];
          const eventHashes = new Set();
          const headTreeMap = new Map();
          const batchHeadTreeMap = new Map();
          for(let i = 0; i < results.get.length; ++i) {
            const {event, meta} = results.get[i];
            const {eventHash} = meta;

            eventHashes.add(eventHash);
            _.defaults(meta, {created: now, updated: now});
            events.push({event, eventHash, meta});

            // build a list of treeHashes that are not included in this batch
            if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
              if(!eventHashes.has(event.treeHash)) {
                // no need to check heads that are in `events`
                headTreeMap.set(eventHash, event.treeHash);
              } else {
                batchHeadTreeMap.set(eventHash, event.treeHash);
              }
            }
          }
          // logger.debug('Prepare complete', {
          //   batchHeadTreeMap: Array.from(batchHeadTreeMap),
          //   eventHashes: Array.from(eventHashes),
          //   headTreeMap: Array.from(headTreeMap),
          // });
          callback(null, {eventHashes, events, headTreeMap, batchHeadTreeMap});
        }],
        // mutates results.prepare.events
        generation: ['prepare', (results, callback) => {
          logger.debug('Starting generation.');
          if(!results.prepare) {
            return callback();
          }
          // Map [eventHash, treeHash]
          const {batchHeadTreeMap, headTreeMap} = results.prepare;
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
            const events = results.prepare.events;

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
            results.prepare.events.forEach(e => {
              if(jsonld.hasValue(e.event, 'type', 'ContinuityMergeEvent')) {
                if(!e.meta.continuity2017.generation) {
                  console.log('NOGENERATION', JSON.stringify(e, null, 2));
                  logger.debug('NOGENERATION', {
                    event: e
                  });
                  throw new Error('NOGENERATION');
                }
              }
            });
            callback(null, {creatorHeads, headGenerationMap});
          });
        }],
        store: ['generation', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          logger.debug(
            `STORING RECORDS. EVENTSLENGTH ${results.prepare.events.length}`);
          this.storage.events.collection.insertMany(
            results.prepare.events, {ordered: true}, err => {
              logger.debug(`MONGO WRITE COMPLETE, err: ${err}`);
              if(err) {
                if(database.isDuplicateError(err) || (err.writeErrors &&
                  err.writeErrors.every(e => e.code === 11000))) {
                  // TODO: increment a dup counter here?
                  return callback();
                }
                return callback(err);
              }
              callback();
            });
        }],
        cache: ['store', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          logger.debug('Start cache update after event write.');
          const eventCountCacheKey = _cacheKey.eventCountPeer({
            ledgerNodeId,
            second: Math.round(Date.now() / 1000)
          });

          const multi = cache.client.multi();
          multi.incrby(eventCountCacheKey, results.get.length);
          multi.expire(eventCountCacheKey, 6000);
          // delete all the events from the cache
          const delKeys = [];
          if(pResult[1]) {
            delKeys.push(pResult[1]);
          }
          if(results.range) {
            delKeys.push(...results.range);
            // remove items from the list
            // ltrim *keeps* items from start to end
            multi.ltrim(this.eventQueueKey, results.range.length, -1);
          }
          multi.del(delKeys);
          multi.srem(this.eventQueueSetKey, ...results.prepare.eventHashes);

          // update heads
          if(results.generation) {
            const {creatorHeads, headGenerationMap} = results.generation;
            // update the key that contains a hash of eventHash and generation
            Object.keys(creatorHeads).forEach(creatorId => {
              const headKey = _cacheKey.head({creatorId, ledgerNodeId});
              const {eventHash, generation} = creatorHeads[creatorId];
              multi.hmset(headKey, 'h', eventHash, 'g', generation);
            });
            // update the key that contains the generation for a head
            for(const [eventHash, generation] of headGenerationMap) {
              const headGenerationKey = _cacheKey.headGeneration(
                {eventHash, ledgerNodeId});
              // these keys are mainly useful during gossip about recent events
              // expire them after an hour
              multi.set(headGenerationKey, generation, 'EX', 3600);
            }

            // new heads, publish to inform ConsensusAgent and other listeners
            multi.publish('continuity2017.event', 'write');
          }
          // excute update
          multi.exec(callback);
        }],
      }, err => {
        this.working = false;
        if(err) {
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
