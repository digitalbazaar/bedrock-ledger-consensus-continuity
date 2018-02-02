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
const BedrockError = bedrock.util.BedrockError;
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
        return this._quit(err);
      }
      if(this.halt || this.quitCalled) {
        if(pResult === null) {
          return this._quit();
        }
        // push the event back on the front of the queue for next cycle
        return this.client.lpush(this.eventQueueKey, pResult, err =>
          this._quit(err));
      }
      // start waiting for events again if stop is still false
      if(pResult === null) {
        return this._workLoop();
      }
      async.auto({
        // grab more events for writing
        // FIXME: make the record count configurable
        range: callback => this.client.lrange(
          this.eventQueueKey, 0, 1000, callback),
        // get the original record, plus the results from range
        get: ['range', (results, callback) => {
          const queue = [pResult[1]];
          // lrange returns null on no data
          if(results.range) {
            queue.concat(results.range);
          }
          this.client.mget(queue, (err, result) => {
            if(err) {
              return callback(err);
            }
            // filter out nulls
            const eventsJson = result.filter(r => r !== null);
            if(eventsJson.length === 0) {
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
            callback(null, events);
          });
        }],
        prepare: ['get', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          // FIXME: is setting all to same time going to work?
          const now = Date.now();
          const events = [];
          const eventHashes = [];
          const headTreeMap = new Map();
          for(let i = 0; i < results.get.length; ++i) {
            const event = results.get[i].event;
            const meta = results.get[i].meta;
            const eventHash = meta.eventHash;

            eventHashes.push(eventHash);
            _.defaults(meta, {created: now, updated: now});
            events.push({event, eventHash, meta});

            // build a list of treeHashes that are not included in this batch
            if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent') &&
              !eventHashes.includes(event.treeHash)) {
              // no need to check heads that are in `events`
              headTreeMap.set(eventHash, event.treeHash);
            }
          }
          callback(null, {eventHashes, events, headTreeMap});
        }],
        // mutates results.events
        generation: ['prepare', (results, callback) => {
          if(!results.prepare || results.prepare.headTreeMap.size === 0) {
            return callback();
          }
          // Map [eventHash, treeHash]
          const headTreeMap = results.prepare.headTreeMap;
          // treeHashes
          const eventHashes = Array.from(headTreeMap.values());
          _events.getHeads(
            {eventHashes, ledgerNode: this.ledgerNode}, (err, result) => {
              if(err) {
                return callback(err);
              }
              const headGenerationMap = new Map();
              const creatorHeads = {};
              const events = results.prepare.events;

              // assign generations to the merge events in this batch
              for(const [treeHash, generation] of result) {
                const mergeEvent = _.find(events, e =>
                  e.event.treeHash === treeHash &&
                  jsonld.hasValue(e.event, 'type', 'ContinuityMergeEvent'));
                const nextGeneration = generation + 1;
                mergeEvent.meta.continuity2017.generation = nextGeneration;
                const creatorId = mergeEvent.meta.continuity2017.creator;
                const eventHash = mergeEvent.eventHash;

                // generation has been set, remove from headTreeMap
                headTreeMap.delete(eventHash);

                // capturing eventHash and nextGeneration for each head
                headGenerationMap.set(eventHash, nextGeneration);
                // capturing the *last* head for each creator
                creatorHeads[creatorId] = {
                  eventHash, generation: nextGeneration
                };
              }
              // any items that remain in headTreeMap have ancestors in `events`
              for(const [eventHash, treeHash] of headTreeMap) {
                for(let i = 0; i < events.length; ++i) {
                  // first find the treehash
                  if(events[i].eventHash === treeHash) {
                    let generation = events[i].meta.continuity2017.generation;
                    if(generation === undefined) {
                      // this is the first ever event for this creator
                      generation = 1;
                      events[i].meta.continuity2017.generation = generation;
                    }
                    const nextGeneration = generation + 1;
                    // now find the child which will come after parent in events
                    const child = _.find(
                      events, e => e.eventHash === eventHash, i);
                    child.meta.continuity2017.generation = nextGeneration;

                    // TODO: can this/does this ever happen? Check may
                    // be unnecessary
                    if(!child) {
                      return callback(new BedrockError(
                        'Missing child event.', 'NotFoundError'), {eventHash});
                    }

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
              callback(null, {creatorHeads, headGenerationMap});
            });
        }],
        store: ['generation', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          this.storage.events.collection.insertMany(
            results.prepare.events, {ordered: true}, err => {
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
            delKeys.concat(results.range);
          }
          multi.del(delKeys);
          multi.srem(this.eventQueueSetKey, results.prepare.eventHashes);

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
