/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _events = require('./events');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
const jsonld = bedrock.jsonld;
const logger = require('./logger');

module.exports = class EventWriter {
  constructor({ledgerNode}) {
    this.ledgerNode = ledgerNode;
    this.storage = ledgerNode.storage;
    this.client = new cache.Client().client;
    this.halt = false;
    this.onQuit = null;
  }

  start() {
    logger.debug('Starting event writer.');
    const ledgerNodeId = this.ledgerNode.id;
    // timeout every 5s to check for work stoppage
    this.client.blpop(_cacheKey.eventQueue(ledgerNodeId), 1, (err, pResult) => {
      if(err) {
        return logger.error('Error in event writer.', err);
      }
      // start waiting for events again if stop is still false
      if(pResult === null) {
        if(this.halt) {
          return this._quit();
        }
        return this.start();
      }
      async.auto({
        // grab more events for writing
        range: callback => this.client.lrange(
          _cacheKey.eventQueue(ledgerNodeId), 0, 250, callback),
        count: callback => {
          const cacheKey = _cacheKey.eventCount({
            ledgerNodeId,
            second: Math.round(Date.now() / 1000)
          });
          cache.client.multi()
            .incr(cacheKey)
            .expire(cacheKey, 6000)
            .exec(callback);
        },
        // get the original record, plus the results from range
        get: ['range', (results, callback) => this.client.mget(
          [pResult[1], ...results.range], (err, result) => {
            // filter out nulls
            const events = result.filter(r => r !== null);
            if(events.length === 0) {
              // event was a duplicate and has been removed from the cache
              // nothing to do
              return callback();
            }
            callback(null, events.map(e => JSON.parse(e)));
          })],
        // mutates results.get.meta
        generation: ['get', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          async.each(results.get, (e, callback) => {
            if(!jsonld.hasValue(e.event, 'type', 'ContinuityMergeEvent')) {
              return callback();
            }
            const creatorId = e.meta.continuity2017.creator;
            _events._getLocalBranchHead({
              creatorId, ledgerNode: this.ledgerNode
            }, (err, result) => {
              if(err) {
                return callback(err);
              }
              e.meta.continuity2017.generation = result.generation + 1;
              callback();
            });
          }, callback);
        }],
        store: ['generation', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          const now = Date.now();
          const records = results.get.map(e => {
            _.defaults(e.meta, {created: now, updated: now});
            return {event: e.event, eventHash: e.meta.eventHash, meta: e.meta};
          });
          // FIXME: ordered?
          this.storage.events.collection.insertMany(
            records, {continueOnError: true, ordered: false}, err => {
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
        // TODO: may be interesting possibilities if just setEx here
        delete: ['store', (results, callback) => {
          if(!results.get) {
            // nothing to do
            return callback();
          }
          // TODO: non-existent keys could be removed from results.range
          this.client.del([pResult[1], ...results.range], callback);
        }],
        head: ['store', (results, callback) => {
          async.each(results.get, (e, callback) => {
            const eventCountCacheKey = _cacheKey.eventCount({
              ledgerNodeId,
              second: Math.round(Date.now() / 1000)
            });
            if(!jsonld.hasValue(e.event, 'type', 'ContinuityMergeEvent')) {
              // only update event counter
              // TODO: use INCRBY to increment once by event count
              return this.client.multi()
                .incr(eventCountCacheKey)
                .expire(eventCountCacheKey, 6000)
                .exec(callback);
            }
            const meta = e.meta;
            const creatorId = meta.continuity2017.creator;
            this.client.multi()
              .set(_cacheKey.head({creatorId, ledgerNodeId}), meta.eventHash)
              .incr(_cacheKey.headGeneration({creatorId, ledgerNodeId}))
              .incr(eventCountCacheKey)
              .expire(eventCountCacheKey, 6000)
              .exec(callback);
          }, callback);
        }],
      }, err => {
        if(err) {
          throw err;
        }
        // start waiting for another event
        if(this.halt) {
          return this._quit();
        }
        // start listening again
        this.start();
      });
    });
  }

  stop(callback) {
    if(!(callback && typeof callback === 'function')) {
      throw new TypeError('`callback` is required.');
    }
    this.onQuit = callback;
    this.halt = true;
  }

  _quit() {
    logger.debug('Stopping event writer.');
    this.client.quit();
    this.onQuit();
  }
};
