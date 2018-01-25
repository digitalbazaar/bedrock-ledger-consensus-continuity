/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const _events = require('./events');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const jsonld = bedrock.jsonld;
const logger = require('./logger');

module.exports = class EventWriter {
  constructor({ledgerNode}) {
    this.ledgerNode = ledgerNode;
    this.storage = ledgerNode.storage;
    this.client = new cache.Client().client;
    this.stop = false;
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
        if(this.stop) {
          this._quit();
          return;
        }
        this.start();
        return;
      }
      async.auto({
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
        get: callback => this.client.get(pResult[1], (err, result) => {
          if(!result) {
            // event was a duplicate and has been removed from the cache
            // nothing to do
            return callback();
          }
          callback(null, JSON.parse(result));
        }),
        // mutates results.get.meta
        generation: ['get', (results, callback) => {
          if(!(results.get && jsonld.hasValue(
            results.get.event, 'type', 'ContinuityMergeEvent'))) {
            return callback();
          }
          const creatorId = results.get.meta.continuity2017.creator;
          _events._getLocalBranchHead({
            creatorId, ledgerNode: this.ledgerNode
          }, (err, result) => {
            if(err) {
              return callback(err);
            }
            results.get.meta.continuity2017.generation = result.generation + 1;
            callback();
          });
        }],
        store: ['generation', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          const {event, meta} = results.get;
          this.storage.events.add(event, meta, err => {
            if(err && err.name !== 'DuplicateError') {
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
          this.client.del(pResult[1], callback);
        }],
        head: ['store', (results, callback) => {
          const eventCountCacheKey = _cacheKey.eventCount({
            ledgerNodeId,
            second: Math.round(Date.now() / 1000)
          });
          if(!(results.get && jsonld.hasValue(
            results.get.event, 'type', 'ContinuityMergeEvent'))) {
            // only update event counter
            return this.client.multi()
              .incr(eventCountCacheKey)
              .expire(eventCountCacheKey, 6000)
              .exec(callback);
          }
          const meta = results.get.meta;
          const creatorId = meta.continuity2017.creator;
          this.client.multi()
            .set(_cacheKey.head({creatorId, ledgerNodeId}), meta.eventHash)
            .incr(_cacheKey.headGeneration({creatorId, ledgerNodeId}))
            .incr(eventCountCacheKey)
            .expire(eventCountCacheKey, 6000)
            .exec(callback);
        }],
      }, err => {
        if(err) {
          throw err;
        }
        // start waiting for another event
        if(this.stop) {
          this._quit();
          return;
        }
        // start listening again
        this.start();
      });
    });
  }

  stop(callback) {
    console.log('=================================================');
    // this.onQuit = callback;
    // this.stop = true;
  }

  _quit() {
    console.log('QQQQQQQQQQQQQQQQQ');
    logger.debug('Stopping event writer.');
    // this.client.quit();
    this.onQuit();
  }
};
