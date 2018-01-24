/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const bedrock = require('bedrock');
const async = require('async');
const cache = require('bedrock-redis');
const jsonld = bedrock.jsonld;

module.exports = class EventWriter {
  constructor({ledgerNode}) {
    this.ledgerNode = ledgerNode;
    this.storage = ledgerNode.storage;
    this.client = new cache.Client().client;
  }

  start() {
    const ledgerNodeId = this.ledgerNode.id;
    // TODO: maybe the timeout here is correlated with the session?
    this.client.blpop(
      _cacheKey.eventQueue(ledgerNodeId), 0, (err, eventRecord) => {
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
          get: callback => this.client.get(eventRecord[1], (err, result) => {
            if(!result) {
              // event was a duplicate and has been removed from the cache
              // nothing to do
              return callback();
            }
            callback(null, JSON.parse(result));
          }),
          store: ['get', (results, callback) => {
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
            this.client.del(eventRecord[1], callback);
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
          this.start();
        });
      });
  }
};
