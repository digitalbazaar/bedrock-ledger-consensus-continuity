/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
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
    console.log('SSSSSSSSSSSSSSSSSSSSS');
    this.client.blpop(_cacheKey.eventQueue(ledgerNodeId), 30, (err, pResult) => {
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
        store: ['get', (results, callback) => {
          if(!results.get) {
            return callback();
          }
          // FIXME: is setting all to same time going to work?
          const now = Date.now();
          const records = results.get.map(e => {
            _.defaults(e.meta, {created: now, updated: now});
            return {
              event: e.event,
              eventHash: e.meta.eventHash,
              meta: e.meta
            };
          });
          // FIXME: ordered?
          this.storage.events.collection.insertMany(
            records, {ordered: true, writeConcern: {level: 'majority'}},
            err => {
              if(err) {
                if(database.isDuplicateError(err) || (err.writeErrors &&
                  err.writeErrors.every(e => e.code === 11000))) {
                  // TODO: increment a dup counter here?
                  throw new Error('DUPLICATE');
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
          // build update for all the new heads that were just stored
          const headUpdate = [];
          for(let i = 0; i < results.get.length; ++i) {
            if(!jsonld.hasValue(
              results.get[i].event, 'type', 'ContinuityMergeEvent')) {
              continue;
            }
            const meta = results.get[i].meta;
            const creatorId = meta.continuity2017.creator;
            const headKey = _cacheKey.head({creatorId, ledgerNodeId});
            const headGenerationKey = _cacheKey.headGeneration(
              {creatorId, ledgerNodeId});
            headUpdate.push(
              headKey, meta.eventHash,
              headGenerationKey, meta.continuity2017.generation);
          }
          const eventCountCacheKey = _cacheKey.eventCount({
            ledgerNodeId,
            second: Math.round(Date.now() / 1000)
          });

          const multi = cache.client.multi();
          multi.incrby(eventCountCacheKey, results.get.length);
          multi.expire(eventCountCacheKey, 6000);
          // delete all the events from the cache
          multi.del([pResult[1], ...results.range]);
          if(headUpdate.length !== 0) {
            multi.mset(headUpdate);
          }
          multi.exec(callback);
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
