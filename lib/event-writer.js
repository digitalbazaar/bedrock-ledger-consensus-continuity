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
    // TODO: maybe the timeout here is correlated with the session?
    this.client.blpop(
      _cacheKey.eventQueue(this.ledgerNode.id), 0, (err, eventRecord) => {
        async.auto({
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
            if(!(results.get && jsonld.hasValue(
              results.get.event, 'type', 'ContinuityMergeEvent'))) {
              return callback();
            }
            const meta = results.get.meta;
            const creatorId = meta.continuity2017.creator;
            this.client.set(_cacheKey.head({
              creatorId, ledgerNodeId: this.ledgerNode.id
            }), meta.eventHash, callback);
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
