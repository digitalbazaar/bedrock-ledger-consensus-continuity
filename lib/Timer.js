/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache/cacheKey');
const cache = require('bedrock-redis');

module.exports = class Timer {
  // start timer
  start({name, ledgerNodeId}) {
    if(!(name && typeof name === 'string')) {
      throw new Error('"name" must be a string.');
    }
    if(!(ledgerNodeId && typeof ledgerNodeId === 'string')) {
      throw new Error('"ledgerNodeId" must be a string.');
    }
    this.name = name;
    this.ledgerNodeId = ledgerNodeId;
    this.startTime = Date.now();
  }

  // stop timer, record result (ignore errors, for stats only), and
  // return duration for external use
  stop() {
    const {name, ledgerNodeId, startTime} = this;
    const duration = Date.now() - startTime;
    const key = _cacheKey.timer({name, ledgerNodeId});
    cache.client.set(key, duration).catch(() => {});
    return duration;
  }
};
