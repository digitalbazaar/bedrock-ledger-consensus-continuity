/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cacheKey');

const api = {};
module.exports = api;

// expose cache key API for testing
api.cacheKey = _cacheKey;

api.Timer = require('../Timer');

api.blocks = require('./blocks');
