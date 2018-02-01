/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
// module API
const api = {};
module.exports = api;

api.keys = require('./storage-keys');
api.voters = require('./storage-voters');
