/*
 * Storage for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
// module API
const api = {};
module.exports = api;

api.manifests = require('./manifestStorage');
api.votes = require('./voteStorage');
api.voters = require('./voterStorage');
