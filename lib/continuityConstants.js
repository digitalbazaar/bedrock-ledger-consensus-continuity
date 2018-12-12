/*!
 * Continuity2017 non-configurable constants
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const api = {};
module.exports = api;

api.events = {
  // the maximum number of operations assigned to an event
  maxOperations: 250,
};

api.mergeEvents = {
// the maximum number of events to merge into a single merge event
  maxEvents: 16,
};

api.operations = {
  // 256K maximum operation size
  maxBytes: 256 * 1024,
};
