/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const api = {};
module.exports = api;

// Note: The limits here need to ensure that we will not go over the mongodb
// document size limit of 16 MiB and that we generally won't create data that
// is too large to work with, leading to potential vulnerabilities.

api.events = {
  // the maximum number of operations assigned to an event
  maxOperations: 250,
};

api.mergeEvents = {
  // the maximum number of events to merge into a single merge event
  maxParents: 16,
  // the maximum number of non-merge parents to merge into a single merge event
  maxNonMergeParents: 10
};

api.operations = {
  // 256K maximum operation size
  maxBytes: 256 * 1024,
};
