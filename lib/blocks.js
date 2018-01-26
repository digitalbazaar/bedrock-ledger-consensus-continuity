/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');

const api = {};
module.exports = api;

api.getLatest = (ledgerNode, callback) => {
  async.auto({
    latest: callback => ledgerNode.storage.blocks.getLatest(callback),
    // mutates block
    expand: ['latest', (results, callback) => _expandConsensusProofEvents(
      {block: results.latest.eventBlock.block, ledgerNode}, callback)]
  }, (err, results) => err ? callback(err) : callback(null, results.latest));
};

function _expandConsensusProofEvents({block, ledgerNode}, callback) {
  if(block.consensusProof) {
    // block already has `consensusProof` set
    return callback();
  }

  // find all events that must be fetched
  const events = block.consensusProofHash || [];
  const eventsToFetch = {};
  for(let i = 0; i < events.length; ++i) {
    const event = events[i];
    // TODO: Determine if we want to expand other hash types
    const isEventHash = typeof(event) === 'string' &&
      event.startsWith('ni:///');
    if(isEventHash) {
      eventsToFetch[event] = i;
    }
  }
  const hashes = Object.keys(eventsToFetch);
  if(hashes.length === 0) {
    // no event hashes to fetch
    return callback();
  }

  // get all event hashes from event collection
  const query = {
    eventHash: {
      $in: hashes
    }
  };
  const projection = {
    event: 1,
    'meta.eventHash': 1,
    _id: 0
  };
  block.consensusProof = [];
  ledgerNode.storage.events.collection.find(query, projection).forEach(e => {
    block.consensusProof[eventsToFetch[e.meta.eventHash]] = e.event;
  }, err => {
    if(err) {
      return callback(err);
    }
    delete block.consensusProofHash;
    callback();
  });
}
