/*
 * Client for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const config = bedrock.config;

require('./config');

// module API
const api = {};
module.exports = api;

// define request pool for all gossip requests
const requestPool = {};
Object.defineProperty(requestPool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-continuity'].gossip.requestPool.maxSockets
});

// TODO: add method for getting block status
api.getBlockStatus = (blockHeight, peer, callback) => {
  // TODO: implement me, currently mocked
  const status = mockStatus.status[mockStatus.statusCount];
  mockStatus.statusCount = Math.min(
    mockStatus.statusCount + 1, mockStatus.status.length);
  callback(null, status);
};

// TODO: add method for getting gossip given block status
api.getManifest = (manifestHash, peer, callback) => {
  // TODO: get manifest (event hashes) from peer and store via manifestStorage
};

// TODO: add method for sending gossip given block status
api.sendEvents = (blockStatus, peer, callback) => {
  // TODO: get all non-consensus, non-deleted events from storage modulo the
  // ones with hashes in blockStatus.eventHash... and send them to the peer.
  // TODO: potentially send one at a time instead of in bulk
  callback(new Error('Not implemented'));
}

// TODO: add method for getting event
api.getEvent = (eventHash, peer, callback) => {
  // TODO: get event from peer and pass to consensus.add.events
  callback(new Error('Not implemented'));
};
