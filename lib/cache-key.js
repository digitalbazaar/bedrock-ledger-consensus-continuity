/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const database = require('bedrock-mongodb');

const api = {};
module.exports = api;

api.childless = ledgerNodeId => `cl|${_lni(ledgerNodeId)}`;

api.event = ({eventHash, ledgerNodeId}) =>
  `e|${_lni(ledgerNodeId)}|${eventHash}`;

api.eventCountLocal = ({ledgerNodeId, second}) =>
  `ecl|${_lni(ledgerNodeId)}|${second}`;
api.eventCountPeer = ({ledgerNodeId, second}) =>
  `ecp|${_lni(ledgerNodeId)}|${second}`;

api.eventQueue = ledgerNodeId => `eq|${_lni(ledgerNodeId)}`;
api.eventQueueSet = ledgerNodeId => `eqs|${_lni(ledgerNodeId)}`;

api.genesis = (ledger) => `g|${ledger.substr(-36)}`;

// NOTE: this key contains the head that has been stored in mongo, this head
// can be used for $graphLookup operations
api.head = ({creatorId, ledgerNodeId}) =>
  `h|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.headGeneration = ({eventHash, ledgerNodeId}) =>
  `hg|${_lni(ledgerNodeId)}|${eventHash}`;

// hash portion of the voterId
api.gossipNotification = creatorId => `gn-${_ci(creatorId)}`;

api.lastPeerHeads = ledgerNodeId => `lph|${_lni(ledgerNodeId)}`;

api.ledgerNode = creatorId => `ln|${_ci(creatorId)}`;

api.manifest = ({ledgerNodeId, manifestId}) =>
  `m|${_lni(ledgerNodeId)}|${manifestId}`;

api.mergeDebounce = ledgerNodeId => `md|${_lni(ledgerNodeId)}`;

// contains a list of all non-consensus merge events, local and peer
api.outstandingMerge = ledgerNodeId => `om|${_lni(ledgerNodeId)}`;

// used with an expiration to track time since last peer contact
api.peerContact = ({creatorId, ledgerNodeId}) =>
  `pc|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.peerList = ledgerNodeId => `pl|${ledgerNodeId}`;

// NOTE: this key stores the very latest head for a peer, this head cannot
// be used for $graphLookup operations because the head may not yet be in mongo
api.latestPeerHead = ({ledgerNodeId, creatorId}) =>
  `latestph|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.event = ({eventHash, ledgerNodeId}) =>
  `e|${_lni(ledgerNodeId)}|${eventHash}`;

api.publicKey = ({ledgerNodeId, publicKeyId}) =>
  `pk|${_lni(ledgerNodeId)}|${_ci(publicKeyId)}`;

api.voter = ledgerNodeId => `v|${_lni(ledgerNodeId)}`;

function _ci(creatorId) {
  return database.hash(creatorId);
}

function _lni(ledgerNodeId) {
  return database.hash(ledgerNodeId);
}
