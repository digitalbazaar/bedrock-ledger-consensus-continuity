/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

// NOTE: these functions are used to define the namespace for keys that are
// used to store values in the cache. The goal is to use key names that are
// as compact as possible while still ensuring uniqueness in order to conserve
// cache memory.

// FIXME: this should be a class that computes
// at least some of these values only once if possible

const api = {};
module.exports = api;

// used to track latest blockHeight
api.blockHeight = ledgerNodeId => `bh|${_lni(ledgerNodeId)}`;

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

api.gossipBehind = ledgerNodeId => `gb|${_lni(ledgerNodeId)}`;

// hash portion of the voterId
api.gossipNotification = creatorId => `gn|${_ci(creatorId)}`;

api.lastPeerHeads = ledgerNodeId => `lph|${_lni(ledgerNodeId)}`;

api.ledgerNode = creatorId => `ln|${_ci(creatorId)}`;

api.manifest = ({ledgerNodeId, manifestId}) =>
  `m|${_lni(ledgerNodeId)}|${manifestId}`;

api.mergeDebounce = ledgerNodeId => `md|${_lni(ledgerNodeId)}`;

// contains a list of all non-consensus merge events, local and peer
api.outstandingMerge = ledgerNodeId => `om|${_lni(ledgerNodeId)}`;

api.opCountLocal = ({ledgerNodeId, second}) =>
  `ocl|${_lni(ledgerNodeId)}|${second}`;
api.opCountPeer = ({ledgerNodeId, second}) =>
  `ocp|${_lni(ledgerNodeId)}|${second}`;

// used with an expiration to track time since last peer contact
api.peerContact = ({creatorId, ledgerNodeId}) =>
  `pc|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.peerList = ledgerNodeId => `pl|${ledgerNodeId}`;

// NOTE: this key stores the very latest head for a peer, this head cannot
// be used for $graphLookup operations because the head may not yet be in mongo
api.latestPeerHead = ({ledgerNodeId, creatorId}) =>
  `latestph|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.operation = ({ledgerNodeId, opId}) => `o|${_lni(ledgerNodeId)}|${opId}`;

// set of operation hashes before they are written to events
api.operationList = ledgerNodeId => `ol|${_lni(ledgerNodeId)}`;

// set of operation hashes used to record operations selected for an event
api.operationSelectedList = ledgerNodeId => `osl|${_lni(ledgerNodeId)}`;

api.publicKey = ({ledgerNodeId, publicKeyId}) =>
  `pk|${_lni(ledgerNodeId)}|${_ci(publicKeyId)}`;

api.voter = ledgerNodeId => `v|${_lni(ledgerNodeId)}`;

const lastPathReg = /([^\/]*)\/*$/;
function _ci(creatorId) {
  // return the last part of the URL which is the public key
  return creatorId.match(lastPathReg)[1];
}

const urnUuidReg = /([^\:]*)\:*$/;
const allHyphenReg = /-/g;
function _lni(ledgerNodeId) {
  // return the uuid portion with hypens removed
  return ledgerNodeId.match(urnUuidReg)[1].replace(allHyphenReg, '');
}
