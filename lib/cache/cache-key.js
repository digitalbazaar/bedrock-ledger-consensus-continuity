/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

// NOTE: these functions are used to define the namespace for keys that are
// used to store values in the cache. The goal is to use key names that are
// as compact as possible while still ensuring uniqueness in order to conserve
// cache memory.

const ledgerNodeIds = new Map();
const creatorIds = new Map();

const api = {};
module.exports = api;

// used to track latest blockHeight
api.blockHeight = ledgerNodeId => `bh|${_lni(ledgerNodeId)}`;

// used to track the participants of a block
api.blockParticipants = ledgerNodeId => `bp|${_lni(ledgerNodeId)}`;

api.childless = ledgerNodeId => `cl|${_lni(ledgerNodeId)}`;

api.diff = uuid => `d|${uuid}`;

api.electors = ledgerNodeId => `el|${_lni(ledgerNodeId)}`;

api.event = ({eventHash, ledgerNodeId}) =>
  `e|${_lni(ledgerNodeId)}|${eventHash}`;

api.eventCountLocal = ({ledgerNodeId, second}) =>
  `ecl|${_lni(ledgerNodeId)}|${second}`;
api.eventCountPeer = ({ledgerNodeId, second}) =>
  `ecp|${_lni(ledgerNodeId)}|${second}`;

// a stringified version of events used by gossip
api.eventGossip = ({eventHash, ledgerNodeId}) =>
  `eg|${_lni(ledgerNodeId)}|${eventHash}`;

api.eventQueue = ledgerNodeId => `eq|${_lni(ledgerNodeId)}`;
api.eventQueueSet = ledgerNodeId => `eqs|${_lni(ledgerNodeId)}`;

api.genesis = ledgerNodeId => `g|${_lni(ledgerNodeId)}`;

// NOTE: this key contains the head that has been stored in mongo, this head
// can be used for $graphLookup operations
api.head = ({creatorId, ledgerNodeId}) =>
  `h|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.headGeneration = ({eventHash, ledgerNodeId}) =>
  `hg|${_lni(ledgerNodeId)}|${eventHash}`;

// hash portion of the voterId
api.gossipNotification = ledgerNodeId => `gn|${_lni(ledgerNodeId)}`;

// used to determine if a gossip session has occurred since notification sent
api.gossipNotifyFlag = ledgerNodeId => `gnf|${_lni(ledgerNodeId)}`;

// hash map that stores meta data about gossip peers
api.gossipPeerStatus = ({creatorId, ledgerNodeId}) =>
  `gps|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.lastPeerHeads = ledgerNodeId => `lph|${_lni(ledgerNodeId)}`;

api.ledgerNode = creatorId => `ln|${_ci(creatorId)}`;

// childless local regular events
api.localChildless = ledgerNodeId => `lc|${_lni(ledgerNodeId)}`;

// contains a list of all non-consensus merge events, local and peer
api.outstandingMerge = ledgerNodeId => `om|${_lni(ledgerNodeId)}`;
api.outstandingMergeEvent = ({eventHash, ledgerNodeId}) =>
  `ome|${_lni(ledgerNodeId)}|${eventHash}`;

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

// NOTE: these two APIs are linked
// the basisBlockHeightFromOperationKey API below corresponds to this key
api.operation = ({basisBlockHeight, ledgerNodeId, operationHash}) =>
  `o|${_lni(ledgerNodeId)}|${basisBlockHeight}|${operationHash}`;
// extract the basisBlockHeight from a key value
api.basisBlockHeightFromOperationKey = key => parseInt(key.split('|')[2]);
api.operationHashFromOperationKey = key => key.split('|')[3];

// this key is used to track the existence of an operation in the queue
api.operationHash = ({ledgerNodeId, operationHash}) =>
  `oh|${_lni(ledgerNodeId)}|${operationHash}`;

// set of operation hashes before they are written to events
api.operationList = ledgerNodeId => `ol|${_lni(ledgerNodeId)}`;

// set of operation hashes used to record operations selected for an event
api.operationSelectedList = ledgerNodeId => `osl|${_lni(ledgerNodeId)}`;

api.voter = ledgerNodeId => `v|${_lni(ledgerNodeId)}`;

api.timer = ({name, ledgerNodeId}) => `t|${name}|${_lni(ledgerNodeId)}`;

const lastPathReg = /([^\/]*)\/*$/;
function _ci(creatorId) {
  let ci = creatorIds.get(creatorId);
  if(ci) {
    return ci;
  }
  // return the last part of the URL which is the public key
  ci = creatorId.match(lastPathReg)[1];
  creatorIds.set(creatorId, ci);
  return ci;
}

const urnUuidReg = /([^\:]*)\:*$/;
const allHyphenReg = /-/g;
function _lni(ledgerNodeId) {
  let lni = ledgerNodeIds.get(ledgerNodeId);
  if(lni) {
    return lni;
  }
  // return the uuid portion with hypens removed
  lni = ledgerNodeId.match(urnUuidReg)[1].replace(allHyphenReg, '');
  ledgerNodeIds.set(ledgerNodeId, lni);
  return lni;
}
