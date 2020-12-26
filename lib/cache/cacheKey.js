/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const api = {};
module.exports = api;

// used to track the participants of a block
api.blockParticipants = ledgerNodeId => `bp|${_lni(ledgerNodeId)}`;

api.witnesses = ledgerNodeId => `w|${_lni(ledgerNodeId)}`;

api.genesis = ledgerNodeId => `g|${_lni(ledgerNodeId)}`;

// hash portion of the voterId
api.gossipNotification = ledgerNodeId => `gn|${_lni(ledgerNodeId)}`;

// used to determine if a gossip session has occurred since notification sent
api.gossipNotifyFlag = ledgerNodeId => `gnf|${_lni(ledgerNodeId)}`;

// hash map that stores meta data about gossip peers
api.gossipPeerStatus = ({peerId, ledgerNodeId}) =>
  `gps|${_lni(ledgerNodeId)}|${_ci(peerId)}`;

api.ledgerNode = creatorId => `ln|${_ci(creatorId)}`;

api.opCountLocal = ({ledgerNodeId, second}) =>
  `ocl|${_lni(ledgerNodeId)}|${second}`;
api.opCountPeer = ({ledgerNodeId, second}) =>
  `ocp|${_lni(ledgerNodeId)}|${second}`;

// used with an expiration to track time since last peer contact
api.peerContact = ({creatorId, ledgerNodeId}) =>
  `pc|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.peerList = ledgerNodeId => `pl|${ledgerNodeId}`;

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

// NOTE: these functions are used to define the namespace for keys that are
// used to store values in the cache. The goal is to use key names that are
// as compact as possible while still ensuring uniqueness in order to conserve
// cache memory.
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
