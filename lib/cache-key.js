/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const api = {};
module.exports = api;

api.event = ({eventHash, ledgerNodeId}) =>
  `e|${_lni(ledgerNodeId)}|${_eh(eventHash)}`;

api.eventCountLocal = ({ledgerNodeId, second}) =>
  `ecl|${_lni(ledgerNodeId)}|${second}`;
api.eventCountPeer = ({ledgerNodeId, second}) =>
  `ecp|${_lni(ledgerNodeId)}|${second}`;

api.eventQueue = ledgerNodeId => `eq|${_lni(ledgerNodeId)}`;
api.eventQueueSet = ledgerNodeId => `eqs|${_lni(ledgerNodeId)}`;

api.genesis = (ledger) => `g|${ledger.substr(-36)}`;

api.head = ({creatorId, ledgerNodeId}) =>
  `h|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.headGeneration = ({eventHash, ledgerNodeId}) =>
  `hg|${_lni(ledgerNodeId)}|${_eh(eventHash)}`;

// hash portion of the voterId
api.gossipNotification = creatorId => `gn-${_ci(creatorId)}`;

api.lastPeerHeads = ledgerNodeId => `lph|${ledgerNodeId}`;

api.ledgerNode = creatorId => `ln|${_ci(creatorId)}`;

api.lockHead = ({creatorId, ledgerNodeId}) =>
  `lh|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

api.manifest = ({ledgerNodeId, manifestId}) =>
  `m|${_lni(ledgerNodeId)}|${manifestId}`;

api.peerList = ledgerNodeId => `pl|${ledgerNodeId}`;

api.publicKey = ({ledgerNodeId, publicKeyId}) =>
  `pk|${_lni(ledgerNodeId)}|${_ci(publicKeyId)}`;

api.voter = ledgerNodeId => `v|${_lni(ledgerNodeId)}`;

function _eh(eventHash) {
  return eventHash.substr(-43);
}

function _ci(creatorId) {
  return creatorId.substr(-43);
}

function _lni(ledgerNodeId) {
  return ledgerNodeId.substr(-36);
}
