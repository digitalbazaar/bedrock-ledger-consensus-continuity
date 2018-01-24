/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const api = {};
module.exports = api;

api.event = ({eventHash, ledgerNodeId}) =>
  `e|${_lni(ledgerNodeId)}${_eh(eventHash)}`;

api.eventQueue = ledgerNodeId => `eq|${_lni(ledgerNodeId)}`;

api.head = ({creatorId, ledgerNodeId}) =>
  `h|${_lni(ledgerNodeId)}|${_ci(creatorId)}`;

function _eh(eventHash) {
  return eventHash.substr(-43);
}

function _ci(creatorId) {
  return creatorId.substr(-43);
}

function _lni(ledgerNodeId) {
  return ledgerNodeId.substr(-36);
}
