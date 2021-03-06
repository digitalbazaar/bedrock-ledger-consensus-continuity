/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const api = {};
module.exports = api;

// used to track the participants of a block
api.blockParticipants = ledgerNodeId => `bp|${_lni(ledgerNodeId)}`;

api.opCountLocal = ({ledgerNodeId, second}) =>
  `ocl|${_lni(ledgerNodeId)}|${second}`;
api.opCountPeer = ({ledgerNodeId, second}) =>
  `ocp|${_lni(ledgerNodeId)}|${second}`;

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

api.timer = ({name, ledgerNodeId}) => `t|${name}|${_lni(ledgerNodeId)}`;

const urnUuidReg = /([^\:]*)\:*$/;
const allHyphenReg = /-/g;
function _lni(ledgerNodeId) {
  // return the uuid portion with hypens removed
  return ledgerNodeId.match(urnUuidReg)[1].replace(allHyphenReg, '');
}
