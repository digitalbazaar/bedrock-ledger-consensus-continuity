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

api.timer = ({name, ledgerNodeId}) => `t|${name}|${_lni(ledgerNodeId)}`;

const urnUuidReg = /([^\:]*)\:*$/;
const allHyphenReg = /-/g;
function _lni(ledgerNodeId) {
  // return the uuid portion with hypens removed
  return ledgerNodeId.match(urnUuidReg)[1].replace(allHyphenReg, '');
}
