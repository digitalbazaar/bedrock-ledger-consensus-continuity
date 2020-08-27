/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _continuityConstants = require('../continuityConstants');
const _events = require('../events');
const bedrock = require('bedrock');
const logger = require('../logger');
const {BedrockError} = bedrock.util;

exports.merge = async ({ledgerNode, creatorId, priorityPeers, halt}) => {
  const {mergeEvents: {maxEvents}} = _continuityConstants;
  const maxPeerEvents = Math.ceil(maxEvents / 2);

  try {
    // ensure cache and mongo are in sync
    await _validateCache({ledgerNode, creatorId});

    // get merge status
    const mergeStatus = await _events.getMergeStatus(
      {ledgerNode, creatorId, priorityPeers});
    const {
      mergeable, peerChildlessHashes, localChildlessHashes,
      hasOutstandingOperations
    } = mergeStatus;

    // can't merge yet; we'll run again when later to try again
    if(!mergeable) {
      return {merged: false, hasOutstandingOperations};
    }

    // up to 50% of parent spots are for peer merge events, one spot is
    // for `treeHash` and remaining spots will be filled with regular
    // events; creating them on demand here
    const peerEventCount = Math.min(
      maxPeerEvents, peerChildlessHashes.length);
    let maxRegularEvents = maxEvents - peerEventCount - 1 -
      localChildlessHashes.length;
    let regularEvents = localChildlessHashes.length;
    while(maxRegularEvents > 0 && !halt()) {
      const {hasMore} = await _events.create({ledgerNode});
      if(!hasMore) {
        break;
      }
      maxRegularEvents--;
      regularEvents++;
    }

    // if there are no regular events to merge and the creator is not a
    // priority peer, then don't merge
    if(regularEvents === 0 &&
      (priorityPeers.length > 0 && !priorityPeers.includes(creatorId))) {
      return {merged: false, hasOutstandingOperations};
    }

    // do the merge
    if(!halt()) {
      const record = await _events.merge({creatorId, ledgerNode, mergeStatus});
      return {merged: !!record, record, hasOutstandingOperations};
    }
  } catch(e) {
    logger.error(`Error during merge: ${ledgerNode.id}`, {error: e});
  }

  return {merged: false};
};

async function _validateCache({ledgerNode, creatorId}) {
  const [cacheHead, mongoHead] = await Promise.all([
    _events.getHead({creatorId, ledgerNode}),
    _events.getHead({creatorId, ledgerNode, useCache: false})
  ]);
  if(_.isEqual(cacheHead, mongoHead)) {
    // success
    return;
  }
  // this should never happen
  if((mongoHead.generation - cacheHead.generation) !== 1) {
    const ledgerNodeId = ledgerNode.id;
    throw new BedrockError(
      'Cache is behind by more than one merge event.',
      'InvalidStateError',
      {cacheHead, mongoHead, ledgerNodeId});
  }
  const {eventHash} = mongoHead;
  await _events.repairCache({eventHash, ledgerNode});
}
