/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('./cache');
const _util = require('./util');
const bedrock = require('bedrock');
const {BedrockError} = bedrock.util;
const {config} = bedrock;

const api = {};
module.exports = api;

// FIXME: consider removing participants related code

/**
 * Get all peers that participated in the block at the given `blockHeight`.
 *
 * @param blockHeight the block height to get the participants for.
 * @param ledgerNode the ledger node to check.
 *
 * @return a Promise that resolves to an object with:
 *           consensusProofPeers - peers that participated in consensus proving.
 *           mergeEventPeers - peers that participated with merge events.
 */
api.getParticipants = async ({blockHeight, ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  // special case genesis block -- only need to get mergeEventPeers, there is
  // only one and it is the same as the consensusProofPeers
  const {getMergeEventPeers} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  if(blockHeight === 0) {
    const mergeEventPeers = await getMergeEventPeers({blockHeight});
    const consensusProofPeers = mergeEventPeers.slice();
    return {consensusProofPeers, mergeEventPeers};
  }

  // any other block
  // return cached particants if available
  const cachedParticipants = await _cache.blocks.getParticipants(
    {blockHeight, ledgerNodeId});
  if(cachedParticipants) {
    return cachedParticipants;
  }
  // cache miss
  const {getConsensusProofPeers} = ledgerNode.storage.blocks
    .plugins['continuity-storage'];
  const [consensusProofPeers, mergeEventPeers] = await Promise.all([
    getConsensusProofPeers({blockHeight}),
    getMergeEventPeers({blockHeight})
  ]);

  // update the cache
  await _cache.blocks.setParticipants(
    {blockHeight, consensusProofPeers, ledgerNodeId, mergeEventPeers});

  return {consensusProofPeers, mergeEventPeers};
};

/**
 * Gets the latest consensus block and returns the new proposed block height
 * for the ledger (i.e. the current `blockHeight + 1`) and the latest block
 * hash as what would become the next `previousBlockHash`.
 *
 * @param ledgerNode the ledger node to get the latest block for.
 *
 * @return a Promise that resolves to {blockHeight, previousBlockHash}.
 */
api.getNextBlockInfo = async ({ledgerNode} = {}) => {
  // Note: This consensus method assumes that `blockHeight` will always exist
  // on the previous block because it cannot be used on a blockchain that
  // does not have that information. There has presently been no mechanism
  // devised for switching consensus methods between hashgraph-like blocks
  // and typical blockchains with block heights.
  const block = await ledgerNode.storage.blocks.getLatestSummary();
  const lastBlockHeight = _.get(block, 'eventBlock.block.blockHeight');
  if(lastBlockHeight === undefined) {
    throw new BedrockError(
      'blockHeight is missing from latest block.', 'NotFoundError',
      {block});
  }
  const previousBlockHash = _.get(block, 'eventBlock.meta.blockHash');
  const previousBlockId = _.get(block, 'eventBlock.block.id');
  const blockHeight = lastBlockHeight + 1;
  return {blockHeight, previousBlockHash, previousBlockId};
};

// TODO: document
// consensusResult = {
//   eventHash, ordering, mergeEventHash, witnesses, replayerSet
// }
api.write = async ({worker, consensusResult}) => {
  // FIXME: does this need to be checked each time? this could instead be
  // computed at work session startup and whenever a new block is created
  // that contained a config; eliminating the need to run this check for
  // every block
  const {ledgerNode} = worker;
  const {event: {ledgerConfiguration: ledgerConfig}} =
    await ledgerNode.storage.events.getLatestConfig();
  if(ledgerConfig.consensusMethod !== 'Continuity2017') {
    throw new BedrockError(
      'Consensus method must be "Continuity2017".',
      'InvalidStateError', {
        consensusMethod: ledgerConfig.consensusMethod
      });
  }

  const {
    // use next block height since we're writing that block now
    nextBlockHeight: blockHeight,
    previousBlockId: previousBlock,
    previousBlockHash
  } = worker.consensusState;

  // use `consensusResult.replayerSet` to update *all* merge events for *all*
  // entries in the `replayerSet` to have
  // `meta.continuity2017.replayDetectedBlockHeight=blockHeight`
  const {replayerSet} = consensusResult;
  if(replayerSet.size > 0) {
    // FIXME: this could be made parallel with `updateMany` below
    await _markNewReplayers(
      {ledgerNode, replayers: [...replayerSet], blockHeight});
  }

  // update events with bulk update operation and batches
  const {ordering} = consensusResult;
  let now = Date.now();
  const eventUpdates = [];
  for(const {eventHash, blockOrder} of ordering) {
    eventUpdates.push({
      eventHash,
      patch: [{
        op: 'set',
        changes: {
          meta: {
            blockHeight,
            blockOrder,
            consensus: true,
            consensusDate: now,
            updated: now
          }
        }
      }]
    });
  }

  // concurrently update any events with `requiredBlockHeight: -1` that
  // were created by a witness to use `requiredBlockHeight: blockHeight`
  const {witnesses} = consensusResult;
  await Promise.all([
    ledgerNode.storage.events.updateMany({events: eventUpdates}),
    _setRequiredBlockHeight(
      {ledgerNode, witnesses: [...witnesses], blockHeight})
  ]);

  // create new block ID
  const {ledger: ledgerId} = ledgerConfig;
  const blockId = _generateBlockId({blockHeight, ledgerId});

  const block = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    id: blockId,
    blockHeight,
    consensusMethod: 'Continuity2017',
    type: 'WebLedgerEventBlock',
    eventHash: consensusResult.eventHash,
    // FIXME: remove `consensusProofHash`
    consensusProofHash: consensusResult.consensusProofHash,
    previousBlock,
    previousBlockHash
  };
  const blockHash = await _util.hasher(block);

  // convert `eventHash` predicate to `event`
  // TODO: make internal storage use `eventHash` and convert it to
  //   `event` using consensus plugin
  block.event = block.eventHash;
  delete block.eventHash;

  now = Date.now();
  const blockRecord = {
    block,
    meta: {
      blockHash,
      consensus: true,
      consensusDate: now,
      continuity2017: {witness: [...witnesses]}
    }
  };

  await ledgerNode.storage.blocks.add(blockRecord);

  // if there is a configuration event in the block, ensure that the sequence
  // for the new configuration is correct and mark the configuration as valid
  const {setEffectiveConfiguration} = ledgerNode.storage.events
    .plugins['continuity-storage'];
  const {hasEffectiveConfigurationEvent} = await setEffectiveConfiguration({
    blockHeight,
    sequence: ledgerConfig.sequence + 1
  });

  return {blockRecord, blockHeight, hasEffectiveConfigurationEvent};
};

api.writeGenesis = async ({block, creator, ledgerNode}) => {
  const meta = {
    blockHash: await _util.hasher(block),
    consensus: true,
    consensusDate: Date.now(),
    continuity2017: {witness: [creator]}
  };
  await ledgerNode.storage.blocks.add({block, meta});
};

function _generateBlockId({blockHeight, ledgerId}) {
  return `${ledgerId}/blocks/${blockHeight}`;
}

// FIXME: move to bedrock-ledger-consensus-continuity-storage
async function _markNewReplayers({ledgerNode, replayers, blockHeight}) {
  // FIXME: this query could take a *long* time for replayers with many
  // events -- is that a legitimate concern -- can we improve this at all?

  // FIXME: make this a covered query
  const {collection} = ledgerNode.storage.events;
  const result = await collection.updateMany({
    'meta.continuity2017.creator': {$in: replayers}
  }, {
    $set: {'meta.continuity2017.replayDetectedBlockHeight': blockHeight}
  });
  return result;
}

/**
 * Sets the Required Block Height for a block.
 *
 * @param {object} options - Options to use.
 * @param {object} options.ledgerNode - A ledger node.
 * @param {Array<string>} options.witnesses - An array of witness ids.
 * @param {number} options.blockHeight - A blockHeight to set.
 *
 * @returns {Promise<object>} The result of the operation.
 */
// FIXME: move to bedrock-ledger-consensus-continuity-storage
async function _setRequiredBlockHeight({ledgerNode, witnesses, blockHeight}) {
  // FIXME: make this a covered query
  const {collection} = ledgerNode.storage.events;
  const result = await collection.updateMany({
    'meta.continuity2017.creator': {$in: witnesses},
    'meta.continuity2017.requiredBlockHeight': -1
  }, {
    $set: {'meta.continuity2017.requiredBlockHeight': blockHeight}
  });
  return result;
}
