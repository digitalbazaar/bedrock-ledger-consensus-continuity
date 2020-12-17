/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _cache = require('../cache');
const _consensus = require('../consensus');
const _history = require('../history');
const _peers = require('../peers');
const _witnesses = require('../witnesses');
const bedrock = require('bedrock');
const logger = require('../logger');
const {BedrockError} = bedrock.util;

/**
 * Continually attempts to achieve consensus and write new blocks until
 * consensus can't be reached because more merge events are needed.
 *
 * @param ledgerNode {Object} - The ledger node instance.
 * @param savedState {Object} - An object to use to store consensus state
 *   across runs of `extendBlockChain`.
 * @param halt {Function} - A function to use to see if work should halt.
 *
 * @returns {Promise} - Resolves once the operation completes.
 */
exports.extend = async ({ledgerNode, savedState, halt}) => {
  const state = {};
  let blocks = 0;
  let result = {
    consensus: false,
    writeBlock: false,
    priorityPeers: null,
    witnesses: null,
    blockHeight: -1
  };

  while(!halt()) {
    await _updateState({ledgerNode, state, savedState});
    try {
      // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
      result = await _findConsensus({ledgerNode, state});
    } catch(error) {
      logger.error('Error in _findConsensus', {error});
      throw error;
    }
    /* Note: `_findConsensus` checks for WebLedgerConfigurationEvents in the
    new block. If one exists, then further processing should stop. By
    throwing an error here, the work session will terminate and the next
    work session will use a new LedgerNode instance with the new
    ledger configuration. */
    if(result.writeBlock &&
      result.writeBlock.hasEffectiveConfigurationEvent) {
      throw new BedrockError(
        'Ledger configuration change detected.',
        'LedgerConfigurationChangeError', {
          blockHeight: result.writeBlock.blockHeight
        });
    }
    if(!result.consensus) {
      // no consensus reached -- return helpful info for reaching consensus
      return {...result, blocks};
    }

    blocks++;
    logger.verbose(
      'Found consensus; consensus algorithm found consensus ' +
      `${blocks} consecutive time(s).`);
  }

  return {...result, blocks};
};

/**
 * Gets the latest consensus block and returns the new proposed block height
 * for the ledger (i.e. the current `blockHeight + 1`) and the latest block
 * hash as what would become the next `previousBlockHash`.
 *
 * @param ledgerNode {Object} - ledger node instance.
 * @param state {Object} - state that governs the consensus process.
 *
 * @returns {Promise}
 */
async function _findConsensus({ledgerNode, state}) {
  const ledgerNodeId = ledgerNode.id;
  const creator = await _peers.get({ledgerNodeId});
  const history = await _history.getRecent({creatorId: creator.id, ledgerNode});

  // Note: DO NOT LOG RESULTS OF FIND CONSENSUS
  logger.verbose('Starting blockchain.extend consensus.find.');
  const consensusResult = await _consensus.find({
    ledgerNode,
    history,
    blockHeight: state.blockHeight,
    witnesses: state.witnesses,
    state: state.state
  });
  logger.verbose('extendBlockchain.findConsensus complete.');

  if(!consensusResult.consensus) {
    // consensus not yet reached, return info
    return {
      consensus: false,
      writeBlock: false,
      priorityPeers: consensusResult.priorityPeers,
      witnesses: state.witnesses,
      blockHeight: state.blockHeight
    };
  }

  const writeBlock = await _blocks.write(
    {consensusResult, ledgerNode, state});
  return {
    consensus: consensusResult,
    writeBlock,
    priorityPeers: null,
    witnesses: state.witnesses,
    blockHeight: state.blockHeight
  };
}

async function _updateState({ledgerNode, state, savedState}) {
  const ledgerNodeId = ledgerNode.id;
  const {blockHeight: nextBlockHeight, previousBlockHash, previousBlockId} =
    await _blocks.getNextBlockInfo(ledgerNode);
  const cachedBlockHeight = await _cache.blocks.blockHeight(ledgerNodeId);
  const blockHeight = nextBlockHeight - 1;
  if(blockHeight !== cachedBlockHeight) {
    // this should never happen
    if((blockHeight - cachedBlockHeight) !== 1) {
      throw new BedrockError(
        'Cache is behind by more than one block.', 'InvalidStateError',
        {blockHeight, cachedBlockHeight, ledgerNodeId});
    }
    // inconsistency needs to be repaired
    logger.debug('BLOCKS REPAIRCACHE');
    await _blocks.repairCache({blockHeight, ledgerNode});
  }
  const {witnesses} = await _witnesses.getBlockWitnesses(
    {blockHeight: nextBlockHeight, ledgerNode});

  state.witnesses = witnesses;
  state.blockHeight = nextBlockHeight;
  state.previousBlockHash = previousBlockHash;
  state.previousBlockId = previousBlockId;
  if(!state.state || state.state.blockHeight !== nextBlockHeight) {
    savedState.state = state.state = {
      init: false,
      eventMap: new Map(),
      blockHeight: -1,
      hashToMemo: new Map(),
      symbolToMemo: new Map(),
      supportCache: new Map()
    };
  }
}
