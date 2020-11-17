/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _cache = require('../cache');
const _election = require('../election');
const _events = require('../events');
const _voters = require('../voters');
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
exports.extendBlockchain = async ({ledgerNode, savedState, halt}) => {
  const state = {};
  let recoveryMode = false;
  let consensusCount = 0;

  while(!halt()) {
    let result;
    await _updateState({ledgerNode, recoveryMode, state, savedState});
    try {
      // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
      result = await _findConsensus({ledgerNode, state});
    } catch(error) {
      if(error.name !== 'NewElectorsRequiredError') {
        logger.error('Error in _findConsensus', {error});
        throw error;
      }
      // TODO: remove `recoveryMode`
      // signal to _updateState that recoveryElectors should be used
      recoveryMode = true;
      continue;
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
      return result;
    }

    consensusCount++;
    logger.verbose(
      'Found consensus; consensus algorithm found consensus ' +
      `${consensusCount} consecutive time(s).`);

    // consensus was reached and could potentially be reached again, so clear
    // recovery mode flag and loop
    recoveryMode = false;
  }

  return {halted: true};
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
  const creator = await _voters.get({ledgerNodeId});
  const history = await _events.getRecentHistory({
    creatorId: creator.id,
    excludeLocalRegularEvents: true,
    ledgerNode,
  });

  // Note: DO NOT LOG RESULTS OF FINDCONSENSUS
  logger.verbose('Starting extendBlockchain.findConsensus.');
  const consensusResult = await _election.findConsensus({
    ledgerNode,
    history,
    blockHeight: state.blockHeight,
    electors: state.electors,
    recoveryElectors: state.recoveryElectors,
    recoveryGenerationThreshold: state.recoveryGenerationThreshold,
    mode: state.mode,
    state: state.state
  });
  logger.verbose('extendBlockchain.findConsensus complete.');

  if(!consensusResult.consensus) {
    // consensus not reached, set some merge permits to allow progress...
    // specifically, allow merge events from each priority peer and
    // up to 10 additional merge events from any node; but do not allow
    // more than 30 more events at a time to ensure there is backpressure
    const permits = Math.min(30, consensusResult.priorityPeers.length + 10);
    // return failed consensus info
    return {
      consensus: false,
      writeBlock: false,
      priorityPeers: consensusResult.priorityPeers,
      mergePermits: permits
    };
  }

  const writeBlock = await _blocks.write(
    {consensusResult, ledgerNode, state});
  return {consensus: consensusResult, writeBlock};
}

async function _updateState({ledgerNode, recoveryMode, state, savedState}) {
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
  const blockElectors = await _election.getBlockElectors(
    {blockHeight: nextBlockHeight, ledgerNode, recoveryMode});

  state.electors = blockElectors.electors;
  state.recoveryElectors = blockElectors.recoveryElectors;
  state.recoveryGenerationThreshold =
    blockElectors.recoveryGenerationThreshold;
  state.mode = 'first';
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
