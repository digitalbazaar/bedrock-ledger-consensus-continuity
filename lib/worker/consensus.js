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
const brCooldown = require('bedrock-cooldown');
const logger = require('../logger');
const {BedrockError} = bedrock.util;

/**
 * Continually attempts to achieve consensus and write new blocks until
 * consensus can't be reached because more merge events are needed.
 *
 * @param ledgerNode {Object} - ledger node instance.
 * @param halt {Function} - A function to use to see if work should halt.
 *
 * @returns {Promise} - Resolves once the operation completes.
 */
exports.extendBlockchain = async ({ledgerNode, halt}) => {
  const state = {};
  let recoveryMode = false;
  let consensusCount = 0;

  logger.verbose('Start extendBlockchain...');
  const start = Date.now();
  while(!halt()) {
    let result;
    logger.verbose('Start _updateState ...');
    const startUpdateState = Date.now();
    await _updateState({ledgerNode, recoveryMode, state});
    logger.verbose('End _updateState ...', {
      duration: Date.now() - startUpdateState
    });
    try {
      logger.verbose('Start _findConsensus ...');
      const startFindConsensus = Date.now();
      // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
      result = await _findConsensus({ledgerNode, state});
      logger.verbose('End _findConsensus ...', {
        duration: Date.now() - startFindConsensus
      });
    } catch(error) {
      logger.verbose('extendBlockchain contains an error...', error);
      if(error.name !== 'NewElectorsRequiredError') {
        logger.error('Error in _findConsensus', {error});
        throw error;
      }
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
      logger.verbose('Ledger configuration change detected.');
      throw new BedrockError(
        'Ledger configuration change detected.',
        'LedgerConfigurationChangeError', {
          blockHeight: result.writeBlock.blockHeight
        });
    }
    if(!result.consensus) {
      // no consensus reached -- return helpful info for reaching consensus
      logger.verbose('End extendBlockchain (no consensus reached)...', {
        duration: Date.now() - start
      });
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

  logger.verbose('End extendBlockchain (halted: true)...', {
    duration: Date.now() - start
  });
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
  logger.verbose('History stats in _findConsensus', {
    creator: creator.id,
    historyEventsLength: history.events.length
  });
  // Note: DO NOT LOG RESULTS OF FINDCONSENSUS
  logger.verbose('Starting extendBlockchain.findConsensus.', {
    blockHeight: state.blockHeight,
    electors: state.electors,
    recoveryElectors: state.recoveryElectors,
    recoveryGenerationThreshold: state.recoveryGenerationThreshold,
    mode: state.mode
  });
  const consensusResult = await _election.findConsensus({
    ledgerNode,
    history,
    blockHeight: state.blockHeight,
    electors: state.electors,
    recoveryElectors: state.recoveryElectors,
    recoveryGenerationThreshold: state.recoveryGenerationThreshold,
    mode: state.mode
  });
  logger.verbose('extendBlockchain.findConsensus complete.', {
    blockHeight: state.blockHeight,
    electors: state.electors,
    recoveryElectors: state.recoveryElectors,
    recoveryGenerationThreshold: state.recoveryGenerationThreshold,
    mode: state.mode
  });

  if(!consensusResult.consensus) {
    // consensus not reached, set some merge permits to allow progress...
    // specifically, allow merge events from each priority peer and
    // up to 20 additional merge events from any node; but do not allow
    // more than 30 more events at a time to ensure there is backpressure
    const alert = await brCooldown.get('ledger-consensus-continuity:catchup');
    const permits = !alert ?
      Math.min(30, consensusResult.priorityPeers.length + 20) :
      state.electors.length * alert.blocksBehind;
    logger.verbose('Catchup alert...', {
      alert,
      permits
    });
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

async function _updateState({ledgerNode, recoveryMode, state}) {
  const ledgerNodeId = ledgerNode.id;
  const {blockHeight: nextBlockHeight, previousBlockHash, previousBlockId} =
    await _blocks.getNextBlockInfo(ledgerNode);
  const cachedBlockHeight = await _cache.blocks.blockHeight(ledgerNodeId);
  const blockHeight = nextBlockHeight - 1;
  if(blockHeight !== cachedBlockHeight) {
    // this should never happen
    if((blockHeight - cachedBlockHeight) !== 1) {
      // FIXME: remove, *assumes* cache needs to be primed
      if(cachedBlockHeight === 0) {
        await _cache.prime.primeAll({ledgerNode});
      } else {
        throw new BedrockError(
          'Cache is behind by more than one block.', 'InvalidStateError',
          {blockHeight, cachedBlockHeight, ledgerNodeId});
      }
    } else {
      // inconsistency needs to be repaired
      logger.debug('BLOCKS REPAIRCACHE');
      await _blocks.repairCache({blockHeight, ledgerNode});
    }
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
}
