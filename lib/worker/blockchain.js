/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _consensus = require('../consensus');
const bedrock = require('bedrock');
const logger = require('../logger');
const {BedrockError} = bedrock.util;

/**
 * Continually attempts to achieve consensus and write new blocks until
 * consensus can't be reached because more merge events are needed.
 *
 * @param worker {Object} - The worker extending the blockchain.
 *
 * @returns {Promise} - Resolves once the operation completes.
 */
exports.extend = async ({worker}) => {
  let blocks = 0;
  let result = {
    consensus: false,
    writeBlock: false,
    priorityPeers: null,
    witnesses: null,
    blockHeight: -1
  };

  while(!worker.halt()) {
    try {
      // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
      result = await _findConsensus({worker});
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
 * @param worker {Object} - The worker extending the blockchain.
 *
 * @returns {Promise}
 */
async function _findConsensus({worker}) {
  const {consensusState, ledgerNode} = worker;
  const history = worker.getRecentHistory();

  // Note: DO NOT LOG RESULTS OF FIND CONSENSUS
  logger.verbose('Starting blockchain.extend consensus.find.');
  // get block height to use for consensus based on the "next" block height
  // for the block to be added next to the blockchain
  const {nextBlockHeight: blockHeight, continuityState: state, witnesses} =
    consensusState;
  const consensusResult = await _consensus.find(
    {ledgerNode, history, blockHeight, witnesses, state});
  logger.verbose('extendBlockchain.findConsensus complete.');

  if(!consensusResult.consensus) {
    // consensus not yet reached, return info
    return {
      consensus: false,
      writeBlock: false,
      priorityPeers: consensusResult.priorityPeers,
      witnesses,
      blockHeight
    };
  }

  // write next block (which will also update worker state)
  const writeBlock = await _blocks.write({worker, consensusResult});
  return {
    consensus: consensusResult,
    writeBlock,
    priorityPeers: null,
    witnesses,
    blockHeight
  };
}
