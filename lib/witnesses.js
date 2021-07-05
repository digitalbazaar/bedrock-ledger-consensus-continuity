/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {BedrockError} = bedrock.util;
const brLedgerNode = require('bedrock-ledger-node');
const {default: {LruMemoize}} = require('@digitalbazaar/lru-memoize');

// load config defaults
require('./config');

let WITNESS_CACHE;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock.init', () => {
  // FIXME: make size configurable
  // create LRU cache for witnesses given a ledger node + block height
  WITNESS_CACHE = new LruMemoize({max: 1000});
});

/**
 * Get the witnesses for the given ledger node and block height.
 *
 * The witnesses will be returned using the given data structure:
 *
 * [{id: peerId}, ... ]
 *
 * @param ledgerNode the ledger node API to use.
 * @param blockHeight the height of the block.
 *
 * @return a Promise that resolves to an object with properties:
 *   witnesses the array of witnesses.
 */
api.getBlockWitnesses = async ({ledgerNode, blockHeight}) => {
  return WITNESS_CACHE.memoize({
    key: `${ledgerNode.id}|${blockHeight}`,
    fn: async () => _getWitnesses({ledgerNode, blockHeight})
  });
};

/**
 * Gets all witnesses for events after a certain block height.
 *
 * @private
 * @param {object} options - Options to use.
 * @param {object} options.ledgerNode - a Ledger Node.
 * @param {number|string} options.blockHeight - A block height.
 *
 * @returns {Promise<object.<symbol, Set<string>>} An object with a witnesses
 *   property that is a set of ids.
 */
async function _getWitnesses({ledgerNode, blockHeight}) {
  // get config and latest summary
  // FIXME: remove passing latest block summary to block electors API and
  // let the implementation of the API fetch it if it needs it
  const [ledgerConfiguration, latestBlockSummary] = await Promise.all([
    _getLatestConfig(ledgerNode),
    // NOTE: getLatestSummary is required here because the summary is
    // passed into the witness selection API
    ledgerNode.storage.blocks.getLatestSummary(ledgerNode)
  ]);

  // use witness selection method plugin to get witnesses for block
  const witnessSelectionMethod = _getWitnessSelectionMethod(
    {ledgerConfiguration});
  if(witnessSelectionMethod.type !== 'witnessSelection') {
    throw new BedrockError(
      'Witness selection method is invalid.', 'InvalidStateError');
  }
  const result = await witnessSelectionMethod.api.getBlockWitnesses({
    ledgerNode, ledgerConfiguration, latestBlockSummary, blockHeight
  });
  const witnesses = new Set(result.witnesses);
  // validate that `witnesses` is either size `1` or forms a `3f+1` size set
  if(!(witnesses.size === 1 || witnesses.size % 3 === 1)) {
    throw new BedrockError(
      'Witnesses do not form a set of size "3f+1".', 'InvalidStateError');
  }

  return {witnesses};
}

async function _getLatestConfig(ledgerNode) {
  const result = await ledgerNode.storage.events.getLatestConfig();
  // `getLatestConfig` returns an empty object before genesis block is written
  if(Object.keys(result).length === 0) {
    return {};
  }
  const config = result.event.ledgerConfiguration;
  if(config.consensusMethod !== 'Continuity2017') {
    throw new BedrockError(
      'Consensus method must be "Continuity2017".', 'InvalidStateError', {
        consensusMethod: config.consensusMethod
      });
  }
  return config;
}

function _getWitnessSelectionMethod({ledgerConfiguration}) {
  const {electorSelectionMethod} = ledgerConfiguration;
  if(!electorSelectionMethod) {
    throw new TypeError(
      '"ledgerConfiguration.electorSelectionMethod" is required.');
  }
  return brLedgerNode.use(electorSelectionMethod.type);
}
