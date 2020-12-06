/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const _cache = require('./cache');
const {BedrockError} = bedrock.util;
const brLedgerNode = require('bedrock-ledger-node');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

/**
 * Get the witnesses for the given ledger node and block height.
 *
 * The witnesses will be returned using the given data structure:
 *
 * [{id: voter_id, sameAs: previous_voter_id}, ... ]
 *
 * @param ledgerNode the ledger node API to use.
 * @param blockHeight the height of the block.
 *
 * @return a Promise that resolves to an object with properties:
 *   witnesses the array of witnesses.
 */
api.getBlockWitnesses = async ({ledgerNode, blockHeight}) => {
  const ledgerNodeId = ledgerNode.id;

  let witnesses = await _cache.witnesses.getWitnesses(
    {blockHeight, ledgerNodeId});

  // return cached witnesses
  if(witnesses) {
    return {witnesses};
  }

  // witnesses not in cache, will need to be computed
  const [ledgerConfiguration, latestBlockSummary] = await Promise.all([
    _getLatestConfig(ledgerNode),
    // NOTE: getLatestSummary is required here because the summary is
    // passed into the witness selection API
    ledgerNode.storage.blocks.getLatestSummary(ledgerNode)
  ]);

  const {eventBlock} = latestBlockSummary;

  // FIXME: do we need to force this ... can we avoid this check?
  // ensure requested `blockHeight` matches next block
  const expectedBlockHeight = eventBlock.block.blockHeight + 1;
  if(expectedBlockHeight !== blockHeight) {
    throw new BedrockError(
      'Invalid `blockHeight` specified.', 'InvalidStateError', {
        blockHeight,
        expectedBlockHeight
      });
  }

  // use witness selection method plugin to get witnesses for block
  const witnessSelectionMethod = _getWitnessSelectionMethod(
    {ledgerConfiguration});
  if(witnessSelectionMethod.type !== 'electorSelection') {
    throw new BedrockError(
      'Witness selection method is invalid.', 'InvalidStateError');
  }
  const result = await witnessSelectionMethod.api.getBlockElectors({
    ledgerNode, ledgerConfiguration, latestBlockSummary, blockHeight
  });

  // only include `id` of witnesses, no extra data that may be set by
  // the selection algorithm
  witnesses = result.electors.map(({id}) => id);

  // validate that `witnesses` is either length `1` or forms a `3f+1` size set
  if(witnesses.length !== 1) {
    if(witnesses.length % 3 !== 1) {
      throw new BedrockError(
        'Witnesses do not form a set of size "3f+1".', 'InvalidStateError');
    }
  }

  // cache witnesses
  await _cache.witnesses.setWitnesses({blockHeight, witnesses, ledgerNodeId});

  return {witnesses};
};

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
