/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const logger = require('../logger');

exports.primeAll = async ({ledgerNode}) => {
  logger.debug('Priming the cache...', {ledgerNodeId: ledgerNode.id});
  await exports.primeBlockHeight({ledgerNode});
  logger.debug(
    'Successfully primed block height.', {ledgerNodeId: ledgerNode.id});
  logger.debug('Successfully primed the cache.', {ledgerNodeId: ledgerNode.id});
};

exports.primeBlockHeight = async ({ledgerNode}) => {
  const ledgerNodeId = ledgerNode.id;
  const blockHeight = await ledgerNode.blocks.getLatestBlockHeight();
  await _blocks.setBlockHeight({blockHeight, ledgerNodeId});
};
