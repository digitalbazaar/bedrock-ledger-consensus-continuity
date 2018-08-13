/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {callbackify} = require('bedrock').util;
const {config} = bedrock;
const _events = require('./events');
const _util = require('./util');
const _voters = require('./voters');

const api = {};
module.exports = api;

/**
 * Adds a new event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 * @param options the options to use.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.change = callbackify(async (
  {genesis, genesisBlock, ledgerConfiguration, ledgerNode}) => {
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerConfigurationEvent',
    ledgerConfiguration
  };
  const eventHash = await _util.hasher(event);
  await _events.add({event, eventHash, genesis, genesisBlock, ledgerNode});
});
