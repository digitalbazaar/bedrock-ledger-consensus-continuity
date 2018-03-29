/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const _events = require('./events');
const _util = require('./util');

const api = {};
module.exports = api;

/**
 * Adds a new event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 * @param options the options to use.
 * @param callback(err, record) called once the operation completes.
 */
api.change = (
  {genesis, genesisBlock, ledgerConfiguration, ledgerNode}, callback) => {
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerConfigurationEvent',
    ledgerConfiguration
  };
  async.auto({
    eventHash: callback => _util.hasher(event, callback),
    add: ['eventHash', (results, callback) => {
      const {eventHash} = results;
      _events.add(
        {event, eventHash, genesis, genesisBlock, ledgerNode}, callback);
    }]
  }, err => callback(err));
};
