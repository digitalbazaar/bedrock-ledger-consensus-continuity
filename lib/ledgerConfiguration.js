/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
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
api.change = callbackify(async ({
  basisBlockHeight, genesis, genesisBlock, ledgerConfiguration, ledgerNode
}) => {
  const ledgerNodeId = ledgerNode.id;
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerConfigurationEvent',
    ledgerConfiguration
  };
  if(!genesis) {
    const {id: creatorId} = await _voters.get({ledgerNodeId});
    event.basisBlockHeight = basisBlockHeight;
    const head = await _events.getHead({creatorId, ledgerNode});
    event.parentHash = [head.eventHash];
    event.treeHash = head.eventHash;
  }
  const eventHash = await _util.hasher(event);
  await Promise.all([
    // add the event to cache for gossip purposes
    _cache.events.setEventGossip({event, eventHash, ledgerNodeId}),
    _events.add({event, eventHash, genesis, genesisBlock, ledgerNode}),
  ]);
});
