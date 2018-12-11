/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const bedrock = require('bedrock');
const {callbackify} = require('bedrock').util;
const {config} = bedrock;
const {validate} = require('bedrock-validation');
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
  let event;
  const ledgerNodeId = ledgerNode.id;
  const {id: creatorId} = await _voters.get({ledgerNodeId});
  if(genesisBlock) {
    // the configuration event is the first event in `genesisBlock.event`
    // followed by the genesis merge event
    [event] = genesisBlock.event;
    const [ledgerConfigurationEvent, genesisMergeEvent] = genesisBlock.event;
    let result = validate(
      'continuity.webLedgerConfigurationEvent', ledgerConfigurationEvent);
    if(!result.valid) {
      throw result.error;
    }
    result = validate(
      'continuity.continuityGenesisMergeEvent', genesisMergeEvent);
    if(!result.valid) {
      throw result.error;
    }
  } else {
    event = {
      '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
      type: 'WebLedgerConfigurationEvent',
      creator: creatorId,
      ledgerConfiguration
    };
  }
  if(!genesis) {
    event.basisBlockHeight = basisBlockHeight;
    const head = await _events.getHead({creatorId, ledgerNode});
    event.parentHash = [head.eventHash];
    event.treeHash = head.eventHash;
  }
  const eventHash = await _util.hasher(event);
  const result = await _events.add(
    {event, eventHash, genesis, genesisBlock, ledgerNode});
  // genesis ledger configuration is not required in gossip
  if(!genesis) {
    await _cache.events.setEventGossip({...result, eventHash, ledgerNodeId});
  }
});
