/*!
 * Copyright (c) 2018-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const {config, util: {callbackify, BedrockError}} = require('bedrock');
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
  if(genesisBlock) {
    // the configuration event is the first event in `genesisBlock.event`
    // followed by the genesis merge event
    [event] = genesisBlock.event;
    const [ledgerConfigurationEvent, genesisMergeEvent] = genesisBlock.event;
    let result = validate(
      'continuity.genesisConfigurationEvent', ledgerConfigurationEvent);
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
      ledgerConfiguration
    };
  }
  if(genesis) {
    // the genesis configuration requires a specific validator
    const validateResult = validate(
      'continuity.genesisLedgerConfiguration', ledgerConfiguration);
    if(!validateResult.valid) {
      throw validateResult.error;
    }
  } else {
    event.basisBlockHeight = basisBlockHeight;
    const {id: creatorId} = await _voters.get({ledgerNodeId});
    const head = await _events.getHead({creatorId, ledgerNode});
    event.parentHash = [head.eventHash];
    event.treeHash = head.eventHash;
    // validate the entire event to ensure consistency and also validate
    // the new ledger configuration
    const validateResult = validate(
      'continuity.webLedgerConfigurationEvent', event);
    if(!validateResult.valid) {
      throw validateResult.error;
    }
    const {event: {ledgerConfiguration: {
      ledger: expectedLedger,
      sequence: lastSequence
    }}} = await ledgerNode.storage.events.getLatestConfig();
    const expectedSequence = lastSequence + 1;
    const {creator, ledger, sequence} = ledgerConfiguration;
    if(ledger !== expectedLedger) {
      throw new BedrockError(
        `Invalid configuration 'ledger' value.`, 'SyntaxError', {
          expectedLedger,
          ledger,
          ledgerConfiguration,
          httpStatusCode: 400,
          public: true,
        });
    }
    if(sequence !== expectedSequence) {
      throw new BedrockError(
        `Invalid configuration 'sequence' value.`, 'SyntaxError', {
          expectedSequence,
          ledgerConfiguration,
          sequence,
          httpStatusCode: 400,
          public: true,
        });
    }
    if(creator !== creatorId) {
      throw new BedrockError(
        `Invalid configuration 'creator' value.`, 'SyntaxError', {
          creator,
          expectedCreator: creatorId,
          ledgerConfiguration,
          httpStatusCode: 400,
          public: true,
        });
    }
  }
  const eventHash = await _util.hasher(event);
  const result = await _events.add(
    {event, eventHash, genesis, genesisBlock, ledgerNode});
  // genesis ledger configuration is not required in gossip
  if(!genesis) {
    await _cache.events.setEventGossip({...result, eventHash, ledgerNodeId});
  }
});
