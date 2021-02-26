/*!
 * Copyright (c) 2018-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {config, util: {BedrockError}} = require('bedrock');
const {validate} = require('bedrock-validation');
const _events = require('./events');
const _localPeers = require('./localPeers');
const _util = require('./util');

const api = {};
module.exports = api;

/**
 * Adds a new ledger configuration event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 * @param options the options to use.
 *
 * @return a Promise that resolves once the operation completes.
 */
api.change = async ({
  basisBlockHeight, genesis, genesisBlock, ledgerConfiguration, ledgerNode,
  worker
}) => {
  // non-genesis events must be added from within a work session; the
  // presence of `worker` implies this function is being called from within
  // a work session or by a unit test; adding non-genesis events outside of
  // a work session could cause database corruption and invalidation of the
  // peer on the network because a concurrently running work session assumes
  // it is the only process adding events
  // FIXME: getting `worker` here is a hack to allow tests to pass, this
  // function must be refactored to only schedule a config change and the
  // tests must be refactored to account for that
  worker = worker || ledgerNode.worker;
  if(!genesis && !worker) {
    throw new Error(
      '"worker" is required to add non-genesis events. Non-genesis events ' +
      'must not be added outside of a work session.');
  }

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
    const localPeerId = await _localPeers.getPeerId({ledgerNodeId});
    const {head} = worker;
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
    if(creator !== localPeerId) {
      throw new BedrockError(
        `Invalid configuration 'creator' value.`, 'SyntaxError', {
          creator,
          expectedCreator: localPeerId,
          ledgerConfiguration,
          httpStatusCode: 400,
          public: true,
        });
    }
  }
  const eventHash = await _util.hasher(event);
  await _events.add(
    {event, eventHash, genesis, genesisBlock, ledgerNode, worker});
};
