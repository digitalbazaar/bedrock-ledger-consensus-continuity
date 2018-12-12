/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config} = bedrock;
const {constants} = config;
const _continuityConstants = require('../lib/continuityConstants');
const {schemas} = require('bedrock-validation');

const continuityMergeEvent = {
  title: 'Continuity2017 ContinuityMergeEvent',
  additionalProperties: false,
  required: ['@context', 'parentHash', 'proof', 'treeHash', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    parentHash: {
      type: 'array',
      items: {
        type: 'string'
      },
      minItems: 2,
      maxItems: _continuityConstants.mergeEvents.maxEvents
    },
    proof: schemas.linkedDataSignature2018(),
    treeHash: {
      type: 'string'
    },
    type: {
      type: 'string',
      enum: ['ContinuityMergeEvent']
    },
  }
};

// the genesis merge event does not include `treeHash`
const continuityGenesisMergeEvent = bedrock.util.clone(continuityMergeEvent);
continuityGenesisMergeEvent.title =
  'Continuity2017 Genesis ContinuityMergeEvent';
continuityGenesisMergeEvent.required = continuityMergeEvent.required.filter(
  p => p !== 'treeHash');
continuityGenesisMergeEvent.properties.parentHash.minItems = 1;
continuityGenesisMergeEvent.properties.parentHash.maxItems = 1;
delete continuityGenesisMergeEvent.properties.treeHash;

const webLedgerConfigurationEvent = {
  title: 'Continuity2017 WebLedgerConfigurationEvent',
  additionalProperties: false,
  // signature is not required
  required: ['@context', 'basisBlockHeight', 'creator', 'ledgerConfiguration',
    'parentHash', 'treeHash', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    basisBlockHeight: {
      type: 'integer',
      minimum: 0,
    },
    creator: {
      type: 'string'
    },
    type: {
      type: 'string',
      enum: ['WebLedgerConfigurationEvent']
    },
    ledgerConfiguration: {
      type: 'object',
      properties: {
        type: {
          type: 'string',
          enum: ['WebLedgerConfiguration']
        },
        ledger: {
          type: 'string',
        },
        consensusMethod: {
          type: 'string',
        },
        proof: schemas.linkedDataSignature2018(),
      },
    },
    parentHash: {
      type: 'array',
      items: {
        type: 'string'
      },
      minItems: 1,
      maxItems: 1
    },
    signature: schemas.linkedDataSignature2018(),
    treeHash: {
      type: 'string'
    },
  }
};

const genesisConfigurationEvent = bedrock.util.clone(
  webLedgerConfigurationEvent);
genesisConfigurationEvent.required = genesisConfigurationEvent.required.filter(
  p => !['basisBlockHeight', 'parentHash', 'treeHash'].includes(p));
delete genesisConfigurationEvent.properties.parentHash;
delete genesisConfigurationEvent.properties.treeHash;

const webLedgerOperationEvent = {
  title: 'Continuity2017 WebLedgerOperationEvent',
  additionalProperties: false,
  required: ['@context', 'operation', 'parentHash', 'treeHash', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    basisBlockHeight: {
      type: 'integer',
      minimum: 0,
    },
    operation: {
      type: 'array',
      minItems: 1,
      maxItems: _continuityConstants.events.maxOperations,
    },
    parentHash: {
      type: 'array',
      items: {type: 'string'},
      minItems: 1,
      maxItems: 1
    },
    treeHash: {type: 'string'},
    type: {
      type: 'string',
      enum: ['WebLedgerOperationEvent']
    },
  }
};

const webLedgerEvents = {
  title: 'Web Ledger Events',
  oneOf: [
    webLedgerOperationEvent,
    webLedgerConfigurationEvent,
    continuityMergeEvent
  ]
};

const event = {
  title: 'Continuity2017 Event',
  required: ['callerId', 'event', 'eventHash', 'mergeHash'],
  type: 'object',
  properties: {
    callerId: {
      type: 'string'
    },
    event: {
      oneOf: [webLedgerEvents]
    },
    eventHash: {
      type: 'string'
    },
    mergeHash: {
      type: 'string'
    },
  },
};

module.exports.event = () => event;
module.exports.continuityGenesisMergeEvent = () => continuityGenesisMergeEvent;
module.exports.genesisConfigurationEvent = () => genesisConfigurationEvent;
module.exports.webLedgerEvents = () => webLedgerEvents;
module.exports.webLedgerConfigurationEvent = () => webLedgerConfigurationEvent;
