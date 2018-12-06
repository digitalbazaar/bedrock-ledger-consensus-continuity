/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config} = bedrock;
const {constants} = config;
const {schemas} = require('bedrock-validation');

const continuityMergeEvent = {
  title: 'ContinuityMergeEvent',
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
      minItems: 1
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
continuityGenesisMergeEvent.title = 'Genesis ContinuityMergeEvent';
continuityGenesisMergeEvent.required = continuityMergeEvent.required.filter(
  p => p !== 'treeHash');
delete continuityGenesisMergeEvent.properties.treeHash;

const webLedgerConfigurationEvent = {
  title: 'Continuity WebLedgerConfigurationEvent',
  additionalProperties: false,
  // signature is not required
  required: ['@context', 'creator', 'ledgerConfiguration', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
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
        }
      },
    },
    signature: schemas.linkedDataSignature2018()
  }
};

const webLedgerOperationEvent = {
  title: 'Continuity WebLedgerOperationEvent',
  additionalProperties: false,
  required: ['@context', 'operationHash', 'parentHash', 'treeHash', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    basisBlockHeight: {
      type: 'integer',
      minimum: 0,
    },
    operationHash: {
      type: 'array',
      minItems: 1,
    },
    parentHash: {
      type: 'array',
      items: {type: 'string'},
      minItems: 1
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
  title: 'Continuity Event',
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
module.exports.webLedgerEvents = () => webLedgerEvents;
module.exports.webLedgerConfigurationEvent = () => webLedgerConfigurationEvent;
