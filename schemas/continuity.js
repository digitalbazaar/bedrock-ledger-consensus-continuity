/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const config = require('bedrock').config;
const constants = config.constants;
const schemas = require('bedrock-validation').schemas;

const continuityMergeEvent = {
  title: 'ContinuityMergeEvent',
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

const webLedgerConfigEvent = {
  title: 'WebLedgerConfigurationEvent',
  // signature is not required
  required: ['@context', 'ledgerConfiguration', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
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
  title: 'WebLedgerOperationEvent',
  required: ['@context', 'operationHash', 'parentHash', 'treeHash', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
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
    webLedgerConfigEvent,
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
module.exports.webLedgerEvents = () => webLedgerEvents;
