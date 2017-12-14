/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const config = require('bedrock').config;
const constants = config.constants;
const schemas = require('bedrock-validation').schemas;

// FIXME: rudamentary implementation
const RFC6920 = {
  type: 'string',
  pattern: '^ni:\/\/\/'
};

const continuityMergeEvent = {
  title: 'ContinuityMergeEvent',
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
    signature: {
      anyOf: [schemas.linkedDataSignature()],
    },
    treeHash: {
      type: 'string'
    },
    type: {
      type: 'array',
      items: {
        anyOf: [
          {type: 'string', enum: ['WebLedgerEvent']},
          {type: 'string', enum: ['ContinuityMergeEvent']},
        ]
      },
      minItems: 2,
      uniqueItems: true
    },
  },
  required: ['@context', 'parentHash', 'signature', 'treeHash', 'type'],
};

const webLedgerConfigEvent = {
  title: 'WebLedgerConfigurationEvent',
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
    signature: {
      anyOf: [schemas.linkedDataSignature()],
    }
  }
};

const webLedgerEvent = {
  title: 'WebLedgerEvent',
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    input: {
      type: 'array',
      minItems: 1,
    },
    operation: {
      type: 'string',
    },
    parentHash: {
      type: 'array',
      items: RFC6920,
      minItems: 1
    },
    treeHash: RFC6920,
    type: {
      type: 'string',
      enum: ['WebLedgerEvent']
    },
  },
  required: ['@context', 'input', 'operation', 'parentHash', 'treeHash', 'type']
};

const webLedgerEvents = {
  title: 'Web Ledger Events',
  oneOf: [
    webLedgerEvent,
    webLedgerConfigEvent,
    continuityMergeEvent
  ]
};

const event = {
  type: 'object',
  title: 'Continuity Event',
  properties: {
    event: {
      oneOf: [webLedgerEvents]
    },
    eventHash: {
      type: 'string'
    }
  },
  required: ['event', 'eventHash']
};

module.exports.event = () => (event);
module.exports.webLedgerEvents = () => (webLedgerEvents);
