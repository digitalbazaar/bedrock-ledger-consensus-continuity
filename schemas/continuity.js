/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const config = require('bedrock').config;
const constants = config.constants;
const schemas = require('bedrock-validation').schemas;

// FIXME: rudamentary implementation
const RFC6920 = {
  type: 'string',
  pattern: '^ni:\/\/\/'
};

// TODO: add to `bedrock-validation` schemas
const linkedDataProof = {
  title: 'Linked Data Signature',
  description: 'A Linked Data digital signature.',
  type: 'object',
  properties: {
    id: schemas.identifier(),
    type: {
      title: 'Linked Data Signature Type',
      type: 'string',
      enum: ['Ed25519Signature2018']
    },
    creator: schemas.identifier(),
    created: schemas.w3cDateTime(),
    jws: {
      title: 'Digital Signature Value',
      description: 'The Base64 encoding of the result of the signature ' +
        'algorithm.',
      type: 'string'
    },
  },
  // NOTE: id is not required
  required: ['type', 'creator', 'created', 'jws']
};
const linkedDataProofSchema = {
  title: 'Linked Data Proofs',
  anyOf: [{
    type: 'array',
    items: linkedDataProof,
    minItems: 1,
  }, linkedDataProof]
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
    proof: {
      anyOf: [linkedDataProofSchema]
    },
    treeHash: {
      type: 'string'
    },
    type: {
      type: 'string',
      enum: ['ContinuityMergeEvent']
    },
  },
  required: ['@context', 'parentHash', 'proof', 'treeHash', 'type'],
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

const webLedgerOperationEvent = {
  title: 'WebLedgerOperationEvent',
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    operation: {
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
  },
  required: ['@context', 'operation', 'parentHash', 'treeHash', 'type']
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
  type: 'object',
  title: 'Continuity Event',
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
  required: ['callerId', 'event', 'eventHash', 'mergeHash']
};

module.exports.event = () => (event);
module.exports.webLedgerEvents = () => (webLedgerEvents);
