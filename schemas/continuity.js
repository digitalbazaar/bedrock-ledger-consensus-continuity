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
      minItems: 2
      // maxItems: TODO: set maxItems
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
continuityGenesisMergeEvent.properties.parentHash.minItems = 1;
continuityGenesisMergeEvent.properties.parentHash.maxItems = 1;
delete continuityGenesisMergeEvent.properties.treeHash;

const webLedgerConfigurationEvent = {
  title: 'Continuity WebLedgerConfigurationEvent',
  additionalProperties: false,
  // signature is not required
  required: ['@context', 'basisBlockHeight', 'creator', 'ledgerConfiguration',
    'parentHash', 'treeHash', 'type'],
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
        basisBlockHeight: {
          type: 'integer',
          minimum: 0,
        },
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
        parentHash: {
          type: 'array',
          items: {
            type: 'string'
          },
          minItems: 1,
          maxItems: 1
        },
        proof: schemas.linkedDataSignature2018(),
        treeHash: {
          type: 'string'
        },
      },
    },
    signature: schemas.linkedDataSignature2018()
  }
};

const genesisConfigurationEvent = bedrock.util.clone(
  webLedgerConfigurationEvent);
genesisConfigurationEvent.required = genesisConfigurationEvent.required.filter(
  p => !['basisBlockHeight', 'parentHash', 'treeHash'].includes(p));
delete genesisConfigurationEvent.properties.parentHash;
delete genesisConfigurationEvent.properties.treeHash;

const webLedgerOperationEvent = {
  title: 'Continuity WebLedgerOperationEvent',
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

// saving this schema which is for an operation event with hashes

// const webLedgerOperationEvent = {
//   title: 'Continuity WebLedgerOperationEvent',
//   additionalProperties: false,
//   required: ['@context', 'operationHash', 'parentHash', 'treeHash', 'type'],
//   type: 'object',
//   properties: {
//     '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
//     basisBlockHeight: {
//       type: 'integer',
//       minimum: 0,
//     },
//     operationHash: {
//       type: 'array',
//       minItems: 1,
//     },
//     parentHash: {
//       type: 'array',
//       items: {type: 'string'},
//       minItems: 1
//     },
//     treeHash: {type: 'string'},
//     type: {
//       type: 'string',
//       enum: ['WebLedgerOperationEvent']
//     },
//   }
// };

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
module.exports.genesisConfigurationEvent = () => genesisConfigurationEvent;
module.exports.webLedgerEvents = () => webLedgerEvents;
module.exports.webLedgerConfigurationEvent = () => webLedgerConfigurationEvent;
