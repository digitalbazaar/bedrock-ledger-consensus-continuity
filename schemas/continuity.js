/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config} = bedrock;
const {constants} = config;
const _continuityConstants = require('../lib/continuityConstants');
const {schemas} = require('bedrock-validation');

const mergeEventProof = {
  type: 'object',
  additionalProperties: false,
  required: [
    'created', 'proofValue', 'proofPurpose', 'type', 'verificationMethod'
  ],
  properties: {
    created: schemas.w3cDateTime(),
    proofValue: {
      type: 'string'
    },
    proofPurpose: {
      type: 'string',
      enum: ['assertionMethod']
    },
    type: {
      type: 'string',
      enum: ['Ed25519Signature2020']
    },
    verificationMethod: {
      type: 'string'
    },
  }
};

const continuityMergeEvent = {
  title: 'Continuity2017 ContinuityMergeEvent',
  additionalProperties: false,
  required: [
    '@context', 'basisBlockHeight', 'mergeHeight',
    'parentHash', 'proof', 'treeHash', 'type'
    // `parentHashCommitment` is optional
    // `peerCommitment` is optional
  ],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext([
      constants.WEB_LEDGER_CONTEXT_V1_URL,
      constants.ED25519_2020_CONTEXT_V1_URL
    ]),
    basisBlockHeight: {
      type: 'integer',
      minimum: 0,
    },
    mergeHeight: {
      type: 'integer',
      minimum: 0,
    },
    parentHash: {
      type: 'array',
      items: {
        type: 'string'
      },
      // TODO: ensure a check to make sure node is pulling in new information
      // parentHash is usually > 1, but can be 1 when there is a network with
      // only 1 witness and operations are added on that witness
      minItems: 1,
      maxItems: _continuityConstants.mergeEvents.maxParents
    },
    parentHashCommitment: {
      type: 'array',
      items: {
        // FIXME: this should have a maximum length
        type: 'string'
      },
      minItems: 1,
      maxItems: 1
    },
    peerCommitment: {
      type: 'array',
      items: {
        // FIXME: this should have a maximum length
        type: 'string'
      },
      minItems: 1,
      maxItems: 1
    },
    proof: mergeEventProof,
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

// allow configuration to specify a different consensusMethod
const nonContinuity2017LedgerConfiguration = {
  type: 'object',
  required: ['consensusMethod'],
  properties: {
    consensusMethod: {
      type: 'string',
      not: {enum: ['Continuity2017']}
    }
  }
};

const continuity2017ledgerConfiguration = {
  title: 'Continuity2017 Ledger Configuration',
  type: 'object',
  required: [
    'consensusMethod', 'creator', 'witnessSelectionMethod', 'ledger',
    'sequence', 'type'
  ],
  // additional properties are validated at the ledgerNode layer
  additionalProperties: true,
  properties: {
    creator: {
      type: 'string',
    },
    type: {
      type: 'string',
      enum: ['WebLedgerConfiguration']
    },
    witnessSelectionMethod: {
      // NOTE: this schema should not be too prescriptive, various elector
      // selection methods may require additional properties here such as
      // `electorPool`. The ledger validator for a given ledger implementation
      // will be responsible for ensuring that the remaining properties are
      // valid.
      type: 'object',
      required: ['type'],
      additionalProperties: true,
      properties: {
        type: {type: 'string'},
      }
    },
    ledger: {
      type: 'string',
    },
    consensusMethod: {
      type: 'string',
      enum: ['Continuity2017'],
    },
    proof: schemas.linkedDataSignature2020(),
    sequence: {
      type: 'integer',
      minimum: 1,
    }
  },
};

const webLedgerConfigurationEvent = {
  title: 'Continuity2017 WebLedgerConfigurationEvent',
  additionalProperties: false,
  // signature is not required
  required: ['@context', 'basisBlockHeight', 'ledgerConfiguration',
    'parentHash', 'treeHash', 'type'],
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    basisBlockHeight: {
      type: 'integer',
      minimum: 0,
    },
    type: {
      type: 'string',
      enum: ['WebLedgerConfigurationEvent']
    },
    ledgerConfiguration: {
      anyOf: [
        continuity2017ledgerConfiguration,
        nonContinuity2017LedgerConfiguration
      ],
    },
    parentHash: {
      type: 'array',
      items: {
        type: 'string'
      },
      minItems: 1,
      maxItems: 1
    },
    signature: schemas.linkedDataSignature2020(),
    treeHash: {
      type: 'string'
    },
  }
};

const genesisConfigurationEvent = bedrock.util.clone(
  webLedgerConfigurationEvent);
genesisConfigurationEvent.title =
  'Continuity2017 Genesis WebLedgerConfigurationEvent';
genesisConfigurationEvent.required = genesisConfigurationEvent.required.filter(
  p => !['basisBlockHeight', 'parentHash', 'treeHash'].includes(p));
delete genesisConfigurationEvent.properties.parentHash;
delete genesisConfigurationEvent.properties.treeHash;
// the genesis config may not be a non Continuity2017 configuration
const genesisLedgerConfiguration = bedrock.util.clone(
  continuity2017ledgerConfiguration);
genesisLedgerConfiguration.title =
  'Continuity2017 Genesis Ledger Configuration';
genesisLedgerConfiguration.required = genesisLedgerConfiguration.required
  .filter(p => !['creator'].includes(p));
genesisLedgerConfiguration.properties.sequence = {
  type: 'integer',
  enum: [0],
};
genesisConfigurationEvent.properties.ledgerConfiguration =
  genesisLedgerConfiguration;

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
  anyOf: [
    webLedgerOperationEvent,
    webLedgerConfigurationEvent,
    continuityMergeEvent
  ]
};

// all the other properties of operations have been validated at the ledgerNode
// layer
const localOperation = {
  title: 'Continuity Local Operation Schema',
  type: 'object',
  required: ['creator'],
  properties: {
    creator: {
      type: 'string',
      // this will be filled in at runtime
      enum: [],
    }
  }
};

module.exports.continuityGenesisMergeEvent = () => continuityGenesisMergeEvent;
module.exports.genesisConfigurationEvent = () => genesisConfigurationEvent;
module.exports.genesisLedgerConfiguration = () => genesisLedgerConfiguration;
module.exports.localOperation = () => localOperation;
module.exports.webLedgerEvents = () => webLedgerEvents;
module.exports.webLedgerConfigurationEvent = () => webLedgerConfigurationEvent;
