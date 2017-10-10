/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const config = require('bedrock').config;
const constants = config.constants;
const schemas = require('bedrock-validation').schemas;

const blockHeight = {
  type: 'integer',
  minimum: 0,
  required: true
};

const vote = {
  title: 'Continuity Vote',
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(),
    blockHeight,
    manifestHash: schemas.url(),
    voteRound: {
      type: 'integer',
      minimum: 1,
      required: true
    },
    voter: schemas.url(),
    recommendedElector: {
      type: 'array',
      minItems: 1,
      items: {
        type: 'object',
        properties: {
          id: {type: schemas.url()}
        }
      }
    },
    signature: schemas.linkedDataSignature()
  },
  additionalProperties: false
};

const election = {
  title: 'Continuity Election',
  type: 'array',
  items: {
    type: 'object',
    properties: {
      topic: {
        type: 'string',
        required: true
      },
      electionResult: {
        type: 'array',
        items: {
          type: vote
        }
      }
    },
    additionalProperties: false
  }
};

const blockStatus = {
  title: 'Continuity Block Status',
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(),
    blockHeight,
    previousBlockHash: {
      type: 'string',
      // not required for blockHeight of 0
      required: false
    },
    consensusPhase: {
      type: 'string',
      required: true
    },
    election: {
      type: election,
      required: false
    },
    eventHash: {
      type: 'array',
      items: schemas.url(),
      required: false
    }
  },
  additionalProperties: false
};

const webLedgerConfigEvent = {
  title: 'WebLedgerConfigurationEvent',
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    type: {
      // TODO: enum `WebLedgerConfigurationEvent`
      type: 'string',
      required: true
    },
    ledgerConfiguration: {
      type: 'object',
      properties: {
        type: {
          // TODO: enum `WebLedgerConfiguration`
          type: 'string',
          required: true
        },
        ledger: {
          type: 'string',
          required: true
        },
        consensusMethod: {
          type: 'string',
          required: true
        }
      },
      required: true
    },
    signature: {
      type: schemas.linkedDataSignature(),
      // FIXME: should signature be required?
      required: false
    }
  },
  additionalProperties: false
};

const webLedgerEvent = {
  title: 'WebLedgerEvent',
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(constants.WEB_LEDGER_CONTEXT_V1_URL),
    type: {
      // TODO: enum `WebLedgerEvent`
      type: 'string',
      required: true
    },
    operation: {
      type: 'string',
      required: true
    },
    input: {
      type: 'array',
      minItems: 1,
      required: true
    },
    signature: {
      type: schemas.linkedDataSignature(),
      // FIXME: should signature be required?
      required: false
    }
  },
  additionalProperties: false
};

const webLedgerEvents = {
  title: 'Web Ledger Events',
  type: [webLedgerConfigEvent, webLedgerEvent],
  required: true
};

const event = {
  type: 'object',
  title: 'Continuity Event',
  properties: {
    event: webLedgerEvents,
    eventHash: {
      type: 'string',
      required: true
    }
  },
  additionalProperties: false
};

const manifest = {
  title: 'Continuity Manifest',
  type: 'object',
  properties: {
    // FIXME: @context?
    type: {
      type: 'string',
      required: true,
      enum: ['Events', 'RollCall']
    },
    id: schemas.identifier(),
    blockHeight,
    item: {
      type: 'array',
      minItems: 1,
      required: true,
      items: {
        type: schemas.url()
      }
    }
  }
};

module.exports.blockStatus = () => (blockStatus);
module.exports.election = () => (election);
module.exports.event = () => (event);
module.exports.manifest = () => (manifest);
module.exports.vote = () => (vote);
module.exports.webLedgerEvents = () => (webLedgerEvents);
