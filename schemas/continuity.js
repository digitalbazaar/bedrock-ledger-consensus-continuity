/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const schemas = require('bedrock-validation').schemas;

const signature = schemas.linkedDataSignature({
  properties: {
    type: {
      enum: ['LinkedDataSignature2015']
    }
  }
});

const vote = {
  title: 'Continuity Vote',
  type: 'object',
  properties: {
    '@context': schemas.jsonldContext(),
    blockHeight: {
      type: 'integer',
      minimum: 0,
      required: true
    },
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
    signature
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
      electionResults: {
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
    blockHeight: {
      type: 'integer',
      minimum: 0,
      required: true
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

module.exports.blockStatus = () => (blockStatus);
module.exports.election = () => (election);
module.exports.vote = () => (vote);
