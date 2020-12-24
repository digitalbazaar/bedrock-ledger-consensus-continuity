/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config} = bedrock;
const {'ledger-consensus-continuity': {gossip: {maxEvents}}} = config;

const getEvents = {
  title: 'Continuity Server getEvents',
  type: 'object',
  additionalProperties: false,
  required: ['eventHash'],
  properties: {
    eventHash: {
      type: 'array',
      minItems: 1,
      maxItems: maxEvents,
      items: {
        type: 'string',
        maxLength: 256,
      }
    }
  }
};

// TODO: can this be more specific?
const peerId = {
  type: 'string',
};

const gossip = {
  title: 'Continuity Gossip',
  // ensure that `headsOnly` and `creatorHeads` are mutually exclusive
  anyOf: [{
    type: 'object',
    required: ['headsOnly', 'peerId'],
    additionalProperties: false,
    properties: {
      headsOnly: {
        type: 'boolean',
      },
      peerId,
    }
  }, {
    type: 'object',
    // FIXME: `creatorHeads` is old, remove
    required: ['blockHeight', 'peerId', 'peerHeads', 'creatorHeads'],
    additionalProperties: false,
    properties: {
      blockHeight: {
        type: 'number'
      },
      peerId,
      peerHeads: {
        type: 'array',
        maxItems: 1000,
        items: {
          // FIXME: add more validation rules to shape of peer heads
          type: 'object'/*,
          properties: {
            creator,
            generation,
            eventHash
          },
          additionalProperties: false*/
        }
      },
      // FIXME: remove creator heads
      creatorHeads: {
        type: 'object'
      }
    }
  }]
};

const notification = {
  title: 'Continuity Server Gossip Notification',
  type: 'object',
  additionalProperties: false,
  require: ['peerId'],
  properties: {peerId}
};

module.exports.getEvents = () => getEvents;
module.exports.gossip = () => gossip;
module.exports.notification = () => notification;
