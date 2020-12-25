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
  type: 'object',
  // `blockOrder` is optional
  required: ['blockHeight', 'peerId', 'peerHeads'],
  additionalProperties: false,
  properties: {
    blockHeight: {
      type: 'number'
    },
    eventIndex: {
      type: 'number'
    },
    peerId,
    peerHeads: {
      type: 'array',
      maxItems: 1000,
      items: {
        type: 'object',
        properties: {
          creator: {
            type: 'string'
          },
          generation: {
            type: 'number'
          },
          eventHash: {
            type: 'string'
          }
        },
        additionalProperties: false
      }
    }
  }
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
