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
  // `blockEventCount`, `cursor` are optional
  required: ['basisBlockHeight', 'blockHeight', 'peerId', 'peerHeads'],
  additionalProperties: false,
  properties: {
    basisBlockHeight: {
      type: 'integer',
      minimum: 0
    },
    blockHeight: {
      type: 'integer',
      minimum: 0
    },
    blockEventCount: {
      type: 'integer',
      minimum: 0
    },
    localEventNumber: {
      type: 'integer',
      minimum: 0
    },
    peerId,
    peerHeads: {
      title: 'Continuity Gossip Peer Heads',
      type: 'array',
      // FIXME: what do we want to cap this at?
      maxItems: 1000,
      items: {
        type: 'object',
        required: ['creator', 'heads'],
        properties: {
          creator: {
            type: 'string'
          },
          heads: {
            type: 'array',
            // FIXME: what do we want to cap this at?
            maxItems: 2,
            items: {
              type: 'object',
              required: ['eventHash', 'generation'],
              properties: {
                // FIXME: this is unnecessary, do not require it to be sent;
                // it has to be looked up anyway
                generation: {
                  type: 'integer',
                  minimum: 0
                },
                // FIXME: all that really needs to be sent is an `eventHash`
                // ... we validate the rest
                eventHash: {
                  type: 'string'
                }
              },
              additionalProperties: false
            }
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
