/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
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
        // this cap should be the same as the max gossip events
        maxLength: 64,
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
  // `localEventNumber` is optional
  required: ['basisBlockHeight', 'peerId', 'peerHeads'],
  additionalProperties: false,
  properties: {
    basisBlockHeight: {
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
      // this cap should be no more than 2 * max gossip events to allow for
      // 2 heads per creator
      maxItems: 128,
      items: {
        // every item in the array must be an event hash
        type: 'string',
      }
    }
  }
};

const notification = {
  title: 'Continuity Server Gossip Notification',
  type: 'object',
  additionalProperties: false,
  require: ['peer'],
  properties: {
    peer: {
      type: 'object',
      additionalProperties: false,
      require: ['id', 'url'],
      properties: {
        id: peerId,
        url: {
          type: 'string'
        }
      }
    }
  }
};

module.exports.getEvents = () => getEvents;
module.exports.gossip = () => gossip;
module.exports.notification = () => notification;
