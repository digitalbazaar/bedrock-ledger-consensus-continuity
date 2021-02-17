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
      // this cap should be the same as the max gossip events
      maxItems: 100,
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
  require: ['peerId'],
  properties: {peerId}
};

module.exports.getEvents = () => getEvents;
module.exports.gossip = () => gossip;
module.exports.notification = () => notification;
