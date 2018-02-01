/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _events = require('./events');
const _cacheKey = require('./cache-key');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const database = require('bedrock-mongodb');
const jsonld = bedrock.jsonld;
const logger = require('./logger');

module.exports = class GossipAgent {
  constructor({ledgerNode}) {
    this.ledgerNode = ledgerNode;
    this.halt = false;
    this.onQuit = null;
  }

  /*
    elector rotation list
    an elector failure key

    get all voters and vet them based on some process
    get list of all notifications
    pick one from each list to gossip with this iteration
    keep going like this until halt
  */
  start() {

  }

};
