/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _events = require('../events');
const _voters = require('../voters');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('../logger');
const {BedrockError} = bedrock.util;
const ContinuityAgent = require('./continuity-agent');

module.exports = class MergeAgent extends ContinuityAgent {
  constructor({agentName, gossipAgent, ledgerNode}) {
    agentName = agentName || 'merge';
    super({agentName, ledgerNode});
    this.creatorId = null;
    this.config = config['ledger-consensus-continuity'].merge;
    this.gossipAgent = gossipAgent;
    this.messageListener = null;
    this.subscriber = new cache.Client().client;
  }

  _onQuit() {
    this.subscriber.quit();
  }

  async _workLoop() {
    this.messageListener = this._onMessage.bind(this);
    const {ledgerNodeId} = this;
    try {
      this.creatorId = (await _voters.get({ledgerNodeId})).id;
      await this.subscriber.subscribe(`continuity2017|event|${ledgerNodeId}`);
    } catch(e) {
      return this._quit(e);
    }
    // important to start worker right away to catch regular events that
    // may have already been added
    this._work();
  }

  _onMessage(channel, message) {
    // this node should only merge local regular events and whenever a merge is
    // needed by another agent
    if(!(message === 'regular' || message === 'needsMerge')) {
      return;
    }

    if(!(this.working || this.halt)) {
      this.working = true;
      this.subscriber.removeListener('message', this.messageListener);

      // used a fixed debounce interval from the config
      return setTimeout(() => this._work(), this.config.fixedDebounce);
    }
  }

  async _validateCache() {
    const {creatorId, ledgerNode, ledgerNodeId} = this;
    const [cacheHead, mongoHead] = await Promise.all([
      _events.getHead({creatorId, ledgerNode}),
      _events.getHead({creatorId, ledgerNode, useCache: false})
    ]);
    if(_.isEqual(cacheHead, mongoHead)) {
      // success
      return;
    }
    // this should never happen
    if((mongoHead.generation - cacheHead.generation) !== 1) {
      throw new BedrockError(
        'Cache is behind by more than one merge event.',
        'InvalidStateError',
        {cacheHead, mongoHead, ledgerNodeId});
    }
    const {eventHash} = mongoHead;
    await _events.repairCache({eventHash, ledgerNode});
  }

  async _work() {
    this.working = true;
    const {creatorId, ledgerNode, ledgerNodeId} = this;
    let notifyFlag;
    try {
      notifyFlag = await _cache.gossip.notifyFlag({ledgerNodeId});
      await this._validateCache();
    } catch(e) {
      return this._quit(e);
    }
    let result;
    let error;
    do {
      result = undefined;
      try {
        result = await _events.merge({creatorId, ledgerNode});
      } catch(e) {
        error = e;
      }
    } while(!error && !this.halt && result && result.truncated);
    if(error) {
      this.working = false;
      return this._quit(error);
    }
    // there is a new merge event, or the last notification attempt
    // did not result in a gossip session
    // it is imperative that notifications eventually result in a gossip
    // session in order for the local node to have its events included
    // in the ledger
    if(result || notifyFlag !== null) {
      try {
        await Promise.all([
          _cache.gossip.notifyFlag({add: true, ledgerNodeId}),
          this.gossipAgent.sendNotification()
        ]);
      } catch(e) {
        // just log the error, another attempt will be made on the next cycle
        logger.error(
          'An error occurred while attempting to send merge notification.',
          {error: e});
      }
    }
    this.working = false;
    if(this.halt) {
      return this._quit();
    }
    this.subscriber.on('message', this.messageListener);
  }
};
