/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('../cache-key');
const _events = require('../events');
const _voters = require('../voters');
const async = require('async');
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

  _workLoop() {
    this.messageListener = this._onMessage.bind(this);
    const {ledgerNodeId} = this;
    async.auto({
      creator: callback => _voters.get({ledgerNodeId}, (err, result) => {
        if(err) {
          return callback(err);
        }
        this.creatorId = result.id;
        callback();
      }),
      // subscribing, but not adding an event handler
      subscribe: ['creator', (results, callback) => this.subscriber.subscribe(
        `continuity2017|event|${ledgerNodeId}`, callback)]
    }, err => {
      if(err) {
        return this._quit(err);
      }
      // important to start worker right away to catch regular events that
      // may have already been added
      this._work();
    });
  }

  _onMessage() {
    // this node should merge on both local regular and remote merge events
    if(!(this.working || this.halt)) {
      this.working = true;
      this.subscriber.removeListener('message', this.messageListener);

      // used a fixed debounce interval from the config
      return setTimeout(() => this._work(), this.config.fixedDebounce);
    }
  }

  _validateCache(callback) {
    const {creatorId, ledgerNode, ledgerNodeId} = this;
    async.auto({
      cacheHead: callback => _events.getHead({creatorId, ledgerNode}, callback),
      mongoHead: callback => _events.getHead(
        {creatorId, ledgerNode, useCache: false}, callback),
      consistencyCheck: ['cacheHead', 'mongoHead', (results, callback) => {
        const {cacheHead, mongoHead} = results;
        if(_.isEqual(cacheHead, mongoHead)) {
          // success
          return callback();
        }
        // this should never happen
        if((mongoHead.generation - cacheHead.generation) !== 1) {
          return callback(new BedrockError(
            'Cache is behind by more than one merge event.',
            'InvalidStateError',
            {cacheHead, mongoHead, ledgerNodeId}));
        }
        const {eventHash} = mongoHead;
        _events.repairCache({eventHash, ledgerNode}, callback);
      }]
    }, callback);
  }

  _work() {
    this.working = true;
    const {creatorId, ledgerNode} = this;
    async.auto({
      validateCache: callback => this._validateCache(callback),
      merge: ['validateCache', (results, callback) => async.doWhilst(
        callback => _events.merge({creatorId, ledgerNode}, callback),
        result => !this.halt && result && result.truncated, callback)],
      notify: ['merge', (results, callback) => {
        if(!results.merge) {
          return callback();
        }
        this.gossipAgent.sendNotification(err => {
          if(err) {
            logger.error(
              `An error occurred while attempting to send merge notification.`,
              err);
          }
          callback();
        });
      }]
    }, err => {
      this.working = false;
      if(err) {
        return this._quit(err);
      }
      if(this.halt) {
        return this._quit();
      }
      this.subscriber.on('message', this.messageListener);
    });
  }
};
