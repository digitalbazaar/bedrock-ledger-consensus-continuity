/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('../cache-key');
const _events = require('../events');
const _voters = require('../voters');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('../logger');
// const BedrockError = bedrock.util.BedrockError;
const ContinuityAgent = require('./continuity-agent');

module.exports = class MergeAgent extends ContinuityAgent {
  constructor({agentName, gossipAgent, ledgerNode}) {
    agentName = agentName || 'merge';
    super({agentName, ledgerNode});
    this.creatorId = null;
    this.debounce = config['ledger-consensus-continuity'].merge.debounce;
    this.debounceKey = _cacheKey.mergeDebounce(ledgerNode.id);
    this.gossipAgent = gossipAgent;
    this.messageListener = null;
    this.subscriber = new cache.Client().client;
  }

  _calculateDebounce(callback) {
    // minimumValue/maximumValue must be a multiple of adjustmentSize;
    const adjustmentSize = 100;
    const {debounceKey} = this;
    // TODO: make configurable
    const minimumValue = 1000;
    const maximumValue = 60000;
    // TODO: how is ideal target calculated?
    const targetOustandingCount = 500;
    async.auto({
      outMerge: callback => this._outstandingMergeCount(callback),
      current: callback => cache.client.get(debounceKey, (err, result) => {
        if(err) {
          return callback(err);
        }
        // key has not been set yet, use minimumValue to start
        if(result === null) {
          return callback(null, minimumValue);
        }
        callback(null, parseInt(result, 10));
      }),
      throttle: ['current', 'outMerge', (results, callback) => {
        const {current, outMerge} = results;
        const deviation = targetOustandingCount * 0.2;
        const max = targetOustandingCount + deviation;
        const min = targetOustandingCount - deviation;
        if(outMerge > max && current < maximumValue) {
          return cache.client.incrby(debounceKey, adjustmentSize, callback);
        }
        if(outMerge < min && current > minimumValue) {
          return cache.client.decrby(debounceKey, adjustmentSize, callback);
        }
        // current value is good, keep using it
        callback(null, current);
      }]
    }, (err, results) => {
      if(err) {
        return callback(err);
      }
      callback(null, results.throttle);
    });
  }

  _onQuit() {
    this.subscriber.quit();
  }

  _workLoop() {
    this.messageListener = this._onMessage.bind(this);
    async.auto({
      creator: callback => _voters.get(
        {ledgerNodeId: this.ledgerNode.id}, (err, result) => {
          if(err) {
            return callback(err);
          }
          this.creatorId = result.id;
          callback();
        }),
      // subscribing, but not adding an event handler
      subscribe: ['creator', (results, callback) => this.subscriber.subscribe(
        'continuity2017.event', callback)]
    }, err => {
      if(err) {
        return this._quit(err);
      }
      // important to start worker right away to catch regular events that
      // may have already been added
      this._work();
    });
  }

  // ignore message
  _onMessage(channel /*, message */) {
    // this node should merge on both local regular and remote merge events
    if(channel === 'continuity2017.event' && !this.working && !this.halt) {
      this.working = true;
      this.subscriber.removeListener('message', this.messageListener);
      this._calculateDebounce((err, result) => {
        if(err) {
          return this._quit(err);
        }
        logger.debug(`Merge throttle amount: ${result}`);
        setTimeout(() => this._work(), result);
      });
    }
  }

  _outstandingMergeCount(callback) {
    const collection = this.ledgerNode.storage.events.collection;
    const query = {
      'meta.continuity2017.type': 'm',
      'meta.consensus': {$exists: false},
    };
    // TODO: would specifying `limit` here be helpful?
    collection.count(query, {hint: 'continuity2'}, callback);
  }

  _work() {
    this.working = true;
    const {creatorId, ledgerNode} = this;
    async.auto({
      merge: callback => async.doWhilst(
        callback => _events.merge({creatorId, ledgerNode}, callback),
        result => !this.halt && result && result.truncated, callback),
      notify: ['merge', (results, callback) => {
        if(!results.merge) {
          return callback();
        }
        this.gossipAgent.sendNotification(callback);
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
