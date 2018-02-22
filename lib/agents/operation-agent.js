/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

// const _cacheKey = require('../cache-key');
const _events = require('../events');
const _voters = require('../voters');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
// const logger = require('../logger');
// const BedrockError = bedrock.util.BedrockError;
const ContinuityAgent = require('./continuity-agent');

module.exports = class OperationAgent extends ContinuityAgent {
  constructor({agentName, ledgerNode}) {
    agentName = agentName || 'operation';
    super({agentName, ledgerNode});
    this.creatorId = null;
    this.debounce = config['ledger-consensus-continuity'].operations.debounce;
    this.messageListener = null;
    this.subscriber = new cache.Client().client;
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
        'continuity2017.operation', callback)]
    }, err => {
      if(err) {
        return this._quit(err);
      }
      // important to start worker right away to catch regular ops that
      // may have already been added
      this._work();
    });
  }

  // ignore message
  _onMessage(channel /*, message */) {
    // this node should merge on both local regular and remote merge events
    if(channel === 'continuity2017.operation' && !this.working) {
      this.working = true;
      this.subscriber.removeListener('message', this.messageListener);
      setTimeout(() => this._work(), this.debounce);
    }
  }

  _work() {
    this.working = true;
    const {creatorId, ledgerNode} = this;
    async.auto({
      merge: callback => async.doWhilst(
        callback => _events.create({creatorId, ledgerNode}, callback),
        result => result && result.truncated, callback),
      notify: ['merge', (results, callback) => {
        if(!results.merge) {
          return callback();
        }
        this.gossipAgent.sendNotification(callback);
      }]
    }, err => {
      if(err) {
        return this._quit(err);
      }
      if(this.halt) {
        return this._quit();
      }
      this.working = false;
      this.subscriber.on('message', this.messageListener);
    });
  }
};
