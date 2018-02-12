/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _events = require('../events');
const _voters = require('../voters');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
// const logger = require('../logger');
// const BedrockError = bedrock.util.BedrockError;
const ContinuityAgent = require('./continuity-agent');

module.exports = class MergeAgent extends ContinuityAgent {
  constructor({agentName, gossipAgent, ledgerNode}) {
    agentName = agentName || 'merge';
    super({agentName, ledgerNode});
    this.creatorId = null;
    this.debounce = config['ledger-consensus-continuity'].merge.debounce;
    this.gossipAgent = gossipAgent;
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

  _onMessage(channel, message) {
    if(channel === 'continuity2017.event' && message === 'write' &&
      !this.working) {
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
        callback => _events.merge({creatorId, ledgerNode}, callback),
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
