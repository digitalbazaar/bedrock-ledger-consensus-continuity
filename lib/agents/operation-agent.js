/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _events = require('../events');
const _voters = require('../voters');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
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
        `continuity2017|operation|${ledgerNodeId}`, callback)]
    }, err => {
      if(err) {
        return this._quit(err);
      }
      // important to start worker right away to catch regular ops that
      // may have already been added
      this._work();
    });
  }

  _onMessage() {
    if(!(this.working || this.halt)) {
      this.working = true;
      this.subscriber.removeListener('message', this.messageListener);
      setTimeout(() => this._work(), this.debounce);
    }
  }

  _work() {
    this.working = true;
    const {creatorId, ledgerNode} = this;
    async.doWhilst(
      callback => _events.create({creatorId, ledgerNode}, callback),
      result => !this.halt && result && result.hasMore, err => {
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
