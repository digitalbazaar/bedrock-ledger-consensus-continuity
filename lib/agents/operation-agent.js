/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _events = require('../events');
const _voters = require('../voters');
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

  async _workLoop() {
    this.messageListener = this._onMessage.bind(this);
    const {ledgerNodeId} = this;
    try {
      this.creatorId = (await _voters.get({ledgerNodeId})).id;
      await this.subscriber.subscribe(
        `continuity2017|operation|${ledgerNodeId}`);
    } catch(e) {
      return this._quit(e);
    }
    // important to start worker right away to catch operations that
    // may have already been added
    this._work();
  }

  _onMessage() {
    if(!(this.working || this.halt)) {
      this.working = true;
      this.subscriber.removeListener('message', this.messageListener);
      setTimeout(() => this._work(), this.debounce);
    }
  }

  async _work() {
    this.working = true;
    const {ledgerNode} = this;
    let error;
    let result;
    do {
      result = undefined;
      try {
        result = await _events.create({ledgerNode});
      } catch(e) {
        error = e;
      }
    } while(!error && !this.halt && result && result.hasMore);

    this.working = false;
    if(error) {
      return this._quit(error);
    }
    if(this.halt) {
      return this._quit();
    }
    this.subscriber.on('message', this.messageListener);
  }
};
