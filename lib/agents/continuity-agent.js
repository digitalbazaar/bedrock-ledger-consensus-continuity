/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const logger = require('../logger');
const uuid = require('uuid/v4');

module.exports = class ContinuityAgent {
  constructor({agentName, ledgerNode}) {
    this.agentName = agentName || uuid();
    this.ledgerNode = ledgerNode;
    this.halt = false;
    this.onQuit = null;
    this.running = false;
    this.working = null;
    this.quitCalled = false;
  }

  start(callback) {
    if(!(callback && typeof callback === 'function')) {
      throw new TypeError('`callback` is required.');
    }
    this.onQuit = callback;
    this.quitCalled = false;
    // subclasses must implement a _workLoop method
    if(!this._workLoop) {
      throw new TypeError('A `_workLoop` method must be implemented.');
    }
    // TODO: this could be moved into subclass _onStart
    if(this.running) {
      throw new Error(
        `The agent '${this.agentName}' is already running.`);
    }
    this.running = true;
    if(this._onStart) {
      this._onStart();
    }
    this._workLoop();
  }

  stop() {
    this.halt = true;
    if(!this.working && !this.quitCalled) {
      this._quit();
    }
    this.running = false;
  }

  _quit(err) {
    if(this._onQuit) {
      this._onQuit();
    }
    this.running = false;
    if(err) {
      logger.error(`Error in continuity agent: ${this.agentName}`, err);
    }
    logger.debug(`Stopping continuity agent: ${this.agentName}`);
    if(!this.quitCalled) {
      this.quitCalled = true;
      this.onQuit();
    }
  }
};
