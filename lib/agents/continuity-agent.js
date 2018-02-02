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
    this.working = null;
    this.quitCalled = false;
  }

  start(callback) {
    if(!(callback && typeof callback === 'function')) {
      throw new TypeError('`callback` is required.');
    }
    this.onQuit = callback;
    // subclasses must implement a _workLoop method
    if(!this._workLoop) {
      throw new TypeError('A `_workLoop` method must be implemented.');
    }
    this._workLoop();
  }

  stop() {
    this.halt = true;
    if(!this.working && !this.quitCalled) {
      this._quit();
    }
  }

  _quit(err) {
    if(this._beforeQuit) {
      this._beforeQuit();
    }
    if(err) {
      logger.error(`Error in continuity agent: ${this.agentName}`, err);
    }
    logger.verbose(`Stopping continuity agent: ${this.agentName}`);
    if(!this.quitCalled) {
      this.quitCalled = true;
      this.onQuit();
    }
  }
};
