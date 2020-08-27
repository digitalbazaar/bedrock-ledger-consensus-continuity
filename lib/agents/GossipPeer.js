/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const {config} = require('bedrock');
const logger = require('../logger');

const {backoff: backoffConfig} = config['ledger-consensus-continuity'].gossip;

module.exports = class GossipPeer {
  constructor({creatorId, ledgerNodeId}) {
    this.creatorId = creatorId;
    this.ledgerNodeId = ledgerNodeId;
    this._backoff = 0;
    this._lastContactDate = 0;
    this._lastContactResult = null;
    this._initialized = false;
  }

  async getStatus() {
    const {creatorId, ledgerNodeId} = this;
    if(!this.initialized) {
      const status = await _cache.gossip.getPeerStatus(
        {creatorId, ledgerNodeId});
      // backoff in ms
      this._backoff = status[0] ? parseInt(status[0]) : 0;
      // timestamp
      this._lastContactDate = status[1] ? parseInt(status[1]) : 0;
      // success / fail
      this._lastContactResult = status[2] ? status[2] : 0;
      this._initialized = true;
    }
    return {
      backoff: this._backoff,
      lastContactDate: this._lastContactDate,
      lastContactResult: this._lastContactResult
    };
  }

  async fail(err) {
    logger.error('Gossip peer failure.', {error: err});
    const {creatorId, ledgerNodeId} = this;
    this._backoff = this._backoff ? this._backoff * backoffConfig.factor :
      backoffConfig.min;
    this._lastContactDate = Date.now();
    this._lastContactResult = err.toString();
    // record success
    return _cache.gossip.setPeerStatus({
      backoff: this._backoff,
      creatorId,
      lastContactDate: this._lastContactDate,
      lastContactResult: this._lastContactResult,
      ledgerNodeId
    });
  }

  async success() {
    const {creatorId, ledgerNodeId} = this;
    this._backoff = 0;
    this._lastContactDate = Date.now();
    this._lastContactResult = 'success';
    // record success
    return _cache.gossip.setPeerStatus({
      backoff: this._backoff,
      creatorId,
      lastContactDate: this._lastContactDate,
      lastContactResult: this._lastContactResult,
      ledgerNodeId
    });
  }
};
