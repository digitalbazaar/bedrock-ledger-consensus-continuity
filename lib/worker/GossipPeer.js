/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const {config} = require('bedrock');
const logger = require('../logger');

const {backoff: backoffConfig} = config['ledger-consensus-continuity'].gossip;

module.exports = class GossipPeer {
  constructor({id, ledgerNodeId}) {
    this.id = id;
    this.ledgerNodeId = ledgerNodeId;
    this._status = null;
    this._initialized = false;
  }

  /**
   * Gets peer status information. This includes:
   *
   * backoff {Number} - The backoff period for the peer in ms.
   * lastContactDate {Number} - The last contact date in ms elapsed since
   *   the UNIX epoch.
   * lastContactResult {string} - The result of the last contact.
   */
  async getStatus() {
    const {id: peerId, ledgerNodeId} = this;
    if(!this.initialized) {
      let status = await _cache.gossip.getPeerStatus(
        {peerId, ledgerNodeId});
      if(!status) {
        status = {
          backoff: 0,
          lastContactDate: 0,
          lastContactResult: 0
        };
      }
      this._status = status;
      this._initialized = true;
    }
    return this._status;
  }

  async clearBackoff() {
    const status = await this.getStatus();
    status.backoff = 0;
    // TODO: do we want to update the cache?
  }

  async isRecommended() {
    const {backoff, lastContactDate} = await this.getStatus();
    const nextContact = lastContactDate + backoff;
    return nextContact <= Date.now();
  }

  async fail(err) {
    logger.error('Gossip peer failure.', {error: err});
    const {id: peerId, ledgerNodeId} = this;
    const status = await this.getStatus();
    status.backoff = status.backoff ? status._backoff * backoffConfig.factor :
      backoffConfig.min;
    status.lastContactDate = Date.now();
    status.lastContactResult = err.toString();
    // record failure
    return _cache.gossip.setPeerStatus({ledgerNodeId, peerId, status});
  }

  // FIXME: store truncated/next result
  async success({backoff = 0, result} = {}) {
    const {id: peerId, ledgerNodeId} = this;
    const status = await this.getStatus();
    status.backoff = backoff;
    status.lastContactDate = Date.now();
    status.lastContactResult = 'success';
    // record success
    return _cache.gossip.setPeerStatus({ledgerNodeId, peerId, status});
  }
};
