/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const {config} = require('bedrock');
const logger = require('../logger');

const {backoff: backoffConfig} = config['ledger-consensus-continuity'].gossip;

module.exports = class GossipPeer {
  constructor({id, worker}) {
    this.id = id;
    this.worker = worker;
    this.ledgerNodeId = worker.ledgerNode.id;
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
      // FIXME: remove using cache for peer status information; if we need
      // to persist anything, use mongo
      let status = await _cache.gossip.getPeerStatus(
        {peerId, ledgerNodeId});
      if(!status) {
        status = {
          backoff: 0,
          lastContactDate: 0,
          lastContactResult: 0,
          cursor: null
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
    const {backoff, lastContactDate, cursor} = await this.getStatus();
    const nextContact = lastContactDate + backoff;
    if(nextContact > Date.now()) {
      // not a recommended time to contact the peer
      // FIXME: remove me
      //console.log('NOT RECOMMENDED 1', this.id.substr(-4));
      return false;
    }
    const {blockHeight} = this.worker.consensusState;
    if(cursor && cursor.basisBlockHeight > blockHeight) {
      // the last time we communicated with the peer, it indicated that we
      // need to reach `cursor.basisBlockHeight` to get any more data from it
      // FIXME: remove me
      //console.log('NOT RECOMMENDED 2', this.id.substr(-4));
      return false;
    }
    // if the worker has a withheld event from this peer, then it is not
    // recommended
    // FIXME: make sync
    if(await this.worker.isPeerWithheld({peerId: this.id})) {
      // FIXME: remove me
      //console.log('NOT RECOMMENDED 3', this.id.substr(-4));
      return false;
    }

    return true;
  }

  async fail({error, cursor} = {}) {
    logger.error('Gossip peer failure.', {error});
    const {id: peerId, ledgerNodeId} = this;
    const status = await this.getStatus();
    status.backoff = status.backoff ? status.backoff * backoffConfig.factor :
      backoffConfig.min;
    status.lastContactDate = Date.now();
    status.lastContactResult = error.toString();
    // set cursor if specified
    if(cursor !== undefined) {
      status.cursor = status.cursor;
    }
    // record failure
    return _cache.gossip.setPeerStatus({ledgerNodeId, peerId, status});
  }

  async success({backoff = 0, cursor = null} = {}) {
    const {id: peerId, ledgerNodeId} = this;
    const status = await this.getStatus();
    status.backoff = backoff;
    status.lastContactDate = Date.now();
    status.lastContactResult = 'success';
    status.cursor = cursor;
    // record success
    return _cache.gossip.setPeerStatus({ledgerNodeId, peerId, status});
  }
};
