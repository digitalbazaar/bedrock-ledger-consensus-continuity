/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const {config} = require('bedrock');
const logger = require('../logger');

module.exports = class GossipPeer {
  constructor({id, peer, worker}) {
    this.id = id;
    // FIXME: require `peer`
    if(peer) {
      this._peer = peer;
      this.id = peer.id;
    }
    this.worker = worker;
    this.ledgerNodeId = worker.ledgerNode.id;
    this._status = null;
    this._initialized = false;
  }

  /**
   * Gets peer status information. This includes:
   *
   * backoff {Number} - The backoff period for the peer in ms.
   * lastPullAt {Number} - The timestamp (in ms elapsed since the UNIX epoch)
   *   since the last gossip pull attempt.
   * lastPullResult {string} - The result of the last gossip pull attempt.
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
          lastPullAt: 0,
          lastPullResult: 0,
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

  // FIXME: replace `isRecommended` with `_isRecommended` and delete
  // `isRecommended`
  _isRecommended() {
    return this.peer.recommended;
  }
  async isRecommended() {
    const {backoff, lastPullAt, cursor} = await this.getStatus();
    const nextContact = lastPullAt + backoff;
    if(nextContact > Date.now()) {
      // not a recommended time to contact the peer
      return false;
    }
    const {blockHeight} = this.worker.consensusState;
    if(cursor && cursor.basisBlockHeight > blockHeight) {
      // the last time we communicated with the peer, it indicated that we
      // need to reach `cursor.basisBlockHeight` to get any more data from it
      return false;
    }
    // if the worker has a withheld event from this peer, then it is not
    // recommended
    if(this.worker.isPeerWithheld({peerId: this.id})) {
      return false;
    }

    return true;
  }

  isWithheld() {
    return this.worker.isPeerWithheld({peerId: this.id});
  }

  isNotifier() {
    // FIXME: update once status is passed via constructor
    return this._status.lastPushAt > this._status.lastPullAt;
  }

  async fail({error, cursor} = {}) {
    logger.error('Gossip peer failure.', {error});
    const {id: peerId, ledgerNodeId} = this;
    const status = await this.getStatus();
    const {backoff: backoffConfig} =
      config['ledger-consensus-continuity'].gossip;
    status.backoff = status.backoff ? status.backoff * backoffConfig.factor :
      backoffConfig.min;
    status.lastPullAt = Date.now();
    status.lastPullResult = error.toString();
    // set cursor if specified
    if(cursor !== undefined) {
      status.cursor = status.cursor;
    }

    // FIXME: update peer information in mongo peers collection

    // record failure
    return _cache.gossip.setPeerStatus({ledgerNodeId, peerId, status});
  }

  async success({backoff = 0, cursor = null} = {}) {
    const {id: peerId, ledgerNodeId} = this;
    const status = await this.getStatus();
    status.backoff = backoff;
    status.lastPullAt = Date.now();
    status.lastPullResult = 'success';
    status.cursor = cursor;

    // FIXME: update peer information in mongo peers collection

    // record success
    return _cache.gossip.setPeerStatus({ledgerNodeId, peerId, status});
  }
};
