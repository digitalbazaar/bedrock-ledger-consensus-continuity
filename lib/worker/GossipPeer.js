/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {config} = require('bedrock');
const logger = require('../logger');

module.exports = class GossipPeer {
  constructor({id, peer, worker}) {
    this.id = id;
    // FIXME: require `peer`
    if(peer) {
      this._peer = peer;
      this.id = peer.id;
    } else {
      this._peer = {id};
    }
    this.worker = worker;
    this.ledgerNodeId = worker.ledgerNode.id;
    this._status = null;
    this._initialized = false;
    this._backoff = 0;
  }

  /**
   * Gets peer status information. This includes:
   *
   * backoffUntil {Number} - The timestamp (in ms elapsed since the UNIX epoch)
   *   that should be waited for before attempting another pull.
   * lastPullAt {Number} - The timestamp (in ms elapsed since the UNIX epoch)
   *   since the last gossip pull attempt.
   * lastPullResult {string} - The result of the last gossip pull attempt.
   */
  async getStatus() {
    const {id: peerId} = this;
    if(!this.initialized) {
      // FIXME: remove, initialization code entirely, `peer` information
      // will be passed into constructor
      const {worker: {ledgerNode}} = this;
      let status;
      try {
        const _peer = await ledgerNode.peers.get({id: peerId});
        status = _peer.status;
        this._peer = _peer;
        //console.log('_peer', _peer);
      } catch(e) {
        if(e.name !== 'NotFoundError') {
          throw e;
        }
      }

      if(!status) {
        status = {
          backoffUntil: 0,
          lastPullAt: 0,
          lastPullResult: 0,
          cursor: null
        };
        // FIXME: remove, rely on peers to be inserted elsewhere
        const peer = {...this._peer, status: {...status}};
        if(status.cursor) {
          // FIXME: rename `cursor.basisBlockHeight` to
          // `cursor.requiredBlockHeight`
          peer.status.requiredBlockHeight = status.cursor.basisBlockHeight;
        }
        try {
          await ledgerNode.peers.add({peer});
        } catch(e) {
          if(e.name !== 'DuplicateError') {
            throw e;
          }
        }
        this._peer = peer;
      }
      this._status = status;

      this._initialized = true;
    }
    return this._status;
  }

  async clearBackoff() {
    this._backoff = 0;
    // FIXME: either update the database at this point or remove this method;
    // this method is currently only called when selecting peers to gossip
    // with -- and it is at that point that we check for notifications,
    // however, this should be changed to write notifications to the database
    // and rely on gossip peer candidate refreshes to pull them in; when
    // those notifications are written to the database, we could optionally
    // also clear `backoffUntil` at that point
  }

  // FIXME: replace `isRecommended` with `_isRecommended` and delete
  // `isRecommended`
  _isRecommended() {
    return this._peer.recommended;
  }
  async isRecommended() {
    const {backoffUntil, cursor} = await this.getStatus();
    if(backoffUntil > Date.now()) {
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
    const status = await this.getStatus();
    const {backoff: backoffConfig} =
      config['ledger-consensus-continuity'].gossip;
    // FIXME: re-enable scaling of backoff; perhaps track consecutive failures
    // in status or last successful pull to do so
    /*status.backoffUntil = status.backoff ?
      status.backoff * backoffConfig.factor : backoffConfig.min;*/
    status.backoffUntil = Date.now() + backoffConfig.min;
    status.lastPullAt = Date.now();
    status.lastPullResult = error.toString();
    // set cursor if specified
    if(cursor !== undefined) {
      status.cursor = status.cursor;
    }

    // FIXME: consider conditions required for deleting the peer entirely
    // ... need to consider peers that notify to DoS and would keep getting
    // put back into the database and deleted again; these need to get
    // backed off quickly and stay backed off for a long time before being
    // deleted -- so process perhaps is:
    // 1. backoff for failure, increasing backoff time based on consecutive
    //    failures or last success time threshold; each consecutive failure
    //    should increase the backoff time to talk to the peer again
    // 2. once consecutive failures reaches a threshold, delete the peer
    // 3. peer gets inserted again -- and process repeats itself, but the
    //    peer should only be inserted every couple of days such that the
    //    damage ends up being just a few requests per day; but we don't want
    //    to allow the peer database to fill up so we have to protect against
    //    that as well -- or we need a separate block list/some basis for
    //    determining how many peers we allow in the block list
    // 4. Also, we may want to consider the age of a peer in making the above
    //    determinations

    // FIXME: update peer information in mongo peers collection
    const {worker: {ledgerNode}} = this;
    const peer = {...this._peer, status: {...status}};
    if(cursor) {
      // FIXME: rename `cursor.basisBlockHeight` to `cursor.requiredBlockHeight`
      peer.status.requiredBlockHeight = cursor.basisBlockHeight;
    }
    //console.log('fail peer', peer);
    await ledgerNode.peers.update({peer});
    return;
  }

  async success({backoff = 0, cursor = null} = {}) {
    const status = await this.getStatus();
    status.backoffUntil = Date.now() + backoff;
    status.lastPullAt = Date.now();
    status.lastPullResult = 'success';
    status.cursor = cursor;

    // FIXME: update peer information in mongo peers collection
    const {worker: {ledgerNode}} = this;
    const peer = {...this._peer, status: {...status}};
    if(cursor) {
      // FIXME: rename `cursor.basisBlockHeight` to `cursor.requiredBlockHeight`
      peer.status.requiredBlockHeight = cursor.basisBlockHeight;
    }
    //console.log('success peer', peer);
    await ledgerNode.peers.update({peer});
    return;
  }
};
