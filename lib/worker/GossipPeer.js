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
      this._peer = {id, url: id, sequence: 0};
    }
    this.worker = worker;
    this.ledgerNodeId = worker.ledgerNode.id;
    this._deleted = false;
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
    if(this._peer.status) {
      return this._peer.status;
    }
    // FIXME: remove, initialization code entirely, `peer` information
    // must be passed into constructor
    const {worker: {ledgerNode}} = this;
    try {
      this._peer = await ledgerNode.peers.get({id: peerId});
      //console.log('_peer', this._peer);
      return this._peer.status;
    } catch(e) {
      if(e.name !== 'NotFoundError') {
        throw e;
      }
    }

    this._peer.status = {
      backoffUntil: 0,
      lastPullAt: 0,
      lastPullResult: 0,
      consecutiveFailures: 0,
      cursor: null,
      requiredBlockHeight: 0
    };
    // FIXME: remove, rely on peers to be inserted elsewhere
    const peer = {...this._peer, status: {...this._peer.status}};
    try {
      await ledgerNode.peers.add({peer});
    } catch(e) {
      if(e.name !== 'DuplicateError') {
        throw e;
      }
      return this.getStatus();
    }
    return this._peer.status;
  }

  async delete() {
    if(this._deleted) {
      return;
    }
    const {worker: {ledgerNode}} = this;
    try {
      await ledgerNode.peers.remove({id: this.id});
    } catch(e) {
      if(e.name !== 'NotFoundError') {
        throw e;
      }
    }
    this._deleted = true;
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

  isDeleted() {
    return this._deleted;
  }

  isNotifier() {
    return this._peer.status.lastPushAt > this._peer.status.lastPullAt;
  }

  isWithheld() {
    return this.worker.isPeerWithheld({peerId: this.id});
  }

  async fail({error, cursor, fatal = false} = {}) {
    logger.error('Gossip peer failure.', {fatal, error});

    // if the error was fatal (a protocol violation), remove the peer entirely
    const {_peer, worker: {ledgerNode, consensusState: {witnesses}}} = this;
    if(fatal) {
      await this.delete();
      return;
    }

    // FIXME: use this._peer.status
    const status = await this.getStatus();
    const {backoff: backoffConfig} =
      config['ledger-consensus-continuity'].gossip;
    status.consecutiveFailures++;
    status.lastPullAt = Date.now();
    status.lastPullResult = error.toString();
    status.idle = null;

    // handle reputation updates
    if(status.consecutiveFailures === 1) {
      // store first failure stats and decrement reputation
      status.firstFailure = {
        reputation: _peer.reputation,
        time: status.lastPullAt
      };
      _peer.reputation--;
    } else {
      // FIXME: consider case where peer sends a notification and gets
      // backoff cleared -- so there will be more consecutive failures ...
      // should we check to see if the number of consecutive failures is
      // more than would normally occur from max backoff -- and if so,
      // decrease reputation more or delete the peer? Is this a threat?

      /* Another consecutive failure, compute the new reputation. The new
      reputation score is based on how long the peer has been detected as
      failing and the maximum grace period for a max-reputation peer (100).

      A peer with a reputation of `100` will have a reputation of `0` if it
      continues to have consecutive failures (no successes) for the
      `maxFailureGracePeriod`. Reputation decreases linearly over this period
      of time. */
      const {maxFailureGracePeriod} = backoffConfig;
      const totalFailTime = Date.now() - status.firstFailure.time;
      const points = Math.floor(totalFailTime / maxFailureGracePeriod) * 100;
      const {firstFailure: {reputation: startReputation}} = status;
      _peer.reputation = Math.min(
        startReputation - 1, startReputation - points);
    }

    if(_peer.reputation < 0) {
      const isWitness = witnesses.some(id => id === _peer.id);
      if(!isWitness) {
        // peer has no reputation, remove it
        await this.delete();
        return;
      }
      // do not delete peer if they are presently a witness, just force
      // their reputation to 0
      _peer.reputation = 0;
    }

    // determine next backoff
    const backoff = Math.min(
      backoffConfig.max, status.consecutiveFailures * backoffConfig.min);
    status.backoffUntil = Date.now() + backoff;

    // set cursor if specified
    if(cursor !== undefined) {
      status.cursor = status.cursor;
    }
    if(status.cursor) {
      // FIXME: rename `cursor.basisBlockHeight` to `cursor.requiredBlockHeight`
      status.requiredBlockHeight = status.cursor.basisBlockHeight;
    }

    // FIXME: update peer information in mongo peers collection
    this._peer.sequence++;
    const peer = {...this._peer, status: {...status}};
    //console.log('fail peer', peer);
    await ledgerNode.peers.update({peer});
    return;
  }

  async success({mergeEventsReceived, backoff = 0, cursor = null} = {}) {
    // if the peer has a reputation of `0`, it may be dropped...
    const {
      _peer,
      worker: {ledgerNode, consensusState: {blockHeight, witnesses}}
    } = this;
    const isWitness = witnesses.some(id => id === _peer.id);
    // do not delete peer if they are presently a witness
    if(!isWitness && _peer.reputation === 0) {
      /* Note: We have a maximum number of 110 peers that can be stored in
      the peers collection at any time. We have to assume that some number of
      the peers we persist are byzantine so we do not want to store too many
      for fear of degraded performance. If we don't store enough peers, then
      it may harm our ability to productively transmit merge events around a
      large network. We also need to allow for untrusted peers to onboard,
      which we have allocated 10 slots for (the smallest power of ten). We
      choose the next smallest power of 10 (100) to be the total target
      number of persistent peers. This means that the peers collection target
      capacity is 110 in total. */
      // FIXME: make max peers configurable?
      // if there is no room for the peer, drop it
      const peerCount = await ledgerNode.peers.count({maxReputation: 0});
      if(peerCount >= 100) {
        await this.delete();
        return;
      }
    }

    // FIXME: use `this._peer.status`
    const status = await this.getStatus();
    const {backoff: backoffConfig} =
      config['ledger-consensus-continuity'].gossip;
    status.backoffUntil = Date.now() + Math.min(backoff, backoffConfig.max);
    status.lastPullAt = Date.now();
    status.lastPullResult = 'success';
    status.cursor = cursor;
    if(cursor) {
      // FIXME: rename `cursor.basisBlockHeight` to `cursor.requiredBlockHeight`
      status.requiredBlockHeight = cursor.basisBlockHeight;
    }
    status.consecutiveFailures = 0;
    delete status.firstFailure;

    /* Note: It is important that peer reputation accounts not only for
    successful gossip sessions but for productivity. If a peer is consistently
    returning success during gossip but sending no new merge events, its
    reputation should decrease over time -- provided that other peers *are*
    sending merge events. In other words, if a peer consistently gives us
    no new merge event but other peers do, we should decrement the reputation
    of the peer that is not sending us merge events.

    How reputation is computed on successes:

    1. If merge events were received, increase reputation by 1 and clear
      `status.idle`.
    2. Otherwise, if there is no `status.idle`, set it to an object
      tracking the current time and local block height.
    3. Otherwise, see if the current local block height has changed from
      what is in `status.idle`.
    4. If not, update the time to the current time.
    5. If so, determine the number of whole reputation points that can be
      subtracted by dividing the `maxIdleGracePeriod` by 100 and multiplying by
      the time in `status.idle`. Set the new reputation and add the amount of
      time that corresponds to the whole number of reputation points to the
      `time` stored in `status.idle`. Update the block height and reputation
      stored in `status.idle`. */
    if(mergeEventsReceived) {
      // increase peer's reputation, it is not idle
      _peer.reputation = Math.min(100, _peer.reputation + 1);
      _peer.status.idle = null;
    } else if(!_peer.status.idle) {
      // peer just started idling
      _peer.status.idle = {
        time: Date.now(),
        localBlockHeight: blockHeight
      };
    } else if(_peer.status.idle.localBlockHeight === blockHeight) {
      // peer is idling but so are other peers, increase idle start time
      _peer.status.idle.time = Date.now();
    } else {
      // peer is idling but other peers are advancing, decrement reputation
      const {maxIdleGracePeriod} = backoffConfig;
      const totalIdleTime = Date.now() - status.idle.time;
      const timePerPoint = Math.ceil(maxIdleGracePeriod / 100);
      const points = Math.floor(totalIdleTime / timePerPoint);
      _peer.reputation -= points;
      status.idle.time += points * timePerPoint;
      status.idle.localBlockHeight = blockHeight;
      if(_peer.reputation < 0) {
        if(!isWitness) {
          await this.delete();
          return;
        }
        // do not delete peer if they are presently a witness, just force
        // their reputation to 0
        _peer.reputation = 0;
      }
    }

    // FIXME: update peer information in mongo peers collection
    this._peer.sequence++;
    const peer = {...this._peer, status: {...status}};
    //console.log('success peer', peer);
    await ledgerNode.peers.update({peer});
    return;
  }
};
