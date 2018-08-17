/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('../cache-key');
const _client = require('../client');
const _gossip = require('../gossip');
const _storage = require('../storage');
const _voters = require('../voters');
const async = require('async');
const bedrock = require('bedrock');
const {config} = bedrock;
const cache = require('bedrock-redis');
const logger = require('../logger');
const ContinuityAgent = require('./continuity-agent');

module.exports = class GossipAgent extends ContinuityAgent {
  constructor({agentName, ledgerNode}) {
    agentName = agentName || 'gossip';
    super({agentName, ledgerNode});
    // TODO: move into cacheKey
    this.peerListKey = _cacheKey.peerList(ledgerNode.id);
    this.eventQueueKey = _cacheKey.eventQueue(ledgerNode.id);
    this.creatorId = null;
    this.notificationKey;
    this.peerList = [];
    this.contactMap = new Map();
    this.cacheKeys = {
      gossipBehind: _cacheKey.gossipBehind(ledgerNode.id)
    };
  }

  _workLoop() {
    this.working = true;
    const ledgerNodeId = this.ledgerNode.id;
    async.auto({
      creator: callback => _voters.get({ledgerNodeId}, (err, result) => {
        if(err) {
          return callback(err);
        }
        this.creatorId = result.id;
        this.notificationKey = _cacheKey.gossipNotification(result.id);
        callback(null, result);
      }),
      // TODO: make scheduled job that reshuffles every N minutes/hours
      // TODO: peer information should track contact success/failure
      loadList: ['creator', (results, callback) => _storage.keys.getPeerIds(
        {creatorId: this.creatorId, ledgerNodeId}, (err, result) => {
          if(err) {
            return callback(err);
          }
          if(result.length === 0) {
            // do not return an error, because there may be notifications
            return callback();
          }
          // shuffle for sending notifications
          this.peerList = _.shuffle(result);
          callback();
        })],
      gossipCycle: ['loadList', (results, callback) =>
        this._gossipCycle(callback)]
    // quit must be called like this or `this` is not proper in ContinuityAgent
    }, err => {
      this.working = false;
      this._quit(err);
    });
  }

  _getAvailablePeer(callback) {
    const peers = this.peerList.map(peerId => ({
      peerId,
      timeToContact: this.contactMap.get(peerId) || 0,
    }));
    // find the peer that should be contacted next
    peers.sort((a, b) => a.timeToContact - b.timeToContact);
    const now = Date.now();
    const peer = peers[0];
    if(peer.timeToContact <= now) {
      return callback(null, peer.peerId);
    }
    setTimeout(() => callback(null, peer.peerId), peer.timeToContact - now);
  }

  _gw({peerId}, callback) {
    const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
    this.contactMap.set(peerId, Date.now() + coolDownPeriod);
    async.doWhilst(callback => async.auto({
      gossip: callback => _gossip.gossipWith(
        {ledgerNode: this.ledgerNode, peerId}, (err, result) => {
          if(err) {
            // if there is an error with one peer, do not stop cycle
            logger.debug('non-critical error in gossip', {err, peerId});
          }
          callback(null, result || {done: true, err});
        }),
      catchUp: ['gossip', (results, callback) => {
        const {done, err} = results.gossip;
        if(done && !err) {
          // clear gossipBehind
          return cache.client.del(this.cacheKeys.gossipBehind, callback);
        }
        // set gossipBehind
        cache.client.set(this.cacheKeys.gossipBehind, '', callback);
      }]
    }, callback), result => !(result.gossip.done || this.halt), callback);
  }

  _gossipCycle(callback) {
    async.until(() => this.halt, callback => async.auto({
      notification: callback => {
        cache.client.spop(this.notificationKey, callback);
      },
      pullNotification: ['notification', (results, callback) => {
        if(!results.notification) {
          return callback();
        }
        this._gw({peerId: results.notification}, callback);
      }],
      coolDown: ['pullNotification', (results, callback) => {
        if(this.peerList.length === 0 || this.halt) {
          return callback();
        }
        this._getAvailablePeer(callback);
      }],
      gossip: ['coolDown', (results, callback) => {
        if(!results.coolDown || this.halt) {
          return callback();
        }
        this._gw({peerId: results.coolDown}, callback);
      }]
    }, (err, results) => {
      if(err && err.name === 'AbortError') {
        // start the cycle over
        return callback();
      }
      if(err) {
        return callback(err);
      }
      if(!(results.notification && results.coolDown && this.halt)) {
        const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
        return setTimeout(() => callback(), coolDownPeriod);
      }
      callback();
    }), err => {
      callback(err);
    });
  }

  sendNotification(callback) {
    const peers = this.peerList.slice(0, 2);
    async.each(peers, (peerId, callback) => _client.notifyPeer(
      {callerId: this.creatorId, peerId}, callback), callback);
  }
};
