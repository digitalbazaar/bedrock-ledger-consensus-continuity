/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _client = require('./client');
const _gossip = require('./gossip');
const _storage = require('./storage');
const _voters = require('./voters');
const async = require('async');
const cache = require('bedrock-redis');
const logger = require('./logger');

module.exports = class GossipAgent {
  constructor({ledgerNode}) {
    this.ledgerNode = ledgerNode;
    this.halt = false;
    this.peerListKey = _cacheKey.peerList(ledgerNode.id);
    this.onQuit = null;
    this.eventQueueKey = _cacheKey.eventQueue(ledgerNode.id);
    this.creatorId = null;
  }

  /*
    elector rotation list
    an elector failure key

    get all voters and vet them based on some process
    get list of all notifications
    pick one from each list to gossip with this iteration
    keep going like this until halt
  */
  start() {
    const ledgerNodeId = this.ledgerNode.id;
    async.auto({
      creator: callback => _voters.get({ledgerNodeId}, (err, result) => {
        if(err) {
          return callback(err);
        }
        this.creatorId = result.id;
        callback(null, result);
      }),
      // move this to a scheduled job that reshuffles every N minutes/hours
      loadList: ['creator', (results, callback) => _storage.keys.getPeerIds(
        {creatorId: this.creatorId, ledgerNodeId}, (err, result) => {
          if(err) {
            return callback(err);
          }
          if(result.length === 0) {
            return callback();
          }
          cache.client.lpush(this.peerListKey, _.shuffle(result), callback);
        })],
      gossipCycle: ['loadList', (results, callback) =>
        this._gossipCycle({creatorId: this.creatorId}, callback)]
    }, err => {
      if(err) {
        console.log('Error in gossip agent:', err);
      }
      if(this.onQuit) {
        this.onQuit();
      }
    });
  }

  _gossipCycle({creatorId}, callback) {
    async.until(() => this.halt, callback => async.auto({
      elector: callback =>
        cache.client.rpoplpush(this.peerListKey, this.peerListKey, callback),
      notification: ['elector', (results, callback) => {
        const notifyCacheKey = _cacheKey.gossipNotification(creatorId);
        async.doWhilst(
          callback => cache.client.spop(notifyCacheKey, callback),
          result => result !== null && result === results.elector, callback);
      }],
      gossip: ['notification', (results, callback) => {
        const toContact = [];
        // attempt to contact the notification peer first
        if(results.notification) {
          toContact.push(results.notification);
        }
        if(results.elector) {
          toContact.push(results.elector);
        }
        logger.debug('Contacting peers:', {toContact});
        async.eachSeries(toContact, (peerId, callback) =>
          this._onQueueEmpty(() => _gossip.gossipWith(
            {ledgerNode: this.ledgerNode, peerId}, err => {
              logger.debug('non-critical error in gossip', {err: err});
              callback();
            })), callback);
      }]
    }, callback), callback);
  }

  sendNotification(callback) {
    async.auto({
      // send notifications to the last two peers that were pulled
      // in the lrange API `end` is included, this gets items 0, 1 starting at
      // offset at end of list
      peers: callback => cache.client.lrange(this.peerListKey, -2, 1, callback),
      notify: ['peers', (results, callback) => {
        if(!results.peers) {
          return callback();
        }
        async.each(results.peers, (peerId, callback) => _client.notifyPeer(
          {callerId: this.creatorId, peerId}, callback), callback);
      }]
    }, callback);
  }

  stop(callback) {
    if(!(callback && typeof callback === 'function')) {
      throw new TypeError('`callback` is required.');
    }
    this.onQuit = callback;
    this.halt = true;
  }

  // TODO: this might also be implemented as a pub/sub on queueKey
  _onQueueEmpty(callback) {
    cache.client.llen(this.eventQueueKey, (err, queueDepth) => {
      if(err) {
        return callback(err);
      }
      if(queueDepth === 0) {
        // queue is empty
        return callback();
      }
      setTimeout(() => this._onQueueEmpty(callback), 250);
    });
  }

  _quit() {
    logger.debug('Stopping gossip agent.');
    this.onQuit();
  }
};
