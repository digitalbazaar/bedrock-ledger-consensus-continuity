/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
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
const {BedrockError} = bedrock.util;
const ContinuityAgent = require('./continuity-agent');

module.exports = class GossipAgent extends ContinuityAgent {
  constructor({agentName, ledgerNode}) {
    agentName = agentName || 'gossip';
    super({agentName, ledgerNode});
    this.peerListKey = _cacheKey.peerList(ledgerNode.id);
    this.eventQueueKey = _cacheKey.eventQueue(ledgerNode.id);
    this.creatorId = null;
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
            return callback();
          }
          cache.client.multi()
            // wipe the list
            .del(this.peerListKey)
            .rpush(this.peerListKey, _.shuffle(result))
            .exec(callback);
        })],
      gossipCycle: ['loadList', (results, callback) =>
        this._gossipCycle(callback)]
    // quit must be called like this or `this` is not proper in ContinuityAgent
    }, err => {
      this.working = false;
      this._quit(err);
    });
  }

  _gossipCycle(callback) {
    const creatorId = this.creatorId;
    async.until(() => this.halt, callback => async.auto({
      elector: callback =>
        cache.client.rpoplpush(this.peerListKey, this.peerListKey, callback),
      coolDown: ['elector', (results, callback) => {
        logger.debug('gossip coolDown elector result', {
          electorResult: results.elector
        });
        if(!results.elector) {
          return callback();
        }
        const creatorId = results.elector;
        const peerContactKey = _cacheKey.peerContact(
          {creatorId, ledgerNodeId: this.ledgerNode.id});
        const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
        cache.client.get(peerContactKey, (err, result) => {
          if(err) {
            return callback(err);
          }
          logger.debug('coolDown result', {result});
          // the key had expired, proceed
          if(result === null) {
            return cache.client.set(
              peerContactKey, '', 'EX', coolDownPeriod, err => {
                if(err) {
                  return callback(err);
                }
                callback(null, creatorId);
              });
          }
          // the key has not expired, do not contact
          callback();
        });
      }],
      notification: ['coolDown', (results, callback) => {
        const notifyCacheKey = _cacheKey.gossipNotification(creatorId);
        // pull notifications that are not from `elector`
        async.doWhilst(
          callback => cache.client.spop(notifyCacheKey, callback),
          result => result !== null && result === results.coolDown,
          callback);
      }],
      gossip: ['notification', (results, callback) => {
        const toContact = [];
        const {coolDown, notification} = results;
        // attempt to contact the notification peer first
        if(notification) {
          toContact.push(notification);
        }
        if(coolDown) {
          toContact.push(coolDown);
        }
        // this should quit gossip for the current work session
        if(toContact.length === 0) {
          return callback(new BedrockError('Nothing to do.', 'AbortError'));
        }
        logger.debug('Contacting peers:', {toContact});
        async.eachSeries(toContact, (peerId, callback) => async.doWhilst(
          callback => _gossip.gossipWith(
            {ledgerNode: this.ledgerNode, peerId}, (err, result) => {
              if(err) {
                // if there is an error with one peer, do not stop cycle
                logger.debug('non-critical error in gossip', {
                  err: err, peerId
                });
              }
              callback(null, result || {done: true});
            }), result => !(result.done || this.halt), callback), callback);
      }]
    }, callback), err => {
      // do not pass AbortError up the chain but we do want to stop for this
      // work session
      logger.debug('GossipCycle Complete', {error1: err});
      if(err && err.name === 'AbortError') {
        return callback();
      }
      callback(err);
    });
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

  // TODO: this might also be implemented as a pub/sub on queueKey
  _onQueueEmpty(callback) {
    if(this.halt) {
      return callback();
    }
    cache.client.llen(this.eventQueueKey, (err, queueDepth) => {
      if(err) {
        return callback(err);
      }
      // queueDepth is null on empty, redis deletes the key
      if(!queueDepth) {
        // queue is empty
        return callback();
      }
      logger.debug(`Queue is not empty. queueDepth: ${queueDepth}`);
      // BLPOP in eventWriter is effectively on a 1 sec interval
      setTimeout(() => this._onQueueEmpty(callback), 1000);
    });
  }
};
