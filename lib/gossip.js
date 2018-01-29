/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _client = require('./client');
const _events = require('./events');
const _storage = require('./storage');
const _voters = require('./voters');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
// const database = require('bedrock-mongodb');
const logger = require('./logger');
// const BedrockError = bedrock.util.BedrockError;

const gossipConfig = config['ledger-consensus-continuity'].gossip;

const api = {};
module.exports = api;

api.contactElectors = ({
  condition, contacted = {}, creatorId, electors, ledgerNode, state
}, callback) => {
  // maximum number of peers to communicate with at once
  const limit =
    bedrock.config['ledger-consensus-continuity'].gossip.concurrentPeers;

  // TODO: track electors that are consistently hard to get in contact with
  // and avoid them

  // const isElector = electors.some(e => e.id === creatorId);

  // restrict electors to contact to 1/3 of total or limit
  // 1. Contact N electors at random
  // const toContact = _.shuffle(electors.filter(e => !(e.id in contacted)));
  // if(toContact.length === 0) {
  //   return callback();
  // }
  // logger.debug('ELECTORS TO BE CONTACTED', {toContact});
  const notContacted = electors.filter(e => !(e.id in contacted));
  // FIXME: need to proceed to do notifications
  // if(notContacted.length === 0) {
  //   // nothing to do
  //   return callback();
  // }
  const activeCacheKey = `ap-${creatorId}`;
  let activeCached = false;
  // const notifyCacheKey = `n-${creatorId.substr(-43)}`;
  const notifyCacheKey = _cacheKey.gossipNotification(creatorId.substr(-43));

  const toContact = _.shuffle(notContacted);
  const maxLength = Math.min(
    toContact.length, Math.ceil(electors.length / 3), limit);
  toContact.length = maxLength;

  // mark the peers as contacted
  toContact.forEach(p => contacted[p.id] = true);

  // FIXME: REMOVE, shortcircuiting gossip
  // state.pulledNotifications = true;
  // return callback();

  async.auto({
    sadd: callback => {
      return callback();
      // activeCached = true;
      // const activePeers = toContact.map(p => p.id);
      // cache.client.sadd(activeCacheKey, activePeers, () => callback());
    },
    notifications: callback =>
      cache.client.spop(notifyCacheKey, 2, (err, result) => {
        // FIXME: what happens in error condition
        state.pulledNotifications = true;
        if(err) {
          return callback(err);
        }
        callback(null, result);
      }),
    // TODO: expand the population to notify
    // only send notification if not an elector
    notify: ['sadd', (results, callback) => {
      async.eachLimit(toContact.map(p => p.id), limit,
        (peerId, callback) => _client.notifyPeer(
          {callerId: creatorId, peerId}, callback), callback);
    }],
    pull: ['notifications', (results, callback) => async.eachSeries([
      ...toContact.map(p => p.id),
      ...results.notifications
    ], (peerId, callback) => {
      // no need to gossip if condition met
      if(condition()) {
        return callback();
      }
      // 2. Gossip with a single elector.
      api.gossipWith(
        {callerId: creatorId, condition, ledgerNode, peerId}, err => {
          // ignore communication error from a single voter
          if(err) {
            logger.debug('Non-critical error in _gossipWith.', err);
          }
          callback();
        });
    }, callback)],
    srem: ['sadd', 'pull', 'notify', (results, callback) => {
      // NOTE: freezeCacheKey needs to be removed first
      async.series([
        // callback => cache.client.del(freezeCacheKey, () => callback()),
        callback => cache.client.del(activeCacheKey, () => callback()),
      ], callback);
    }]
  }, err => {
    // always clear the cache
    if(activeCached) {
      // do not wait for callback
      cache.client.del(activeCacheKey);
    }
    callback(err);
  });
};

// FIXME: update documentation
/**
 * Causes the given ledger node, identified by the given `voter` information,
 * to gossip with a `peer` about the next block identified by `blockHeight`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param previousBlockHash the hash of the most current block (the one to
 *          build on top of as the new previous block).
 * @param blockHeight the height of the next block (the one to gossip about).
 * @param peer the peer to gossip with.
 * @param callback(err) called once the operation completes.
 */
api.gossipWith = ({ledgerNode, peerId}, callback) => {
  logger.debug('Start gossipWith', {peerId});
  const startTime = Date.now();
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => _voters.get(ledgerNodeId, callback),
    creatorHeads: callback => _events.getCreatorHeads(
      {ledgerNode, peerId}, callback),
    newHeads: ['creatorHeads', (results, callback) => {
      const lastPeerHeadsKey = _cacheKey.lastPeerHeads(ledgerNodeId);
      cache.client.get(lastPeerHeadsKey, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return callback(null, results.creatorHeads.heads);
        }
        const headsFromLastPeer = JSON.parse(result);
        const localHeads = results.creatorHeads.heads;
        // use the head with the highest generation for each peer
        const combined = _.assignWith(
          localHeads, headsFromLastPeer, (sVal, tVal) => {
            if(tVal === undefined) {
              return sVal;
            }
            if(tVal.generation < sVal.generation) {
              return sVal;
            }
          });
        callback(null, combined);
      });
    }],
    history: ['creator', 'newHeads', (results, callback) =>
      _client.getCompressedHistory({
        callerId: results.creator.id,
        creatorHeads: results.newHeads,
        peerId
      }, callback)],
    process: ['history', (results, callback) => {
      _events.processCompressed({
        creatorId: results.creator.id, eventsGz: results.history.file
      }, callback);
    }],
    saveHeads: ['history', (results, callback) => {
      // TODO: might be useful to namespace heads by peer
      const creatorHeads = results.history.creatorHeads;
      const lastPeerHeadsKey = _cacheKey.lastPeerHeads(ledgerNodeId);
      cache.client.set(
        lastPeerHeadsKey, JSON.stringify(creatorHeads), callback);
    }]
  }, (err, results) => {
    logger.debug('End sync _gossipWith', {
      duration: Date.now() - startTime
    });
    if(err && _.get(err, 'details.httpStatusCode') === 404) {
      // peer has nothing to share
      return callback();
    }
    // results are used in unit tests, no need for history(gz)
    delete results.history;
    callback(err, results);
  });
};

// used in server to only get current heads
api.getHeads = ({creatorId}, callback) => {
  async.auto({
    voter: callback => _storage.voters.get({voterId: creatorId}, callback),
    ledgerNode: ['voter', (results, callback) =>
      brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
    localHeads: ['ledgerNode', (results, callback) => _events.getCreatorHeads({
      ledgerNode: results.ledgerNode
    }, callback)],
  }, (err, results) => err ? callback(err) :
    callback(null, results.localHeads.heads));
};

api.pollPeers = (
  {callerId, contacted, isElector, ledgerNode, peers}, callback) => {
  const peerHeads = {};
  async.auto({
    heads: callback => async.each(peers, (peer, callback) => {
      _client.getHistory(
        {callerId, headsOnly: true, peerId: peer.id}, (err, result) => {
          // TODO: is always saying that peer has been contacted optimal?
          contacted[peer.id] = true;
          // NOTE: if a session is in progress, a 503 AbortError is expected
          if(err) {
            logger.debug('Polling error', {
              peerId: peer.id,
              error: err
            });
            return callback();
          }
          peerHeads[peer.id] = result;
          callback();
        });
    }, () => callback(null, peerHeads)),
    top: ['heads', (results, callback) => {
      // get list of unique heads
      const allHeads = [];
      const eventMap = {};
      Object.keys(peerHeads).forEach(p => {
        allHeads.push(..._.values(peerHeads[p]));
        Object.keys(peerHeads[p]).forEach(h => {
          if(!eventMap[peerHeads[p][h]]) {
            eventMap[peerHeads[p][h]] = h;
          }
        });
      });
      // determine which heads this node does not know about
      ledgerNode.storage.events.difference(_.uniq(allHeads), (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.length === 0) {
          // nothing more to do
          return callback();
        }
        // build list of peers that have the events we need
        const needed = {};
        result.forEach(h => {
          needed[h] = [];
          Object.keys(peerHeads).forEach(p => {
            if(_.values(peerHeads[p]).includes(h)) {
              needed[h].push(p);
            }
          });
        });
        // iterate over needed and tally up events for each peer
        const peerTally = {};
        Object.keys(needed).forEach(h => {
          needed[h].forEach(p => {
            if(!peerTally[p]) {
              return peerTally[p] = 1;
            }
            peerTally[p]++;
          });
        });
        const peerArray = [];
        Object.keys(peerTally).forEach(id => {
          peerArray.push({id, tally: peerTally[id]});
        });
        // descending sort by tally
        peerArray.sort((a, b) => {
          return b.tally - a.tally;
        });
        // FIXME: what is ideal number of electors to contact?
        // NOTE: 2 seems to be better than 1 or 3
        if(isElector) {
          peerArray.length = Math.min(2, peerArray.length);
        } else {
          // do more pull gossip when not an elector
          peerArray.length = Math.min(4, peerArray.length);
        }
        // TODO: calculate which heads to give to each elector and pass on that
        // information ?
        peerArray.forEach(p => {
          p.heads = peerHeads[p.id];
        });
        const headsFromSelected = [];
        peerArray.forEach(p => headsFromSelected.push(..._.values(p.heads)));
        callback(null, peerArray);
      });
    }],
  }, callback);
};
