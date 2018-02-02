/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _client = require('./client');
const _events = require('./events');
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
  const limit = gossipConfig.concurrentPeers;

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
  // let activeCached = false;
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
      let done = false;
      async.until(() => done || condition(), callback => {
        _onQueueEmpty(ledgerNode.id, err => {
          if(err) {
            return callback(err);
          }
          api.gossipWith(
            {callerId: creatorId, condition, ledgerNode, peerId},
            (err, result) => {
              done = result.done;
              if(err) {
                // ignore communication error from a single voter
                logger.debug('Non-critical error in _gossipWith.', err);
                done = true;
              }
              callback();
            });
        });
      }, callback);
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
    // if(activeCached) {
    //   // do not wait for callback
    //   cache.client.del(activeCacheKey);
    // }
    callback(err);
  });
};

api.gossipWith = ({ledgerNode, peerId}, callback) => {
  logger.verbose('Start gossipWith', {peerId});
  const startTime = Date.now();
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => _voters.get({ledgerNodeId}, callback),
    creatorHeads: callback => _events.getCreatorHeads(
      {ledgerNode, peerId}, callback),
    // newHeads: ['creatorHeads', (results, callback) => {
    //   const lastPeerHeadsKey = _cacheKey.lastPeerHeads(ledgerNodeId);
    //   cache.client.get(lastPeerHeadsKey, (err, result) => {
    //     if(err) {
    //       return callback(err);
    //     }
    //     if(!result) {
    //       return callback(null, results.creatorHeads.heads);
    //     }
    //     const peerData = JSON.parse(result);
    //     const localHeads = results.creatorHeads.heads;
    //     localHeads[peerData.peerId] = peerData.creatorHeads[peerData.peerId];
    //     callback(null, localHeads);
    //   });
    // }],
    history: ['creator', 'creatorHeads', (results, callback) =>
      _client.getHistory({
        callerId: results.creator.id,
        creatorHeads: results.creatorHeads.heads,
        peerId
      }, callback)],
    events: ['history', (results, callback) => {
      const {history} = results.history;
      const ancestors = history.map(e => e.eventHash);
      const limit = gossipConfig.requestPool.maxSockets;
      async.timesLimit(history.length, limit, (i, callback) => {
        // i not included
        const eventHashFilter = ancestors.slice(0, i);
        const mergeEvent = history[i];
        _gossipMergeEvent({eventHashFilter, mergeEvent, peerId}, callback);
      }, callback);
    }],
    process: ['events', (results, callback) => {
      const events = results.events.reduce((a, b) => a.concat(b), []);
      if(events.length === 0) {
        return callback();
      }
      _events.processEvents({events, ledgerNode}, callback);
    }],
    saveHeads: ['process', (results, callback) => {
      // TODO: might be useful to namespace heads by peer
      const creatorHeads = results.history.creatorHeads;
      const lastPeerHeadsKey = _cacheKey.lastPeerHeads(ledgerNodeId);
      cache.client.set(
        lastPeerHeadsKey, JSON.stringify({peerId, creatorHeads}), callback);
    }]
  }, (err, results) => {
    logger.verbose('End sync _gossipWith', {
      duration: Date.now() - startTime
    });
    if(err && _.get(err, 'details.httpStatusCode') === 404) {
      // peer has nothing to share
      return callback();
    }
    results.done = !_.get(results, 'history.truncated', false);
    // results are used in unit tests, no need for history(gz)
    delete results.history;
    callback(err, results);
  });
};

api.gossipWithCompressed = ({ledgerNode, peerId}, callback) => {
  logger.debug('Start gossipWith', {peerId});
  const startTime = Date.now();
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => _voters.get({ledgerNodeId}, callback),
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
        const peerData = JSON.parse(result);
        const localHeads = results.creatorHeads.heads;
        localHeads[peerData.peerId] = peerData.creatorHeads[peerData.peerId];
        callback(null, localHeads);
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
        lastPeerHeadsKey, JSON.stringify({peerId, creatorHeads}), callback);
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
    ledgerNodeId: callback => _voters.getLedgerNodeId(creatorId, callback),
    ledgerNode: ['ledgerNodeId', (results, callback) =>
      brLedgerNode.get(null, results.ledgerNodeId, callback)],
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

function _gossipMergeEvent(
  {eventHashFilter = [], mergeEvent, peerId}, callback) {
  // this operation must be performed in series
  const filter = _.uniq([mergeEvent.event.treeHash, ...eventHashFilter]);
  const eventHash = mergeEvent.event.parentHash.filter(
    h => !filter.includes(h));
  _client.getEvents({eventHash, peerId}, (err, events) => {
    if(err) {
      return callback(err);
    }
    events.push(mergeEvent);
    return callback(null, events);
  });
}

function _onQueueEmpty(ledgerNodeId, callback) {
  const queueKey = _cacheKey.eventQueue(ledgerNodeId);
  cache.client.llen(queueKey, (err, queueDepth) => {
    if(err) {
      return callback(err);
    }
    if(queueDepth === 0) {
      // queue is empty
      return callback();
    }
    setTimeout(() => _onQueueEmpty(ledgerNodeId, callback), 250);
  });
}
