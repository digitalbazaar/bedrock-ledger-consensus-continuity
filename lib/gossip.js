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
const BedrockError = bedrock.util.BedrockError;
const uuid = require('uuid/v4');

const gossipConfig = config['ledger-consensus-continuity'].gossip;

const api = {};
module.exports = api;

// communicate the very latest heads to the peer
api.gossipWith = ({ledgerNode, peerId}, callback) => {
  logger.debug('Start gossipWith', {peerId});
  const startTime = Date.now();
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => _voters.get({ledgerNodeId}, callback),
    creatorHeads: callback => _events.getCreatorHeads(
      {latest: true, ledgerNode, peerId}, callback),
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
    needed: ['history', (results, callback) => {
      const {history} = results.history;
      const eventHashes = _.uniq(history.reduce(
        (a, b) => a.concat(b.event.parentHash), []));
      if(eventHashes.length === 0) {
        return callback(new BedrockError(
          'Nothing to do.', 'AbortError'));
      }
      _diff({eventHashes, ledgerNode}, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.length === 0) {
          return callback(new BedrockError(
            'Nothing to do.', 'AbortError'));
        }
        callback(null, result);
      });
    }],
    events: ['needed', (results, callback) => {
      const {history} = results.history;
      logger.debug('GOSSIP HISTORY LENGTH', {
        historyLength: history.length,
      });
      const ancestors = history.map(e => e.eventHash);
      const limit = gossipConfig.requestPool.maxSockets;
      async.timesLimit(history.length, limit, (i, callback) => {
        const neededEvents = new Set(results.needed);
        // remove ancestors from neededEvents, i not included
        ancestors.slice(0, i).forEach(a => neededEvents.delete(a));
        const mergeEvent = history[i];
        _gossipMergeEvent({mergeEvent, neededEvents, peerId}, callback);
      }, callback);
    }],
    process: ['events', (results, callback) => {
      const events = results.events.reduce((a, b) => a.concat(b), []);
      if(events.length === 0) {
        return callback();
      }
      logger.debug('GOSSIP PROCESSING EVENTS', {
        eventsLength: events.length,
      });
      _events.processEvents({events, ledgerNode}, callback);
    }],
    cacheInsert: ['process', (results, callback) => {
      const {history} = results.history;
      if(history.length === 0) {
        return callback();
      }
      // capture the last head for each creator
      const creatorHeads = {};
      history.forEach(e => {
        const {creator: creatorId, generation} = e.meta.continuity2017;
        creatorHeads[creatorId] = {eventHash: e.eventHash, generation};
      });
      const multi = cache.client.multi();
      Object.keys(creatorHeads).forEach(creatorId => {
        const latestPeerHeadKey = _cacheKey.latestPeerHead(
          {creatorId, ledgerNodeId});
        const {eventHash, generation} = creatorHeads[creatorId];
        // expired the key in an hour, incase the peer/creator goes dark
        multi.hmset(latestPeerHeadKey, 'h', eventHash, 'g', generation);
        multi.expire(latestPeerHeadKey, 3600);
      });
      multi.exec(callback);
    }],
  }, (err, results) => {
    logger.debug('End gossipWith', {
      duration: Date.now() - startTime
    });
    if(err && _.get(err, 'details.httpStatusCode') === 404) {
      // peer has nothing to share
      return callback();
    }
    results.done = !_.get(results, 'history.truncated', false);
    // results are used in unit tests, no need for history
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

function _diff({eventHashes, ledgerNode}, callback) {
  if(eventHashes.length === 0) {
    return callback(null, []);
  }
  const ledgerNodeId = ledgerNode.id;
  const manifestId = uuid();
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  // TODO: use different type of key?
  const manifestKey = _cacheKey.manifest({ledgerNodeId, manifestId});
  // TODO: this could be implemented as smembers as well and diff the hashes
  // as an array, if the eventQueueSetKey contains a large set, then the
  // existing implementation is good
  async.auto({
    redis: callback => cache.client.multi()
      .sadd(manifestKey, eventHashes)
      .sdiff(manifestKey, eventQueueSetKey)
      .del(manifestKey)
      .exec((err, result) => {
        if(err) {
          return callback(err);
        }
        // result of `sadd` is in result[0]
        const notFound = result[1];
        callback(null, notFound);
      }),
    mongo: ['redis', (results, callback) => ledgerNode.storage.events
      .difference(results.redis, callback)]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    return callback(null, results.mongo);
  });
}

function _gossipMergeEvent({mergeEvent, neededEvents, peerId}, callback) {
  // this operation must be performed in series
  // const filter = _.uniq([mergeEvent.event.treeHash, ...eventHashFilter]);
  const eventHash = mergeEvent.event.parentHash.filter(
    h => neededEvents.has(h));
  _client.getEvents({eventHash, peerId}, (err, events) => {
    if(err) {
      return callback(err);
    }
    events.push(mergeEvent);
    callback(null, events);
  });
}
