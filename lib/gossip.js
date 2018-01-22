/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const _ = require('lodash');
const _client = require('./client');
const _events = require('./events');
const _signature = require('./signature');
const _storage = require('./storage');
const _voters = require('./voters');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
// const database = require('bedrock-mongodb');
const jsonld = bedrock.jsonld;
const logger = require('./logger');
const BedrockError = bedrock.util.BedrockError;

const gossipConfig = config['ledger-consensus-continuity'].gossip;

const api = {};
module.exports = api;

let invalidStateErrorCounter = 0;

api.contactElectors = ({
  condition, contacted = {}, creatorId, electors, ledgerNode
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
  if(notContacted.length === 0) {
    // nothing to do
    return callback();
  }
  const activeCacheKey = `ap-${creatorId}`;
  let activeCached = false;

  const toContact = _.shuffle(notContacted);
  const maxLength = Math.min(
    toContact.length, Math.ceil(electors.length / 3), limit);
  toContact.length = maxLength;

  // mark the peers as contacted
  toContact.forEach(p => contacted[p.id] = true);

  async.auto({
    // select: callback => {
    //   if(cacheConfig.enabled) {
    //     const cacheKeyToElector = {};
    //     notContacted.forEach(p => {
    //       cacheKeyToElector[
    //         `r-${creatorId.substr(-43)}-${p.id.substr(-43)}`] = p;
    //     });
    //     const cacheKeys = Object.keys(cacheKeyToElector);
    //     return cache.client.mget(cacheKeys, (err, result) => {
    //       if(err) {
    //         return callback(err);
    //       }
    //       // map results to cached vs. not-cached electors
    //       result = result
    //         .map((r, i) => r ? cacheKeyToElector[cacheKeys[i]] : null);
    //       const cached = result.filter(r => r);
    //       const notCached = _.difference(notContacted, cached);
    //
    //       // treat every cached elector as if they have been contacted this
    //       // cycle
    //       cached.forEach(p => contacted[p.id] = true);
    //
    //       // use non-cached electors as potential contacts
    //       const toContact = _.shuffle(notCached);
    //       const maxLength = Math.min(
    //         toContact.length, Math.ceil(electors.length / 3), limit);
    //       toContact.length = maxLength;
    //       // console.log('TTT', creatorId.substr(-5), toContact.map(e => e.id));
    //       callback(null, toContact);
    //     });
    //   }
    //   const toContact = _.shuffle(notContacted);
    //   const maxLength = Math.min(
    //     toContact.length, Math.ceil(electors.length / 3), limit);
    //   toContact.length = maxLength;
    //   callback(null, toContact);
    // },
    // // NOTE: mutates contacted
    // poll: ['select', (results, callback) => api.pollPeers({
    //   callerId: creatorId, contacted, isElector, ledgerNode,
    //   peers: results.select
    // }, callback)],
    // sadd: ['poll', (results, callback) => {
    //   if(!results.poll.top) {
    //     // nothing to do
    //     return callback();
    //   }
    //   activeCached = true;
    //   const activePeers = results.poll.top.map(p => p.id);
    //   cache.client.sadd(activeCacheKey, activePeers, () => callback());
    // }],
    sadd: callback => {
      activeCached = true;
      const activePeers = toContact.map(p => p.id);
      cache.client.sadd(activeCacheKey, activePeers, () => callback());
    },
    push: ['sadd', (results, callback) => async.eachLimit(
      toContact, limit, (peer, callback) => {
        api.pushGossip({condition, creatorId, peerId: peer.id}, callback);
      }, callback)],
    // pull in series to avoid pulling similar lists of thousands of events
    pull: ['sadd', (results, callback) => async.eachSeries(
      toContact, limit, (peer, callback) => {
        // no need to gossip if condition met
        if(condition()) {
          return callback();
        }
        // 2. Gossip with a single elector.
        api.gossipWith({condition, ledgerNode, peerId: peer.id}, err => {
          // ignore communication error from a single voter
          if(err) {
            logger.debug('Non-critical error in _gossipWith.', err);
          }
          callback();
        });
      }, callback)],
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
api.gossipWith = ({condition, ledgerNode, peerId}, callback) => {
  logger.debug('Start gossipWith', {peerId});
  const startTime = Date.now();
  async.auto({
    creator: callback => _voters.get(ledgerNode.id, callback),
    creatorHeads: callback => _events.getCreatorHeads(
      {ledgerNode, peerId}, callback),
    history: ['creator', 'creatorHeads', (results, callback) =>
      _client.getCompressedHistory({
        callerId: results.creator.id,
        creatorHeads: results.creatorHeads.heads,
        peerId
      }, callback)],
    process: ['history', (results, callback) => {
      _events.processCompressed(
        {creatorId: results.creator.id, eventsGz: results.history}, callback);
    }]
    // getEvents: ['peerHistory', (results, callback) => _gossipEvents({
    //   condition, eventHashes: results.peerHistory.history, ledgerNode, peerId
    // }, callback)],
  }, err => {
    logger.debug('End sync _gossipWith', {
      duration: Date.now() - startTime
    });
    callback(err);
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

api.pushGossip = ({condition, creatorId, peerId}, callback) => {
  async.auto({
    peerHeads: callback => _client.getHistory(
      {callerId: creatorId, headsOnly: true, peerId}, callback),
    partitionHistory: ['peerHeads', (results, callback) =>
      _events.partitionHistory({
        creatorHeads: results.peerHeads, creatorId, fullEvent: true,
        peerId
      }, callback)],
    sendEvents: ['partitionHistory', (results, callback) => {
      const history = results.partitionHistory.history;
      if(history.length === 0) {
        return callback(new BedrockError(
          'No events match the query.',
          'AbortError', {
            httpStatusCode: 404,
            public: true,
          }));
      }
      logger.debug('Push gossiping', {
        destination: peerId,
        eventCount: history.length,
        source: creatorId,
      });

      if(history.length > gossipConfig.compression.threshold) {
        logger.debug('Compressing events for push gossip.', {peerId});
        const events =
          history.map(h => ({event: h.event, eventHash: h.eventHash}));
        const maxEvents = gossipConfig.compression.maxEvents;
        return async.auto({
          compress: callback => _events.compressEvents(
            {events, maxEvents}, callback),
          send: ['compress', (results, callback) => _client.sendCompressed({
            gzEvents: results.compress,
            peerId
          }, callback)]
        }, callback);
      }

      // NOTE: history is processed in opposite order from compressed push
      history.reverse();
      // bundle events with their merge hashes
      const mergeBundles = [];
      let currentMergeBundle = {};
      for(let i = 0; i < history.length; ++i) {
        if(jsonld.hasValue(history[i].event, 'type', 'ContinuityMergeEvent')) {
          // setup a property for the mergeEvent hash to contain all the events
          // the the merge has itself as the first member
          currentMergeBundle = {
            mergeHash: history[i].eventHash,
            depth: history[i]._depth,
            events: [history[i]]
          };
          mergeBundles.unshift(currentMergeBundle);
          for(let p = 0; p < history[i].event.parentHash.length; ++p) {
            const hash = history[i].event.parentHash[p];
            const record = _.find(history, e => {
              return e.eventHash === hash && !jsonld.hasValue(
                e.event, 'type', 'ContinuityMergeEvent');
            }, i + 1); // fromIndex current mergeEvent + 1
            if(record) {
              currentMergeBundle.events.unshift(record);
            }
          }
        }
      }

      async.eachOfSeries(mergeBundles, (bundle, bundleNum, callback) => {
        if(condition()) {
          return callback(new BedrockError(
            'Session timed out during push gossip.',
            'AbortError', {public: true}));
        }
        async.eachSeries(
          bundle.events, (eventRecord, callback) => {
            _client.sendEvent({
              callerId: creatorId.substr(-43),
              event: eventRecord.event,
              eventHash: eventRecord.eventHash,
              mergeHash: bundle.mergeHash,
              peerId
            }, err => callback(err));
          }, err => {
            if(err && err.name === 'DuplicateError') {
              // server has signaled to continue to send next bundle
              logger.debug(
                `dup in bundle ${bundleNum + 1} of ${mergeBundles.length}`);
              return callback();
            }

            // FIXME: there are rare instances when the duplicate detection
            // algorithm results in an event being sent that produces an
            // InvalidStateError on the server that indicates events from
            // `parentHash` are missing
            if(err && err.name === 'NetworkError' &&
              _.get(err, 'details.error.type') === 'InvalidStateError') {
              invalidStateErrorCounter++;
              // NOTE: this accumulator is per worker
              logger.debug('FIXME: corner case causing InvalidStateError.', {
                invalidStateErrorCounter
              });
              return callback();
            }

            err ? callback(err) : callback();
          });
      }, callback);
    }]
  }, err => {
    // do not pass AbortError up the chain
    if(err && err.name !== 'AbortError') {
      return callback(err);
    }
    callback();
  });
};

// eventHashes contains a list of mergeEvent hashes available on the peer
// the merge events will contain a peerHash array which contains a list
// of other merge event hashes and regular event hashes. Regular events
// must be added to the collection before merge events that reference those
// regular events
function _gossipEvents({condition, eventHashes, ledgerNode, peerId}, callback) {
  // this operation must be performed in series
  async.auto({
    difference: callback => ledgerNode.storage.events.difference(
      eventHashes, callback),
    events: ['difference', (results, callback) => async.eachSeries(
      results.difference, (eventHash, callback) => {
        if(condition()) {
          return callback(new BedrockError(
            'Session timed out during pull gossip.',
            'AbortError', {public: true}));
        }
        // do a more current existence check before downloading
        ledgerNode.storage.events.exists(eventHash, (err, result) => {
          if(err) {
            return callback(err);
          }
          if(result) {
            // skip this event
            return callback();
          }
          async.auto({
            mergeEvent: callback => _client.getEvent(
              {eventHash, peerId}, callback),
            gossipMergeEvent: ['mergeEvent', (results, callback) => {
              _gossipMergeEvent(
                {mergeEvent: results.mergeEvent, ledgerNode, peerId}, callback);
            }]
          }, err => callback(err));
        });
      }, callback)]
  }, err => {
    if(err && err.name === 'AbortError') {
      logger.debug(err.message);
      return callback();
    }
    callback(err);
  });
}

function _gossipMergeEvent({mergeEvent, ledgerNode, peerId}, callback) {
  // this operation must be performed in series
  async.auto({
    signature: callback => _signature.verify(
      {doc: mergeEvent, ledgerNodeId: ledgerNode.id}, callback),
    // retrieve all parent events in parallel
    difference: ['signature', (results, callback) =>
      ledgerNode.storage.events.difference(
        mergeEvent.parentHash, callback)],
    parentEvents: ['difference', (results, callback) => async.mapLimit(
      results.difference,
      config['ledger-consensus-continuity'].gossip.requestPool.maxSockets,
      (eventHash, callback) =>
        _client.getEvent({eventHash, peerId}, callback), callback)],
    store: ['parentEvents', (results, callback) => {
      // FIXME: make limit configurable
      async.eachLimit(results.parentEvents, 10, (event, callback) => {
        if(event.type === 'WebLedgerOperationEvent') {
          // add the event
          return ledgerNode.consensus._events.add(
            event, ledgerNode, {continuity2017: {peer: true}}, err => {
              if(err && err.name === 'DuplicateError') {
                err = null;
              }
              callback(err);
            });
        }
        if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
          return callback();
          // FIXME: remove, no need to recurse here since the merge event
          // is in the history already and will be processed later
          // TODO: use process.nextTick() to avoid stack overflow?
          // return _gossipMergeEvent(
          //   {mergeEvent: event, ledgerNode, peerId}, callback);
        }
        callback(new BedrockError(
          'Unknown event type.', 'DataError', {event}));
      }, callback);
    }],
    // store the initial merge event
    storeMerge: ['store', (results, callback) =>
      ledgerNode.consensus._events.add(
        mergeEvent, ledgerNode, {continuity2017: {peer: true}}, err => {
          if(err && err.name === 'DuplicateError') {
            err = null;
          }
          callback(err);
        })]
  }, err => {
    if(err) {
      return callback(err);
    }
    callback();
  });
}
