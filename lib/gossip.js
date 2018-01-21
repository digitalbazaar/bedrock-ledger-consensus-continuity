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
const database = require('bedrock-mongodb');
const jsonld = bedrock.jsonld;
const logger = require('./logger');
const zlib = require('zlib');
const BedrockError = bedrock.util.BedrockError;

const cacheConfig = config['ledger-consensus-continuity'].gossip.cache;

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

  // not needed right now
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
  async.auto({
    select: callback => {
      if(cacheConfig.enabled) {
        const cacheKeyToElector = {};
        notContacted.forEach(p => {
          cacheKeyToElector[
            `r-${creatorId.substr(-43)}-${p.id.substr(-43)}`] = p;
        });
        const cacheKeys = Object.keys(cacheKeyToElector);
        return cache.client.mget(cacheKeys, (err, result) => {
          if(err) {
            return callback(err);
          }
          // map results to cached vs. not-cached electors
          result = result
            .map((r, i) => r ? cacheKeyToElector[cacheKeys[i]] : null);
          const cached = result.filter(r => r);
          const notCached = _.difference(notContacted, cached);

          // treat every cached elector as if they have been contacted this
          // cycle
          cached.forEach(p => contacted[p.id] = true);

          // use non-cached electors as potential contacts
          const toContact = _.shuffle(notCached);
          const maxLength = Math.min(
            toContact.length, Math.ceil(electors.length / 3), limit);
          toContact.length = maxLength;
          // console.log('TTT', creatorId.substr(-5), toContact.map(e => e.id));
          callback(null, toContact);
        });
      }
      const toContact = _.shuffle(notContacted);
      const maxLength = Math.min(
        toContact.length, Math.ceil(electors.length / 3), limit);
      toContact.length = maxLength;
      callback(null, toContact);
    },
    // NOTE: mutates contacted
    poll: ['select', (results, callback) => api.pollPeers(
      {callerId: creatorId, contacted, ledgerNode, peers: results.select},
      callback)],
    sadd: ['poll', (results, callback) => {
      if(!results.poll.top) {
        // nothing to do
        return callback();
      }
      activeCached = true;
      const activePeers = results.poll.top.map(p => p.id);
      cache.client.sadd(activeCacheKey, activePeers, () => callback());
    }],
    push: ['poll', (results, callback) => async.eachLimit(
      results.poll.top, limit, (peer, callback) => {
        const peerHeads = results.poll.heads[peer.id];
        if(!peerHeads) {
          // elector could not be contacted during polling, return early
          return callback();
        }
        api.pushGossip(
          {condition, creatorId, peerHeads, peerId: peer.id}, callback);
      }, callback)],
    pull: ['poll', (results, callback) => async.eachLimit(
      results.poll.top, limit, (elector, callback) => {
        // no need to gossip if condition met
        if(condition()) {
          return callback();
        }
        // 2. Gossip with a single elector.
        api.gossipWith({condition, ledgerNode, peerId: elector.id}, err => {
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
    creatorHeads: callback => api.getCreatorHeads(
      {ledgerNode, peerId}, callback),
    peerHistory: ['creator', 'creatorHeads', (results, callback) =>
      _client.getHistory({
        callerId: results.creator.id,
        creatorHeads: results.creatorHeads.heads,
        peerId
      }, callback)],
    getEvents: ['peerHistory', (results, callback) => _gossipEvents({
      condition, eventHashes: results.peerHistory.history, ledgerNode, peerId
    }, callback)],
  }, (err, results) => {
    logger.debug('End sync _gossipWith', {
      duration: Date.now() - startTime
    });
    callback(err, results);
  });
};

api.getCreatorHeads = (
  {ledgerNode, peerCreators, peerId}, callback) => async.auto({
  // get ids for all the creators known to the node
  localCreators: callback => database.collections.continuity2017_key.find(
    {ledgerNodeId: ledgerNode.id}, {_id: 0, 'publicKey.owner': 1}
  ).toArray((err, result) =>
    err ? callback(err) : callback(null, result.map(k => k.publicKey.owner))),
  heads: ['localCreators', (results, callback) => {
    // this should never happen, the node attempts to contact nodes based
    // on evens from blocks, and therefore the key should be in the cache
    // however, in tests, nodes are instructed to gossip with peers that
    // they have never heard of before, the head for this peer will end
    // up being the genesis merge event
    if(peerId && !results.localCreators.includes(peerId)) {
      // FIXME: can this be optimized to just set head to genesisMergeHash?
      results.localCreators.push(peerId);
    }
    const combinedCreators = peerCreators ?
      _.uniq([...peerCreators, ...results.localCreators]) :
      results.localCreators;
    // get the branch heads for every creator
    const creatorHeads = {};
    async.each(combinedCreators, (creatorId, callback) =>
      _events._getLocalBranchHead({
        eventsCollection: ledgerNode.storage.events.collection,
        creatorId,
        ledgerNodeId: ledgerNode.id
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        creatorHeads[creatorId] = result;
        callback();
      }), err => err ? callback(err) : callback(null, creatorHeads));
  }],
}, callback);

// used in server to only get current heads
api.getHeads = ({creatorId}, callback) => {
  async.auto({
    voter: callback => _storage.voters.get({voterId: creatorId}, callback),
    ledgerNode: ['voter', (results, callback) =>
      brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
    localHeads: ['ledgerNode', (results, callback) => api.getCreatorHeads({
      ledgerNode: results.ledgerNode
    }, callback)],
  }, (err, results) => err ? callback(err) :
    callback(null, results.localHeads.heads));
};

// used in server and for push gossip
api.partitionHistory = ({
  creatorHeads, creatorId, eventTypeFilter, fullEvent = false, peerId
}, callback) => {
  const _creatorHeads = bedrock.util.clone(creatorHeads);
  // NOTE: it is normal for creatorHeads not to include creatorId (this node)
  // if this node has never spoken to the peer before
  if(_creatorHeads[creatorId] &&
    _creatorHeads[creatorId].startsWith('urn:uuid:')) {
    // FIXME: this probably should not be happening in the first place, but
    // if it does, return early
    return callback(null, {history: []});
  }
  // *do not!* remove the local creator from the heads
  // delete _creatorHeads[creatorId];
  async.auto({
    // FIXME: voter and ledgerNode can be passed in some cases
    voter: callback => _storage.voters.get({voterId: creatorId}, callback),
    ledgerNode: ['voter', (results, callback) =>
      brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
    genesisMergeHash: ['ledgerNode', (results, callback) =>
      results.ledgerNode.consensus._worker._events.getGenesisMergeHash(
        {eventsCollection: results.ledgerNode.storage.events.collection},
        callback)],
    // FIXME: use an API
    creatorFilter: ['ledgerNode', (results, callback) => {
      // returns a map {<eventHash>: <creatorId>} which only contains
      // hashes for events that this node does not know about
      const creatorFilter = [];
      const _heads = bedrock.util.clone(creatorHeads);
      // no need to check local creator
      delete _heads[creatorId];

      // automatic filter on peerId
      creatorFilter.push(peerId);
      delete _heads[peerId];

      // urn:uuid: heads are being used to signal a creator filter
      Object.keys(_heads).forEach(c => {
        if(_heads[c].startsWith('urn:uuid:')) {
          creatorFilter.push(c);
          delete _heads[c];
        }
      });
      _events.headDifference({
        creatorHeads: _heads,
        eventsCollection: results.ledgerNode.storage.events.collection,
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        creatorFilter.push(..._.values(result));
        callback(null, creatorFilter);
      });
    }],
    localHeads: ['ledgerNode', (results, callback) => api.getCreatorHeads({
      ledgerNode: results.ledgerNode, peerCreators: Object.keys(creatorHeads),
    }, callback)],
    history: ['localHeads', 'creatorFilter', 'genesisMergeHash',
      (results, callback) => {
        const ledgerNode = results.ledgerNode;

        // aggregate search starts with the local head
        const startHash = results.localHeads.heads[creatorId];

        // if local node knows about a head that the peer does not, set head
        // to genesis for that creato
        // NOTE: if this is the first contact with the peer, the head
        // for the local node will get filled in here as well
        results.localHeads.localCreators.forEach(c => {
          if(!_creatorHeads[c]) {
            _creatorHeads[c] = results.genesisMergeHash;
          }
        });

        const creatorFilter = results.creatorFilter;
        // remove the creators in the filter from the list of heads
        creatorFilter.forEach(c => delete _creatorHeads[c]);

        const heads = _.values(_creatorHeads);
        async.auto({
          restriction: callback => async.map(heads, (h, callback) => {
            const projection = {
              'meta.continuity2017.creator': 1, 'meta.created': 1, _id: 0
            };
            results.ledgerNode.storage.events.collection.findOne(
              {eventHash: h}, projection, callback);
          }, (err, result) => {
            if(err) {
              return callback(err);
            }
            callback(null, result.map(e => ({
              creator: e.meta.continuity2017.creator, created: e.meta.created
            })));
          }),
          getHistory: ['restriction', (results, callback) =>
            _events.aggregateHistory({
              creatorFilter,
              creatorRestriction: results.restriction,
              creatorId,
              // NOTE: server filters continuity merge event, push does not
              eventTypeFilter,
              fullEvent,
              ledgerNode,
              startHash,
            }, callback)]
        }, (err, results) => err ? callback(err) :
          callback(null, results.getHistory));
      }],
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, {
      creatorHeads: results.localHeads.heads,
      history: results.history
    });
  });
};

api.pollPeers = ({callerId, contacted, ledgerNode, peers}, callback) => {
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
        if(peerArray.length > 2) {
          peerArray.length = 2;
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

api.pushGossip = ({condition, creatorId, peerHeads, peerId}, callback) => {
  async.auto({
    partitionHistory: callback => api.partitionHistory(
      {creatorHeads: peerHeads, creatorId, fullEvent: true, peerId}, callback),
    sendEvents: ['partitionHistory', (results, callback) => {
      const history = results.partitionHistory.history;
      logger.debug('Push gossiping', {
        destination: peerId,
        eventCount: history.length,
        source: creatorId,
      });

      const eventCount = history.length;
      // FIXME: make length configurable
      const compressionThreshold = 100;
      if(eventCount > compressionThreshold) {
        logger.debug('Compressing events for push gossip.', {
          compressionThreshold,
          eventCount,
          peerId
        });
        return _pushCompressed({
          events: history.map(h => ({event: h.event, eventHash: h.eventHash})),
          peerId,
        }, callback);
      }

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
        if(event.type === 'WebLedgerEvent') {
          // add the event
          return ledgerNode.events.add(
            event, {continuity2017: {peer: true}}, err => {
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
    storeMerge: ['store', (results, callback) => ledgerNode.events.add(
      mergeEvent, {continuity2017: {peer: true}}, err => {
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

function _pushCompressed({events, peerId}, callback) {
  // TODO: limit max size of history
  // TODO: use a custom zlib dictionary
  async.auto({
    compress: callback => zlib.gzip(JSON.stringify(events), callback),
    send: ['compress', (results, callback) => _client.sendCompressed({
      gzEvents: results.compress,
      peerId
    }, callback)]
  }, callback);
}
