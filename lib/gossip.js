/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const _client = require('./client');
const database = require('bedrock-mongodb');
const _events = require('./events');
const logger = require('./logger');
const _voters = require('./voters');
const _signature = require('./signature');
const _storage = require('./storage');
const BedrockError = bedrock.util.BedrockError;

const api = {};
module.exports = api;

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
  async.auto({
    creator: callback => _voters.get(ledgerNode.id, callback),
    creatorHeads: callback => api.getCreatorHeads(
      {ledgerNode, peerId}, callback),
    peerHistory: ['creatorHeads', (results, callback) =>
      _client.getHistory(
        {creatorHeads: results.creatorHeads.heads, peerId}, callback)],
    getEvents: ['peerHistory', (results, callback) => _gossipEvents({
      eventHashes: results.peerHistory.history, ledgerNode, peerId
    }, callback)],
    // wait for get events because there will be new heads  for push gossip
    pushGossip: ['getEvents', (results, callback) => {
      callback(null, new Promise(resolve => {
        // FIXME: ignoring errors to prevent Promise.all from rejecting
        api.pushGossip({
          creatorHeads: results.peerHistory.creatorHeads,
          creatorId: results.creator.id,
          ledgerNode,
          peerId
        }, resolve);
      }));
    }]
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
        creator: creatorId
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        creatorHeads[creatorId] = result;
        callback();
      }), err => err ? callback(err) : callback(null, creatorHeads));
  }],
}, callback);

// used in server and for push gossip
api.partitionHistory = (
  {creatorHeads, creatorId, eventTypeFilter}, callback) => async.auto({
  // FIXME: voter and ledgerNode can be passed in some cases
  voter: callback => _storage.voters.get({voterId: creatorId}, callback),
  ledgerNode: ['voter', (results, callback) =>
    brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
  genesisMergeHash: ['ledgerNode', (results, callback) =>
    results.ledgerNode.consensus._worker._events.getGenesisMergeHash(
      {eventsCollection: results.ledgerNode.storage.events.collection},
      callback)],
  // FIXME: use an API
  creatorFilter: ['ledgerNode', (results, callback) =>
    // removes all existing heads from creatorHead map (does not mutate)
    // leave only heads that this node does not know about
    _events.removeExistingHeads({
      creatorHeads,
      creatorId,
      eventsCollection: results.ledgerNode.storage.events.collection,
    }, callback)],
  localHeads: ['ledgerNode', (results, callback) => api.getCreatorHeads({
    ledgerNode: results.ledgerNode, peerCreators: Object.keys(creatorHeads),
  }, callback)],
  history: ['localHeads', 'creatorFilter', 'genesisMergeHash',
    (results, callback) => {
      const ledgerNode = results.ledgerNode;
      // do not mutate creatorHeads that was passed in
      const _creatorHeads = bedrock.util.clone(creatorHeads);

      const startHash = results.localHeads.heads[creatorId];

      results.localHeads.localCreators.forEach(c => {
        if(!_creatorHeads[c]) {
          _creatorHeads[c] = results.genesisMergeHash;
        }
      });
      // delete _creatorHeads[creatorId];
      const creatorFilter = _.values(results.creatorFilter);
      creatorFilter.forEach(c => delete _creatorHeads[c]);
      // filter out treeHash
      const eventHashFilter = _.values(_creatorHeads);

      // TODO: turn eventHashFilter into a creator filter, no events for
      // the creator prior to the given hash
      async.auto({
        restriction: callback => async.map(eventHashFilter, (h, callback) => {
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
            // eventHashFilter,
            // TODO: server filters continuity merge event, push does not
            eventTypeFilter,
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

api.pushGossip = ({creatorHeads, creatorId, ledgerNode, peerId}, callback) =>
  async.auto({
    partitionHistory: callback => api.partitionHistory(
      {creatorHeads, creatorId}, callback),
    sendEvents: ['partitionHistory', (results, callback) => {
      logger.debug('Push gossiping', {
        destination: peerId,
        source: creatorId,
        events: results.partitionHistory.history
      });
      async.eachSeries(
        results.partitionHistory.history, (eventHash, callback) => {
          async.auto({
            get: callback => ledgerNode.storage.events.get(eventHash, callback),
            send: ['get', (results, callback) => _client.sendEvent(
              {event: results.get.event, eventHash, peerId}, callback)]
          }, callback);
        }, callback);
    }]
  }, callback);

// eventHashes contains a list of mergeEvent hashes available on the peer
// the merge events will contain a peerHash array which contains a list
// of other merge event hashes and regular event hashes. Regular events
// must be added to the collection before merge events that reference those
// regular events
function _gossipEvents({eventHashes, ledgerNode, peerId}, callback) {
  // this operation must be performed in series
  async.auto({
    difference: callback => ledgerNode.storage.events.difference(
      eventHashes, callback),
    events: ['difference', (results, callback) => async.eachSeries(
      results.difference, (eventHash, callback) => {
        async.auto({
          mergeEvent: callback => _client.getEvent(
            {eventHash, peerId}, callback),
          gossipMergeEvent: ['mergeEvent', (results, callback) => {
            _gossipMergeEvent(
              {mergeEvent: results.mergeEvent, ledgerNode, peerId}, callback);
          }]
        }, (err, results) => {
          if(err) {
            return callback(err);
          }
          callback();
        });
      }, callback)]
  }, callback);
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
    parentEvents: ['difference', (results, callback) =>
      async.map(results.difference, (eventHash, callback) =>
        _client.getEvent({eventHash, peerId}, callback), callback)],
    store: ['parentEvents', (results, callback) => {
      async.eachSeries(results.parentEvents, (event, callback) => {
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
        if(Array.isArray(event.type) &&
          event.type.includes('ContinuityMergeEvent')) {
          // console.log('-------------', event.signature.creator);
          // TODO: use process.nextTick() to avoid stack overflow?
          return _gossipMergeEvent(
            {mergeEvent: event, ledgerNode, peerId}, callback);
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
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback();
  });
}
