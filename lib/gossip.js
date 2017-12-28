/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */

'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const _client = require('./client');
const database = require('bedrock-mongodb');
const _events = require('./events');
const logger = require('./logger');
const _voters = require('./voters');
const _signature = require('./signature');
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
  console.log('Start _gossipWith');
  const eventsCollection = ledgerNode.storage.events.collection;
  async.auto({
    creator: callback => _voters.get(ledgerNode.id, callback),
    genesisMergeHash: callback =>
      ledgerNode.consensus._worker._events.getGenesisMergeHash(
        {eventsCollection}, callback),
    // FIXME: use an API
    allCreators: callback => database.collections.continuity2017_key.find(
      {ledgerNodeId: ledgerNode.id}, {_id: 0, 'publicKey.owner': 1}
    ).toArray((err, result) =>
      err ? callback(err) : callback(null, result.map(k => k.publicKey.owner))),
    creatorHeads: ['allCreators', (results, callback) => {
      if(!results.allCreators.includes(peerId)) {
        // FIXME: can this be optimized to just set head to genesisMergeHash?
        results.allCreators.push(peerId);
      }
      const creatorHeads = {};
      async.each(results.allCreators, (creatorId, callback) =>
        _events._getLocalBranchHead(
          {eventsCollection, creator: creatorId}, (err, result) => {
            if(err) {
              return callback(err);
            }
            creatorHeads[creatorId] = result;
            callback();
          }), err => err ? callback(err) : callback(null, creatorHeads));
    }],
    // TODO: revise dep names to match new functionality, not just history
    peerHistory: ['creatorHeads', (results, callback) =>
      _client.getHistory(
        {creatorHeads: results.creatorHeads, peerId}, callback)],
    getEvents: ['peerHistory', (results, callback) => _gossipEvents({
      eventHashes: results.peerHistory.history, ledgerNode, peerId
    }, callback)],
    // localPeerHistory: ['creatorHeads', (results, callback) =>
    //   events.aggregateHistory({
    //     eventTypeFilter: 'ContinuityMergeEvent',
    //     ledgerNode,
    //     startHash: results.creatorHeads[peerId]
    //   }, callback)],
    // FIXME: this query should be optimized
    creatorFilter: ['creator', 'peerHistory', (results, callback) =>
      // removes all existing heads from creatorHead map (does not mutate)
      _events.removeExistingHeads({
        creatorHeads: results.peerHistory.creatorHeads,
        creatorId: results.creator.id,
        eventsCollection: ledgerNode.storage.events.collection,
      }, callback)],
    // FIXME: necessary to wait for getEvents?
    localHistory: ['getEvents', 'genesisMergeHash', 'creatorFilter',
      (results, callback) => {
        // preserving original for testing purposes
        const peerCreatorHeads = bedrock.util.clone(
          results.peerHistory.creatorHeads);
        const creatorId = results.creator.id;
        const treeHash = peerCreatorHeads[creatorId];
        results.allCreators.forEach(c => {
          if(!peerCreatorHeads[c]) {
            peerCreatorHeads[c] = results.genesisMergeHash;
          }
        });
        // exclusion for the local node is handled via treeHash
        delete peerCreatorHeads[creatorId];
        // remove heads found in creatorFilter, exclude those by creator
        const creatorFilter = _.values(results.creatorFilter);
        creatorFilter.forEach(c => delete peerCreatorHeads[c]);
        const eventHashFilter = _.values(peerCreatorHeads);
        logger.debug(
          'Restricting history', {creatorFilter, eventHashFilter, treeHash});
        _events.getHistory({
          creatorFilter,
          eventHashFilter,
          ledgerNode,
          treeHash
        }, (err, result) => {
          logger.debug('gossipWith localHistory', {localHistory: result});
          // TODO: first implementation getHistory is designed to produce an
          // ordered list of regular events and merge events. The events in the
          // list should be transmitted to the peer serially.
          callback(err, result);
        });
      }],
    sendEvents: ['localHistory', (results, callback) => async.eachSeries(
      results.localHistory, (eventHash, callback) => async.auto({
        get: callback => ledgerNode.storage.events.get(eventHash, callback),
        send: ['get', (results, callback) => _client.sendEvent(
          {event: results.get.event, eventHash, peerId}, callback)]
      }, callback), callback)]
  }, (err, results) => {
    console.log('End sync _gossipWith', {
      duration: Date.now() - startTime
    });
    logger.debug('End sync _gossipWith', {
      duration: Date.now() - startTime
    });
    callback(err, results);
  });
};

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
