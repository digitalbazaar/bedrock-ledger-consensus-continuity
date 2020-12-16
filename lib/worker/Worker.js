/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _events = require('../events');
const _history = require('../history');
const _peers = require('../peers');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const {extend: extendBlockchain} = require('./blockchain');
const {merge} = require('./merge');
const logger = require('../logger');
const {runGossipCycle, sendNotification} = require('./gossip');
const {BedrockError} = bedrock.util;
const EventWriter = require('./EventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

module.exports = class Worker {
  constructor({session, halt = this._halt} = {}) {
    const {ledgerNode} = session;
    this.halt = halt;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.config = config['ledger-consensus-continuity'].writer;
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.eventWriter = new EventWriter({ledgerNode});
    // FIXME: move `creatorId` discovery inside peer selector
    // this.peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
    this.session = session;
    this.storage = ledgerNode.storage;
  }

  async run({targetCycles = -1} = {}) {
    const {eventWriter, ledgerNode, session} = this;
    let {halt} = this;
    const ledgerNodeId = ledgerNode.id;

    logger.verbose('ledger work session job running', {ledgerNodeId});

    // listen for new local operation notifications
    let resume = () => {};
    const subscriber = new cache.Client().client;
    try {
      await subscriber.subscribe(`continuity2017|needsMerge|${ledgerNodeId}`);
      subscriber.on('message', () => resume());
    } catch(e) {
      subscriber.quit();
      logger.verbose(
        'Work session failed, could not subscribe to new pub/sub messages.',
        {session: session.id});
      return;
    }

    try {
      /* This work session is designed with the assumption that any attempt to
      write to the database or the cache will be successful or an error will
      be thrown canceling the work session. This allows for the cache and
      database to be brought into sync at the start of the work session, without
      having to worry about them getting out of sync in the middle of it. It
      also provides a more simple model for reasoning about correctness and
      potential errors. */

      // ensure cache and mongo are in sync
      const creatorId = (await _peers.get({ledgerNodeId})).id;
      await _validateCache({ledgerNode, creatorId});

      // FIXME: move to state on Worker
      const peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
      const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
      let needsGossip = false;

      // commit any previously cached events to the database that could
      // not be written before because gossip timed out; this step ensures
      // valid pending events are always written
      if(!halt()) {
        await eventWriter.write();
      }

      // run consensus/gossip/merge pipeline until work session expires
      // or until the pipeline is run at least once per request
      const savedState = {};
      let cycles = 0;
      if(targetCycles > 0) {
        const _halt = halt;
        halt = () => _halt() || cycles >= targetCycles;
      }
      while(!halt()) {
        // 1. extend blockchain until can't anymore
        const {blocks, priorityPeers, witnesses, blockHeight} =
          await extendBlockchain({ledgerNode, savedState, halt});

        // work session expired
        if(halt()) {
          break;
        }

        // if blocks were created, reset `needsGossip`; to be set again by
        // `merge`
        if(blocks > 0) {
          needsGossip = false;
        }

        // 2. run gossip cycle; the gossip cycle runs an internal loop against
        // selections of peers and it will loop:
        //   until >= 1 merge events received, if `needsGossip=true`;
        //   once, if `needsGossip=false`
        const {mergeEventsReceived} = await runGossipCycle({
          ledgerNode, priorityPeers, creatorId, peerSelector,
          needsGossip, witnesses, blockHeight, halt
        });

        // work session expired
        if(halt()) {
          break;
        }

        // 3. commit all cached events to mongo
        await eventWriter.write();

        // 4. merge if possible
        const {merged, status: mergeStatus} = await merge({
          ledgerNode, creatorId, priorityPeers, witnesses,
          basisBlockHeight: blockHeight - 1, halt
        });
        // keep track of whether a merge would happen if more peer events were
        // received via gossip
        needsGossip = mergeStatus.needsGossip;

        // determine if peers need to be notified of new events
        let notify;
        if(merged || mergeEventsReceived) {
          await _cache.gossip.notifyFlag({add: true, ledgerNodeId});
          notify = true;
        } else {
          notify = (await _cache.gossip.notifyFlag({ledgerNodeId})) !== null;
        }

        if(notify) {
          // notify peers of new/previous merge event(s)
          try {
            // FIXME: notify more than just the `priorityPeers`
            await sendNotification({creatorId, priorityPeers, peerSelector});
          } catch(e) {
            // just log the error, another attempt will be made on the next
            // cycle
            logger.error(
              'An error occurred while attempting to send merge notification.',
              {error: e});
          }
        }

        // work session expired
        if(halt()) {
          break;
        }

        // if there are no outstanding operations (this includes
        // configurations) need to achieve consensus, then delay for cool down
        // period or until a peer notification or a local operation
        // notification comes in
        if(!mergeStatus.hasOutstandingOperations) {
          await new Promise(resolve => {
            resume = resolve;
            setTimeout(resolve, coolDownPeriod);
          });
          // FIXME: if, after cool down, there is still nothing to do, should
          // we end the work session and let the scheduler take it from there?
        }

        // track pipeline runs
        cycles++;
      }
    } finally {
      // unsubscribe from new operation messages
      subscriber.quit();
      logger.verbose('Work session completed.', {session: session.id});
    }
  }

  // default halt function
  _halt() {
    // expire session early, with 5 second buffer for overruns
    return this.session.timeRemaining() < 5000;
  }

  // /**
  //  * Adds an event received from a peer to this cache for later insertion into
  //  * persistent storage.
  //  *
  //  * @param event {Object} - The event to cache.
  //  * @param meta {Object} - The meta data for the event.
  //  *
  //  * @returns {Promise} resolves once the operation completes.
  //  */
  // async addPeerEvent({event, meta} = {}) {
  //   const {ledgerNodeId} = this;

  //   // FIXME: consider removing or revising this entirely, we may not need
  //   // to put anything in redis or it may just be stats (so it should become
  //   // _cache.stats in that case)
  //   await _cache.event.addPeerEvent({event, meta, ledgerNodeId});



  //   const {eventHash} = meta;
  //   const {creator: creatorId, generation, localAncestorGeneration, type} =
  //     meta.continuity2017;
  //   const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  //   const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  //   const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  //   const eventJson = JSON.stringify({event, meta});

  //   // perform update in a single atomic transaction
  //   const txn = cache.client.multi();
  //   if(type === 'r') {
  //     // Note: peer regular events have `operationRecords` not `operation` at
  //     // this point
  //     const opCountKey = _cacheKey.opCountPeer(
  //       {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  //     txn.incrby(opCountKey, event.operationRecords.length);
  //     txn.expire(opCountKey, operationsConfig.counter.ttl);
  //   }
  //   if(type === 'm') {
  //     const {basisBlockHeight, mergeHeight} = event;
  //     const latestPeerHeadKey = _cacheKey.latestPeerHead(
  //       {creatorId, ledgerNodeId});
  //     // expire the key in an hour, in case the peer/creator goes dark
  //     txn.hmset(
  //       latestPeerHeadKey,
  //       'h', eventHash,
  //       'g', generation,
  //       'bh', basisBlockHeight,
  //       'mh', mergeHeight,
  //       'la', localAncestorGeneration);
  //     txn.expire(latestPeerHeadKey, 3600);
  //   }
  //   // add the hash to the set used to check for dups and ancestors
  //   txn.sadd(eventQueueSetKey, eventHash);
  //   // create a key that contains the event and meta
  //   txn.set(eventKey, eventJson);
  //   // push to the list that is handled in the event-writer
  //   txn.rpush(eventQueueKey, eventKey);
  //   txn.publish(`continuity2017|peerEvent|${ledgerNodeId}`, 'new');
  //   return txn.exec();
  //   // TODO: abstract `publish` into some notify/check API on this file to keep
  //   // it isolated and easier to maintain
  // };

  // /**
  //  * Adds the summary information for a local merge event to the cache for later
  //  * processing by the consensus algorithm. Local merge events are already
  //  * present in storage before this method is called; this merely adds a summary
  //  * of their information to the cache so it can be pulled down with the rest
  //  * of "recent history" to be processed by the consensus algorithm.
  //  *
  //  * @param event {Object} - The event to cache.
  //  * @param meta {Object} - The meta data for the event.
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise} resolves once the operation completes.
  //  */
  // exports.addLocalMergeEvent = async ({event, meta, ledgerNodeId}) => {
  //   const childlessKey = _cacheKey.childless(ledgerNodeId);
  //   const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  //   const {basisBlockHeight, mergeHeight} = event;
  //   const {creator: creatorId, generation, localAncestorGeneration} =
  //     meta.continuity2017;
  //   const {eventHash} = meta;
  //   const headKey = _cacheKey.head({creatorId, ledgerNodeId});
  //   const outstandingMergeEventKey = _cacheKey.outstandingMergeEvent(
  //     {eventHash, ledgerNodeId});
  //   const eventGossipKey = _cacheKey.eventGossip({eventHash, ledgerNodeId});
  //   const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  //   const {parentHash, treeHash, type} = event;
  //   const parentHashes = parentHash.filter(h => h !== treeHash);

  //   // full event without meta goes into cache for gossip purposes
  //   const fullEvent = JSON.stringify({event});
  //   const metaString = JSON.stringify({meta});
  //   // for local merge events, only cache a summary of the event because that
  //   // is all that is needed for consensus to be computed
  //   const eventSummary = JSON.stringify({
  //     event: {parentHash, treeHash, type},
  //     meta: {eventHash, continuity2017: {creator: creatorId}}
  //   });
  //   try {
  //     const result = await cache.client.multi()
  //       .srem(childlessKey, parentHashes)
  //       .srem(localChildlessKey, parentHashes)
  //       // this key is removed when the event reaches consensus
  //       .set(outstandingMergeEventKey, eventSummary)
  //       // expire key which is used for gossip
  //       .hmset(eventGossipKey, 'event', fullEvent, 'meta', metaString)
  //       .expire(eventGossipKey, 600)
  //       .sadd(outstandingMergeKey, outstandingMergeEventKey)
  //       .hmset(
  //         headKey,
  //         'h', eventHash,
  //         'g', generation,
  //         'bh', basisBlockHeight,
  //         'mh', mergeHeight,
  //         'la', localAncestorGeneration)
  //       .exec();
  //     // result is inspected in unit tests
  //     return result;
  //   } catch(e) {
  //     // FIXME: fail gracefully
  //     // failure here means head information would be corrupt which
  //     // cannot be allowed
  //     logger.error('Could not set head.', {
  //       creatorId,
  //       // FIXME: fix when logger.error works properly
  //       err1: e,
  //       generation,
  //       basisBlockHeight,
  //       headKey,
  //       ledgerNodeId,
  //     });
  //     throw e;
  //   }
  // };

  // /**
  //  * Record that a new local regular event has been added that needs merging.
  //  *
  //  * @param eventHash {string} - The hash of the new local regular event.
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  * @param [isConfig=false] {Boolean} - `true` if the event is a
  //  *        `WebLedgerConfigurationEvent`.
  //  *
  //  * @returns {Promise} resolves once the operation completes.
  //  */
  // exports.addLocalRegularEvent = async (
  //   {eventHash, ledgerNodeId, isConfig = false}) => {
  //   // new local regular events are `childless` meaning that they have no events
  //   // that descend from them; they must be merged
  //   const childlessKey = _cacheKey.childless(ledgerNodeId);
  //   const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  //   const localRegularEventCountKey = _cacheKey.eventCountLocal(
  //     {ledgerNodeId, second: Math.round(Date.now() / 1000)});
  //   const multi = cache.client.multi()
  //     .sadd(childlessKey, eventHash)
  //     .sadd(localChildlessKey, eventHash)
  //     // this key is for stats gathering only; do not make the system depend
  //     // on it for proper functioning, it may get slightly out of sync due to
  //     // failures
  //     .incr(localRegularEventCountKey)
  //     .expire(localRegularEventCountKey, eventsConfig.counter.ttl);
  //   if(isConfig) {
  //     // must notify that a config needs merging
  //     multi.publish(`continuity2017|needsMerge|${ledgerNodeId}`, 'config');
  //   }
  //   return multi.exec();
  // };

  // /**
  //  * Get event hashes that have no children and are candidates to
  //  * be merged by the local node.
  //  *
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<string[]>} hashes for childless events.
  //  */
  // exports.getChildlessHashes = async ({ledgerNodeId}) => {
  //   const childlessKey = _cacheKey.childless(ledgerNodeId);
  //   const childlessHashes = await cache.client.smembers(childlessKey);
  //   return {childlessHashes};
  // };

  // /**
  //  * Get local event hashes that have no children and are candidates to
  //  * be merged by the local node.
  //  *
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<string[]>} hashes for childless events.
  //  */
  // exports.getLocalChildlessHashes = async ({ledgerNodeId}) => {
  //   const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  //   const localChildlessHashes = await cache.client.smembers(localChildlessKey);
  //   return {localChildlessHashes};
  // };

  // /**
  //  * Get events.
  //  *
  //  * @param eventHash {string|string[]} - The event hash(es) to get.
  //  * @param [includeMeta=false] {Boolean} - Include event meta data.
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<Object[]>} The events.
  //  */
  // exports.getEvents = async ({eventHash, includeMeta = false, ledgerNodeId}) => {
  //   eventHash = [].concat(eventHash);
  //   const fields = ['event'];
  //   if(includeMeta) {
  //     fields.push('meta');
  //   }
  //   const eventKeys = eventHash.map(eventHash =>
  //     _cacheKey.eventGossip({eventHash, ledgerNodeId}));
  //   const txn = cache.client.multi();
  //   for(const key of eventKeys) {
  //     txn.hmget(key, ...fields);
  //   }

  //   const result = await txn.exec();
  //   return result.map(r => {
  //     if(includeMeta) {
  //       const [event, meta] = r;
  //       return {event, meta};
  //     }
  //     const [event] = r;
  //     return {event};
  //   });
  // };

  // /**
  //  * Get the current merge status information. This status information includes:
  //  * - the hashes of any peer childless events (targets for merging ... to become
  //  *   parents of the next potential merge event).
  //  * - the hashes of any local childless events (targets for merging ... to
  //  *   become parents of the next potential merge event).
  //  *
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<Object>} The merge status info.
  //  */
  // exports.getMergeStatus = async ({ledgerNodeId}) => {
  //   // see if there are any childless events to be merged
  //   const childlessKey = _cacheKey.childless(ledgerNodeId);
  //   const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  //   const [
  //     peerChildlessHashes, localChildlessHashes
  //   ] = await cache.client.multi()
  //     .sdiff(childlessKey, localChildlessKey)
  //     .smembers(localChildlessKey)
  //     .exec();
  //   // FIXME: enabling the cache to report not just what is childless but
  //   // what is mergeable (especially if we pass in `witnesses`) would speed
  //   // up processing
  //   return {
  //     peerChildlessHashes,
  //     localChildlessHashes
  //   };
  // };

  // /**
  //  * Store an event and meta data for gossip purposes.
  //  *
  //  * @param event {Object} - The event.
  //  * @param eventHash {string} - The event hash.
  //  * @param [expire=600] {Number} - Expire the event after the specified ms.
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  * @param meta {Object} - The event meta data.
  //  *
  //  * @returns {Promise} resolves once the operation completes.
  //  */
  // exports.setEventGossip = async (
  //   {event, eventHash, expire = 600, ledgerNodeId, meta}) => {
  //   const eventKey = _cacheKey.eventGossip({eventHash, ledgerNodeId});
  //   const eventString = JSON.stringify({event});
  //   const metaString = JSON.stringify({meta});
  //   return cache.client.multi()
  //     .hmset(eventKey, 'event', eventString, 'meta', metaString)
  //     .expire(eventKey, expire)
  //     .exec();
  // };

  // /**
  //  * Compute the difference between the given event hashes and those that are
  //  * in the event cache.
  //  *
  //  * @param eventHashes {string[]} - The hashes of the events.
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<string[]>} The event hashes that are *not* in the cache.
  //  */
  // exports.difference = async ({eventHashes, ledgerNodeId}) => {
  //   if(eventHashes.length === 0) {
  //     return [];
  //   }
  //   // get a random key to temporarily store the results of the diff operation
  //   const diffKey = _cacheKey.diff(uuid());
  //   // get key for event queue
  //   const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);

  //   // TODO: this could be implemented as smembers as well and diff the hashes
  //   // as an array, if the eventQueueSetKey contains a large set, then the
  //   // existing implementation is good

  //   // Note: Here we use an atomic transaction that adds an entry to redis
  //   //   just to perform a diff and then removes it.
  //   // 1. Add `diffKey` with `eventHashes` array as the value.
  //   // 2. Run `sdiff` to diff that value with what is in the event queue
  //   //    for the ledger node (using key `eventQueueSetKey`).
  //   // 3. Delete the `diffKey` once we're done running the diff.
  //   //
  //   // the results of `sadd` is in result[0], `sdiff` is in result[1], so
  //   // we destructure result[1] into `notFound` (i.e. events not in the queue)
  //   const [, notFound] = await cache.client.multi()
  //     .sadd(diffKey, eventHashes)
  //     .sdiff(diffKey, eventQueueSetKey)
  //     .del(diffKey)
  //     .exec();
  //   return notFound;
  // };




  //   async write() {
  //     return this._clearQueue();
  //   }

  //   async _clearQueue() {
  //     let error;
  //     let creators = [];
  //     try {
  //       const rangeData = await cache.client.lrange(
  //         this.cacheKey.eventQueue, 0, -1);
  //       const rawEvents = await this._getEvents({rangeData});
  //       // not async
  //       const processedData = this._processEvents({rawEvents});
  //       ({creators} = await this._storeEvents({processedData, rangeData}));
  //     } catch(e) {
  //       error = e;
  //     }
  //     if(error && error.name !== 'AbortError') {
  //       logger.error(`Error in event writer: ${this.ledgerNodeId}`, {error});
  //     }
  //     return {creators};
  //   }

  //   async _getEvents({rangeData}) {
  //     if(!rangeData || rangeData.length === 0) {
  //       throw new BedrockError('Nothing to do.', 'AbortError');
  //     }
  //     const result = await cache.client.mget(rangeData);
  //     // TODO: use fastest looping algo
  //     // filter out nulls, nulls occur when an event was a duplicate
  //     const eventsJson = result.filter(r => r !== null);
  //     if(eventsJson.length === 0) {
  //       throw new BedrockError('Nothing to do.', 'AbortError');
  //     }
  //     // throw if there is a failure here because there is a serious problem
  //     return eventsJson.map(JSON.parse);
  //   }

  //   _processEvents({rawEvents}) {
  //     const now = Date.now();
  //     const events = [];
  //     const eventHashes = new Set();
  //     const _parentHashes = [];
  //     const headHashes = new Set();
  //     const creatorHeads = {};
  //     const operations = [];
  //     for(const {event, meta} of rawEvents) {
  //       const {eventHash} = meta;

  //       eventHashes.add(eventHash);
  //       _.defaults(meta, {created: now, updated: now});
  //       events.push({event, meta});

  //       if(event.type === 'WebLedgerOperationEvent') {
  //         operations.push(...event.operationRecords);
  //         delete event.operationRecords;
  //       }

  //       // build a list of treeHashes that are not included in this batch
  //       if(meta.continuity2017.type === 'm') {
  //         const {basisBlockHeight, mergeHeight} = event;
  //         const {creator: creatorId, generation, localAncestorGeneration} =
  //           meta.continuity2017;
  //         // capturing the *last* head for each creator
  //         creatorHeads[creatorId] = {
  //           eventHash,
  //           generation,
  //           basisBlockHeight,
  //           mergeHeight,
  //           localAncestorGeneration
  //         };
  //         headHashes.add(eventHash);
  //         _parentHashes.push(...event.parentHash);
  //       }
  //     }
  //     const parentHashes = _.uniq(_parentHashes);
  //     return {
  //       creatorHeads, eventHashes, events, headHashes, operations, parentHashes
  //     };
  //   }

  //   async _storeEvents({processedData, rangeData}) {
  //     const {
  //       creatorHeads, events, eventHashes, headHashes, operations, parentHashes
  //     } = processedData;
  //     if(operations.length !== 0) {
  //       logger.debug(`Attempting to store ${operations.length} operations.`);
  //       await this.storage.operations.addMany({operations});
  //     }
  //     logger.debug(`Attempting to store ${events.length} events.`);
  //     // retry on duplicate events until all events have been processed
  //     const storeEvents = await this.storage.events.addMany({events});
  //     logger.debug('Successfully stored events and operations.');
  //     const {ledgerNodeId} = this;
  //     const eventCountCacheKey = _cacheKey.eventCountPeer({
  //       ledgerNodeId,
  //       second: Math.round(Date.now() / 1000)
  //     });
  //     const newHeadCreators = new Set();
  //     const multi = cache.client.multi();
  //     multi.incrby(eventCountCacheKey, events.length);
  //     multi.expire(eventCountCacheKey, this.eventsConfig.counter.ttl);
  //     // remove items from the list
  //     // ltrim *keeps* items from start to end
  //     multi.ltrim(this.cacheKey.eventQueue, rangeData.length, -1);
  //     multi.srem(this.cacheKey.eventQueueSet, Array.from(eventHashes));
  //     // update heads
  //     const creators = Object.keys(creatorHeads);
  //     // contains the current cache keys for all new heads
  //     const currentKeysForNewHeads = new Set();
  //     // contains new cache keys for all new heads
  //     const newHeadKeys = new Set();
  //     if(creators.length !== 0) {
  //       const {dupHashes} = storeEvents;
  //       const dupSet = new Set(dupHashes);
  //       // used to identify childless events
  //       const hashFilter = new Set(parentHashes.concat(dupHashes));
  //       const newHeads = [];
  //       // update the key that contains a hash of eventHash and generation
  //       creators.forEach(creatorId => {
  //         const headKey = _cacheKey.head({creatorId, ledgerNodeId});
  //         const {
  //           eventHash, generation, basisBlockHeight, mergeHeight,
  //           localAncestorGeneration
  //         } = creatorHeads[creatorId];
  //         multi.hmset(
  //           headKey,
  //           'h', eventHash,
  //           'g', generation,
  //           'bh', basisBlockHeight,
  //           'mh', mergeHeight,
  //           'la', localAncestorGeneration);
  //         if(!hashFilter.has(eventHash)) {
  //           newHeads.push(eventHash);
  //           newHeadCreators.add(creatorId);
  //         }
  //       });
  //       for(const eventHash of headHashes) {
  //         if(dupSet.has(eventHash)) {
  //           continue;
  //         }
  //         const currentKeyForNewHead = _cacheKey.event({eventHash, ledgerNodeId});
  //         const newHeadKey = _cacheKey.outstandingMergeEvent(
  //           {eventHash, ledgerNodeId});
  //         multi.rename(currentKeyForNewHead, newHeadKey);
  //         currentKeysForNewHeads.add(currentKeyForNewHead);
  //         newHeadKeys.add(newHeadKey);
  //       }
  //       // add new childless heads
  //       if(newHeads.length !== 0) {
  //         multi.sadd(this.cacheKey.childless, newHeads);
  //       }
  //     }
  //     // remove head keys so they will not be removed from the cache
  //     if(newHeadKeys.size !== 0) {
  //       // head keys have already been renamed, don't attempt to delete again
  //       _.pull(rangeData, ...currentKeysForNewHeads);
  //       multi.sadd(this.cacheKey.outstandingMerge, [...newHeadKeys]);
  //     }
  //     // TODO: maybe just mark these to expired because they could be used
  //     // for gossip
  //     // delete all the regular events from the cache, it's possible that
  //     // rangeData is empty if the only other items were merge events
  //     if(rangeData.length !== 0) {
  //       multi.del(rangeData);
  //     }
  //     // remove events with new children that have not yet been merged locally
  //     if(parentHashes.length !== 0) {
  //       multi.srem(this.cacheKey.childless, parentHashes);
  //     }
  //     // execute update
  //     await multi.exec();

  //     // return creators of all new events
  //     return {creators: [...newHeadCreators]};
  //   }
};

// FIXME: remove me
async function _validateCache({ledgerNode, creatorId}) {
  /* Note: Ensure that childless events cache is proper, a previous work
  session may have terminated and failed to update the cache; this cache
  absolutely MUST NOT be corrupt in order for the work session operation
  to function properly, a corrupt cache here may result in loss of
  operations/regular events or invalidation as a properly operating peer.
  It should also be noted that the local regular event count in the cache
  may not be properly synced when this happens, but that key is only used
  for statistics gathering purposes and has a short expiration anyway. */
  await _cache.prime.primeChildlessEvents({ledgerNode});

  // ensure the cache head for this ledger node is in sync w/database
  const [cacheHead, mongoHead] = await Promise.all([
    _history.getHead({creatorId, ledgerNode}),
    _history.getHead({creatorId, ledgerNode, useCache: false})
  ]);
  if(_.isEqual(cacheHead, mongoHead)) {
    // success
    return;
  }
  // this should never happen and requires intervention to determine if
  // it can be repaired
  if((mongoHead.generation - cacheHead.generation) !== 1) {
    const ledgerNodeId = ledgerNode.id;
    throw new BedrockError(
      'Critical error: The cache is behind by more than one merge event.',
      'InvalidStateError',
      {cacheHead, mongoHead, ledgerNodeId});
  }
  const {eventHash} = mongoHead;
  await _events.repairCache({eventHash, ledgerNode});
}
