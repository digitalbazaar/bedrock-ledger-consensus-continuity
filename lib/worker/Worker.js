/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _cacheKey = require('../cache/cacheKey');
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
const PeerEventWriter = require('./PeerEventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

module.exports = class Worker {
  constructor({session, halt = this._halt} = {}) {
    const {ledgerNode} = session;
    this.config = config['ledger-consensus-continuity'].writer;
    this.creatorId = null;
    // local regular events that need to be included in the next merge event
    this.pendingLocalRegularEventHashes = new Set();
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.halt = halt;
    this.head = null;
    this.historyMap = new Map();
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.peerChildlessMap = new Map();
    this.peerEventWriter = new PeerEventWriter({worker: this, ledgerNode});
    this.peerSelector = null;
    this.session = session;
    this.storage = ledgerNode.storage;
  }

  async run({targetCycles = -1} = {}) {
    const {ledgerNode, session} = this;
    let {halt} = this;
    const ledgerNodeId = ledgerNode.id;

    logger.verbose('Ledger work session job running', {ledgerNodeId});

    // ensure the genesis block exists before running, if not, exit
    // immediately, there's nothing to do
    try {
      await ledgerNode.blocks.getGenesis();
    } catch(e) {
      if(e.name !== 'NotFoundError') {
        throw e;
      }
      logger.verbose(
        'Ledger work session exiting early; there is no genesis block ' +
        'for the ledger node.', {ledgerNodeId});
      return;
    }

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
      write to the database or other state will be successful or an error will
      be thrown canceling the work session. This allows for the database and
      any other state to be brought into sync at the start of the work session,
      without having to worry about them getting out of sync in the middle of
      it. It also provides a more simple model for reasoning about correctness
      and potential errors. Calling `_init` initializes the state based on
      what is in the database. */
      await this._init();

      const {creatorId, peerEventWriter, peerSelector} = this;
      const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
      let needsGossip = false;

      // commit any previously cached events to the database that could
      // not be written before because gossip timed out; this step ensures
      // valid pending events are always written
      if(!halt()) {
        await peerEventWriter.flush();
      }

      // run consensus/gossip/merge pipeline until work session expires
      // or until the pipeline is run at least once per request
      const savedState = {};
      let cycles = 0;
      if(targetCycles > 0) {
        const _halt = halt;
        this.halt = halt = () => _halt() || cycles >= targetCycles;
      }
      while(!halt()) {
        // 1. extend blockchain until can't anymore
        const {blocks, priorityPeers, witnesses, blockHeight} =
          await extendBlockchain({worker: this, savedState});

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
          worker: this, priorityPeers, needsGossip, witnesses, blockHeight
        });

        // work session expired
        if(halt()) {
          break;
        }

        // 3. commit all cached events to mongo
        await peerEventWriter.flush();

        // 4. merge if possible
        const {merged, status: mergeStatus} = await this._merge({
          priorityPeers, witnesses, basisBlockHeight: blockHeight - 1
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

  getRecentHistory() {
    const {historyMap} = this;
    const events = [];
    for(const eventSummary of historyMap.values()) {
      // shallow copy event summary to allow for modification within
      // continuity algorithm (to attach `._c` meta data)
      events.push({...eventSummary});
    }
    return {events};
  }

  /**
   * Adds a local merge event to storage.
   *
   * @param event {Object} - The event to store.
   * @param meta {Object} - The meta data for the event.
   *
   * @returns {Promise} resolves once the operation completes.
   */
  async _addLocalMergeEvent({event, meta} = {}) {
    const {
      historyMap, ledgerNodeId,
      pendingLocalRegularEventHashes, peerChildlessMap
    } = this;

    // add the event to storage
    const record = await this.storage.events.add({event, meta});

    // update the cache
    // FIXME: determine if this is needed anymore
    await _cache.events.addLocalMergeEvent({...record, ledgerNodeId});

    // update history, childless head tracking info, and local head
    const {basisBlockHeight, parentHash, mergeHeight, treeHash} = event;
    const {creator, generation, localAncestorGeneration} = meta.continuity2017;
    const {eventHash} = meta;
    historyMap.set(eventHash, {
      eventHash,
      event: {parentHash, treeHash},
      meta: {continuity2017: {creator}}
    });
    // remove parents from peer childless map
    for(const hash of parentHash) {
      peerChildlessMap.delete(hash);
    }
    // clear pending regular event set
    pendingLocalRegularEventHashes.clear();
    // set new head
    this.head = {
      eventHash,
      generation,
      basisBlockHeight,
      mergeHeight,
      localAncestorGeneration
    };

    return record;
  }

  async _addPeerEvents({events, dupHashes} = {}) {
    // update recent history
    const {historyMap} = this;
    for(const {event, meta} of events) {
      const {eventHash, continuity2017: {creator, type}} = meta;
      if(type === 'm') {
        const {parentHash, treeHash} = event;
        historyMap.set(eventHash, {
          eventHash,
          event: {parentHash, treeHash},
          meta: {continuity2017: {creator}}
        });
      }
    }

    // FIXME: try to remove this -- potentially just need to invalidate
    // the redis cache for any heads and allow them to get looked up again
    // ...however updating `peerChildlessMap` is currently intertwined
    await this._updateHeads({events, dupHashes});
  }

  // default halt function
  _halt() {
    // expire session early, with 5 second buffer for overruns
    if(this.session.timeRemaining) {
      return this.session.timeRemaining() < 5000;
    }
    // session runs indefinitely for testing
    return false;
  }

  async _init() {
    // initialize `creatorId`
    const {ledgerNode, ledgerNodeId} = this;
    const {id: creatorId} = await _peers.get({ledgerNodeId});
    this.creatorId = creatorId;

    // get current head from database and init gossip peer selector
    this.head = await _history.getHead(
      {creatorId, ledgerNode, useCache: false});
    this.peerSelector = new GossipPeerSelector({creatorId, ledgerNode});

    // FIXME: we should also be fetching "recent history" (all merge events
    // with `consensus=false`... and adding that to the worker state; determine
    // if we can use a single database call to get both childless events and
    // this information -- should be possible)

    // get recent merge history for continuity algorithm and childless event
    // information and initialize peer event writer
    const {historyMap, peerChildlessMap, pendingLocalRegularEventHashes} =
      await _getNonConsensusEvents({ledgerNode, creatorId, head: this.head});
    this.historyMap = historyMap;
    this.peerChildlessMap = peerChildlessMap;
    this.peerEventWriter = new PeerEventWriter({worker: this, ledgerNode});
    this.pendingLocalRegularEventHashes = pendingLocalRegularEventHashes;

    // FIXME: this will unnecessary once head and childless information
    // is moved to in-memory state entirely
    // ensure cache and mongo are in sync
    await _validateCache({ledgerNode, creatorId});
  }

  async _merge({
    priorityPeers = [], witnesses = [], basisBlockHeight,
    nonEmptyThreshold = 1, emptyThreshold
  }) {
    return merge({
      worker: this, priorityPeers, witnesses, basisBlockHeight,
      nonEmptyThreshold, emptyThreshold
    });
  }

  _removeConsensusEvents({eventHashes} = {}) {
    // update recent history
    const {historyMap} = this;
    for(const eventHash of eventHashes) {
      historyMap.delete(eventHash);
    }
  }

  async _updateHeads({events, dupHashes}) {
    const {peerChildlessMap, ledgerNodeId} = this;

    const _parentHashes = [];
    const headHashes = new Set();
    const creatorHeads = {};
    const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
    for(const {event, meta} of events) {
      const {eventHash} = meta;

      // build a list of treeHashes that are not included in this batch
      if(meta.continuity2017.type === 'm') {
        const {basisBlockHeight, mergeHeight} = event;
        const {creator: creatorId, generation, localAncestorGeneration} =
          meta.continuity2017;
        // capturing the *last* head for each creator
        creatorHeads[creatorId] = {
          eventHash,
          generation,
          basisBlockHeight,
          mergeHeight,
          localAncestorGeneration
        };
        headHashes.add(eventHash);
        _parentHashes.push(...event.parentHash);
      }
    }
    const parentHashes = [...new Set(_parentHashes)];

    const newHeadCreators = new Set();
    const multi = cache.client.multi();
    // update heads
    const creators = Object.keys(creatorHeads);
    // contains the current cache keys for all new heads
    const currentKeysForNewHeads = new Set();
    // contains new cache keys for all new heads
    const newHeadKeys = new Set();
    if(creators.length !== 0) {
      const dupSet = new Set(dupHashes);
      // used to identify childless events
      const hashFilter = new Set(parentHashes.concat(dupHashes));
      // update the key that contains a hash of eventHash and generation
      for(const creatorId of creators) {
        const headKey = _cacheKey.head({creatorId, ledgerNodeId});
        const {
          eventHash, generation, basisBlockHeight, mergeHeight,
          localAncestorGeneration
        } = creatorHeads[creatorId];
        multi.hmset(
          headKey,
          'h', eventHash,
          'g', generation,
          'bh', basisBlockHeight,
          'mh', mergeHeight,
          'la', localAncestorGeneration);
        if(!hashFilter.has(eventHash)) {
          newHeadCreators.add(creatorId);
          peerChildlessMap.set(eventHash, {
            creatorId,
            eventHash,
            generation,
            basisBlockHeight,
            mergeHeight,
            localAncestorGeneration
          });
        }
      }
      for(const eventHash of headHashes) {
        if(dupSet.has(eventHash)) {
          continue;
        }
        const currentKeyForNewHead = _cacheKey.event({eventHash, ledgerNodeId});
        const newHeadKey = _cacheKey.outstandingMergeEvent(
          {eventHash, ledgerNodeId});
        multi.rename(currentKeyForNewHead, newHeadKey);
        currentKeysForNewHeads.add(currentKeyForNewHead);
        newHeadKeys.add(newHeadKey);
      }
    }
    // remove head keys so they will not be removed from the cache
    if(newHeadKeys.size !== 0) {
      // head keys have already been renamed, don't attempt to delete again
      multi.sadd(outstandingMergeKey, [...newHeadKeys]);
    }
    // remove events with new children that have not yet been merged locally
    if(parentHashes.length !== 0) {
      for(const parentHash of parentHashes) {
        peerChildlessMap.delete(parentHash);
      }
    }
    // execute update
    await multi.exec();

    // return creators of all new events
    return {creators: [...newHeadCreators]};
  }
};

// FIXME: remove me
async function _validateCache({ledgerNode, creatorId}) {
  /* Note: Ensure the cache is in sync with the database. A previous work
  session may have terminated and failed to update the cache; this cache
  absolutely MUST NOT be corrupt in order for the work session operation
  to function properly, a corrupt cache here may result in invalidation as
  a properly operating peer. */

  // ensure the cache head for this ledger node is in sync w/database
  const [cacheHead, mongoHead] = await Promise.all([
    _history.getHead({creatorId, ledgerNode}),
    _history.getHead({creatorId, ledgerNode, useCache: false})
  ]);
  if(_.isEqual(cacheHead, mongoHead)) {
    // success
    return;
  }
  // FIXME: remove once heads behave as a true cache

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

// gets recent merge history and childless events
async function _getNonConsensusEvents({ledgerNode, creatorId, head}) {
  const result = await ledgerNode.storage.events.collection.find({
    'meta.consensus': false
  }).project({
    // FIXME: check to see if this is still a covered query
    _id: 0,
    'event.basisBlockHeight': 1,
    'event.mergeHeight': 1,
    'event.parentHash': 1,
    'event.treeHash': 1,
    'meta.eventHash': 1,
    'meta.continuity2017.creator': 1,
    'meta.continuity2017.generation': 1,
    'meta.continuity2017.localAncestorGeneration': 1,
    'meta.continuity2017.type': 1
  }).toArray();

  // build recent history and event summary information for determining
  // the hashes of childless events
  const historyMap = new Map();
  const eventSummaryMap = new Map();
  for(const {event, meta} of result) {
    const {
      eventHash,
      continuity2017: {
        creator, type, generation, localAncestorGeneration
      }
    } = meta;
    const {basisBlockHeight, mergeHeight, parentHash, treeHash} = event;
    eventSummaryMap.set(eventHash, {
      basisBlockHeight,
      mergeHeight,
      parentHash,
      creator,
      type,
      generation,
      localAncestorGeneration,
      children: 0
    });
    if(meta.continuity2017.type === 'm') {
      // this is the recent history information used by the continuity
      // algorithm
      historyMap.set(eventHash, {
        eventHash,
        event: {parentHash, treeHash},
        meta: {continuity2017: {creator}}
      });
    }
  }
  // compute the number of children for each event
  for(const [, eventSummary] of eventSummaryMap) {
    for(const p of eventSummary.parentHash) {
      const parent = eventSummaryMap.get(p);
      if(parent) {
        parent.children++;
      }
    }
  }
  // determine which events are childless
  const peerChildlessMap = new Map();
  const pendingLocalRegularEventHashes = new Set();
  for(const [eventHash, eventSummary] of eventSummaryMap) {
    // do not include events with children or the local head childless event
    if(eventSummary.children > 0 || eventHash === head.eventHash) {
      continue;
    }
    if(eventSummary.type === 'm') {
      // childless merge events must be from peers at this point, add to map
      const {
        basisBlockHeight,
        mergeHeight,
        creator,
        generation,
        localAncestorGeneration
      } = eventSummary;
      peerChildlessMap.set(eventHash, {
        creatorId: creator,
        eventHash,
        generation,
        basisBlockHeight,
        mergeHeight,
        localAncestorGeneration
      });
    } else if(eventSummary.creator === creatorId) {
      // must be a pending local event
      pendingLocalRegularEventHashes.add(eventHash);
    }
  }
  return {historyMap, peerChildlessMap, pendingLocalRegularEventHashes};
}
