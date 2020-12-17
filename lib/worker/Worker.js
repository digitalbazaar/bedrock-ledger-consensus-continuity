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
const PeerEventWriter = require('./PeerEventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

module.exports = class Worker {
  constructor({session, halt = this._halt} = {}) {
    const {ledgerNode} = session;
    this.halt = halt;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.config = config['ledger-consensus-continuity'].writer;
    this.creatorId = null;
    // local regular events that need to be included in the next merge event
    this.pendingLocalRegularEventHashes = new Set();
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.head = null;
    this.peerEventWriter = new PeerEventWriter({ledgerNode});
    // FIXME: move `creatorId` discovery inside peer selector
    // this.peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
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
          worker: this, ledgerNode, priorityPeers, creatorId, peerSelector,
          needsGossip, witnesses, blockHeight, halt
        });

        // work session expired
        if(halt()) {
          break;
        }

        // 3. commit all cached events to mongo
        await peerEventWriter.flush();

        // 4. merge if possible
        const {merged, status: mergeStatus} = await merge({
          worker: this, ledgerNode, creatorId, priorityPeers, witnesses,
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

  /**
   * Adds a local merge event to storage.
   *
   * @param event {Object} - The event to store.
   * @param meta {Object} - The meta data for the event.
   *
   * @returns {Promise} resolves once the operation completes.
   */
  async addLocalMergeEvent({event, meta} = {}) {
    const {ledgerNodeId, pendingLocalRegularEventHashes, peerEventWriter} =
      this;

    // add the event to storage
    const record = await this.storage.events.add({event, meta});

    // update the cache
    await _cache.events.addLocalMergeEvent({...record, ledgerNodeId});

    // update childless head tracking info and local head
    const {basisBlockHeight, parentHash, mergeHeight} = event;
    const {generation, localAncestorGeneration} = meta.continuity2017;
    const {eventHash} = meta;
    // remove parents from peer childless map and pending regular event set
    for(const hash of parentHash) {
      peerEventWriter.peerChildlessMap.delete(hash);
      pendingLocalRegularEventHashes.delete(hash);
    }
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

  // default halt function
  _halt() {
    // expire session early, with 5 second buffer for overruns
    return this.session.timeRemaining() < 5000;
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

    // get childless event information and initialize peer event writer
    const {peerChildlessMap, pendingLocalRegularEventHashes} =
      await _getChildlessEvents({ledgerNode, creatorId, head: this.head});
    this.peerEventWriter = new PeerEventWriter({ledgerNode, peerChildlessMap});
    this.pendingLocalRegularEventHashes = pendingLocalRegularEventHashes;

    // FIXME: this will unnecessary once head and childless information
    // is moved to in-memory state entirely
    // ensure cache and mongo are in sync
    await _validateCache({ledgerNode, creatorId});
  }
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

async function _getChildlessEvents({ledgerNode, creatorId, head}) {
  const result = await ledgerNode.storage.events.collection.find({
    'meta.consensus': false
  }).project({
    // FIXME: check to see if this is still a covered query
    _id: 0,
    'event.basisBlockHeight': 1,
    'event.mergeHeight': 1,
    'event.parentHash': 1,
    'meta.eventHash': 1,
    'meta.continuity2017.creator': 1,
    'meta.continuity2017.generation': 1,
    'meta.continuity2017.localAncestorGeneration': 1,
    'meta.continuity2017.type': 1
  }).toArray();
  const eventsMap = new Map();
  for(const e of result) {
    eventsMap.set(e.meta.eventHash, {
      parentHash: e.event.parentHash,
      _creator: e.meta.continuity2017.creator,
      _type: e.meta.continuity2017.type,
      _generation: e.meta.continuity2017.generation,
      _localAncestorGeneration: e.meta.continuity2017.localAncestorGeneration,
      _children: 0,
    });
  }
  // compute the number of children for each event
  for(const [, event] of eventsMap) {
    for(const p of event.parentHash) {
      const parent = eventsMap.get(p);
      if(parent) {
        parent._children++;
      }
    }
  }
  const peerChildlessMap = new Map();
  const pendingLocalRegularEventHashes = new Set();
  for(const [eventHash, event] of eventsMap) {
    // do not include events with children or the local head childless event
    if(event._children > 0 || eventHash === head.eventHash) {
      continue;
    }
    if(event._type === 'm') {
      // childless merge events must be from peers at this point, add to map
      const {
        basisBlockHeight,
        mergeHeight,
        _generation: generation,
        _localAncestorGeneration: localAncestorGeneration
      } = event;
      peerChildlessMap.set(eventHash, {
        eventHash,
        generation,
        basisBlockHeight,
        mergeHeight,
        localAncestorGeneration
      });
    } else if(event._creator === creatorId) {
      // must be a pending local event
      pendingLocalRegularEventHashes.add(eventHash);
    }
  }
  return {peerChildlessMap, pendingLocalRegularEventHashes};
}
