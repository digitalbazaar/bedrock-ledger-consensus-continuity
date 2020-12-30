/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _consensus = require('../consensus');
const _events = require('../events');
const _peers = require('../peers');
const _witnesses = require('../witnesses');
const bedrock = require('bedrock');
const {config} = bedrock;
const {merge} = require('./merge');
const logger = require('../logger');
const LRU = require('lru-cache');
const {runGossipCycle, sendNotification} = require('./gossip');
const {BedrockError} = bedrock.util;
const PeerEventWriter = require('./PeerEventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

module.exports = class Worker {
  constructor({session, halt = this._halt} = {}) {
    const {ledgerNode} = session;
    this.config = config['ledger-consensus-continuity'].writer;
    this.consensusState = {};
    this.creatorId = null;
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.halt = halt;
    this.head = null;
    this.historyMap = new Map();
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.peerChildlessMap = new Map();
    this.peerEventWriter = new PeerEventWriter({worker: this});
    this.peerSelector = null;
    // local regular events that need to be included in the next merge event
    this.pendingLocalRegularEventHashes = new Set();
    this.needsGossipToMerge = false;
    this.session = session;
    this.storage = ledgerNode.storage;
    this._clearCaches();
  }

  /**
   * Continually attempts to achieve consensus and write new blocks until
   * consensus can't be reached because more merge events are needed.
   *
   * @returns {Promise} - Resolves once the operation completes.
   */
  async extendBlockchain() {
    let blocks = 0;
    const {consensusState, ledgerNode} = this;

    while(!this.halt()) {
      // get recent history to run consensus algorithm on
      const history = this.getRecentHistory();

      // Note: DO NOT LOG RESULTS OF FIND CONSENSUS
      logger.verbose('Starting blockchain.extend consensus.find.');
      // get block height to use for consensus based on the "next" block height
      // for the block to be added next to the blockchain
      const {nextBlockHeight: blockHeight, continuityState: state, witnesses} =
        consensusState;
      const consensusResult = await _consensus.find(
        {ledgerNode, history, blockHeight, witnesses, state});
      logger.verbose('extendBlockchain.findConsensus complete.');

      if(!consensusResult.consensus) {
        // no consensus reached, update priority peers and report number of
        // blocks created
        this.consensusState.priorityPeers = consensusResult.priorityPeers;
        return {blocks};
      }

      // consensus found, write next block
      const {blockRecord, hasEffectiveConfigurationEvent} = await _blocks.write(
        {worker: this, consensusResult});
      if(hasEffectiveConfigurationEvent) {
        // if the block involved a configuration change, the work session
        // should terminate and the next work session will use a new LedgerNode
        // instance with the new ledger configuration
        throw new BedrockError(
          'Ledger configuration change detected.',
          'LedgerConfigurationChangeError', {
            blockHeight
          });
      }

      // update worker state using new block record and consensus result
      await this._updateConsensusState({blockRecord, consensusResult});

      blocks++;
      logger.verbose(
        'Found consensus; consensus algorithm found consensus ' +
        `${blocks} consecutive time(s).`);
    }

    // halted, report number of blocks created
    return {blocks};
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

  async init() {
    // clear any caches
    this._clearCaches();

    // initialize `creatorId`
    const {ledgerNode, ledgerNodeId} = this;
    const {id: creatorId} = await _peers.get({ledgerNodeId});
    this.creatorId = creatorId;

    // get current head
    this.head = await this._getHead({peerId: creatorId});
    this.peerSelector = new GossipPeerSelector(
      {peerId: creatorId, worker: this});

    // get recent merge history for continuity algorithm and childless event
    // information and initialize peer event writer
    const {historyMap, peerChildlessMap, pendingLocalRegularEventHashes} =
      await _events.getNonConsensusEvents(
        {ledgerNode, creatorId, head: this.head});
    this.historyMap = historyMap;
    this.peerChildlessMap = peerChildlessMap;
    this.peerEventWriter = new PeerEventWriter({worker: this});
    this.pendingLocalRegularEventHashes = pendingLocalRegularEventHashes;

    // reinitialize consensus state
    this.consensusState = {};
    await this._updateConsensusState();
  }

  // allow `witnesses` to be overridden in tests
  async merge({witnesses, nonEmptyThreshold = 1, emptyThreshold} = {}) {
    const {blockHeight: basisBlockHeight, priorityPeers} = this.consensusState;
    if(!witnesses) {
      witnesses = this.consensusState.witnesses;
    }
    const result = await merge({
      worker: this, priorityPeers, witnesses, basisBlockHeight,
      nonEmptyThreshold, emptyThreshold
    });
    this.needsGossipToMerge = result.status.needsGossip;
    return result;
  }

  async runGossipCycle({needsGossip = this.needsGossipToMerge} = {}) {
    const {mergeEventsReceived} = await runGossipCycle(
      {worker: this, needsGossip});
    if(this.halt()) {
      // ran out of time to write the events
      return {mergeEventsReceived: 0};
    }
    // ensure all received events are written
    await this.writePeerEvents();
    return {mergeEventsReceived};
  }

  async notifyPeers() {
    const {creatorId, peerSelector} = this;
    const {priorityPeers} = this.consensusState;
    return sendNotification({creatorId, priorityPeers, peerSelector});
  }

  async writePeerEvents() {
    return this.peerEventWriter.flush();
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
      historyMap, pendingLocalRegularEventHashes, peerChildlessMap
    } = this;

    // add the event to storage
    const record = await this.storage.events.add({event, meta});

    // update history, childless head tracking info, and local head
    const {basisBlockHeight, parentHash, mergeHeight, treeHash} = event;
    const {creator, generation, localAncestorGeneration} = meta.continuity2017;
    const {eventHash} = meta;
    historyMap.set(eventHash, {
      eventHash,
      event: {basisBlockHeight, parentHash, treeHash},
      // generation is needed for computing non-consensus peer heads, it isn't
      // used in the continuity algorithm
      meta: {continuity2017: {creator, generation}}
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

  async _addPeerEvents({events} = {}) {
    // update recent history
    const {historyMap} = this;
    for(const {event, meta} of events) {
      const {eventHash, continuity2017: {creator, type, generation}} = meta;
      if(type === 'm') {
        const {basisBlockHeight, parentHash, treeHash} = event;
        historyMap.set(eventHash, {
          eventHash,
          event: {basisBlockHeight, parentHash, treeHash},
          // `generation` needed for getting non-consensus peer heads
          meta: {continuity2017: {creator, generation}}
        });
      }
    }

    await this._updatePeerState({events});
  }

  _clearCaches() {
    this.heads = new LRU({max: 1000});
  }

  // returns which event hashes from the given array are not already
  // accounted for in either the recent history map or that are validated
  // but still pending to be written to the database; this call is used
  // during gossip
  _difference({eventHashes} = {}) {
    // check history map and `peerEventWriter` for events
    const notFound = [];
    const {historyMap, peerEventWriter: {eventMap}} = this;
    for(const eventHash of eventHashes) {
      if(!historyMap.has(eventHash) && !eventMap.has(eventHash)) {
        notFound.push(eventHash);
      }
    }
    return notFound;
  }

  _getUncommittedEvents({eventHashes} = {}) {
    // check `peerEventWriter` for events
    const {eventMap} = this.peerEventWriter;
    const results = new Map();
    for(const eventHash of eventHashes) {
      const record = eventMap.get(eventHash);
      if(record) {
        results.set(eventHash, record);
      }
    }
    return results;
  }

  async _getHead({peerId} = {}) {
    // return self-head if already initialized
    if(peerId === this.creatorId && this.head !== null) {
      return this.head;
    }

    // return head from LRU cache
    let head = this.heads.get(peerId);
    if(head) {
      return head;
    }

    // head not found in LRU cache, get from database
    const {ledgerNode} = this;
    const {getHead} = ledgerNode.storage.events.plugins['continuity-storage'];
    // FIXME: how to handle forks?
    const records = await getHead({creatorId: peerId});
    if(records.length > 0) {
      const [{
        event: {basisBlockHeight, mergeHeight},
        meta: {
          eventHash,
          continuity2017: {generation, localAncestorGeneration}
        }
      }] = records;
      head = {
        eventHash,
        generation,
        basisBlockHeight,
        mergeHeight,
        localAncestorGeneration
      };
      if(peerId === this.creatorId) {
        this.head = head;
      } else {
        this.heads.set(peerId, head);
      }
      return head;
    }

    // *still* no head, so use genesis head
    const [{meta}] = await getHead({generation: 0});
    if(!meta) {
      throw new BedrockError(
        'The genesis merge event was not found.',
        'InvalidStateError', {
          httpStatusCode: 400,
          public: true,
        });
    }
    // generation, basisBlockHeight, mergeHeight, localAncestorGeneration for
    // genesis are always zero
    head = {
      eventHash: meta.eventHash,
      generation: 0,
      basisBlockHeight: 0,
      mergeHeight: 0,
      localAncestorGeneration: 0
    };
    if(peerId === this.creatorId) {
      this.head = head;
    } else {
      this.heads.set(peerId, head);
    }
    return head;
  }

  async _getHeads({peerIds} = {}) {
    const peers = [...new Set(peerIds)];
    const promises = [];
    for(const peerId of peers) {
      // FIXME: support checking for forks? how to handle?
      promises.push(this._getHead({peerId}));
    }
    const heads = new Map();
    const results = await Promise.all(promises);
    for(let i = 0; i < results.length; ++i) {
      heads.set(peers[i], results[i]);
    }
    return {heads};
  }

  // gets latest non-consensus peer heads checking both valid but unwritten
  // events in the queue and non-consensus events previously loaded from the
  // database (this method uses memory only; it does not hit the database)
  async _getNonConsensusPeerHeads() {
    const {creatorId, historyMap, peerEventWriter} = this;
    const peerHeads = new Map();

    // FIXME: will need to read `forkId` in events (once implemented) and check
    // for unique values of it to ensure all peer heads are sent for forked
    // peers; need to consider what to do if `forkId` becomes a huge array
    // ... the approach may not be viable if that's possible

    // first check `peerEventWriter` for valid but unflushed events, it is
    // safe to gossip about these; they will all be written to disk together
    // or not at all
    const {eventMap} = peerEventWriter;
    // iterate in reverse to reduce updates by finding most recent first
    const records = [...eventMap.values()].reverse();
    for(const {meta} of records) {
      // skip non-merge events and events created by the local peer
      const {continuity2017: {type, creator, generation}, eventHash} = meta;
      if(type !== 'm' || creator === creatorId) {
        continue;
      }
      const head = peerHeads.get(creator);
      if(!head) {
        peerHeads.set(creator, {eventHash, generation});
        continue;
      }
      if(generation > head.generation) {
        head.eventHash = eventHash;
        head.generation = generation;
      }
    }

    // iterate in reverse to reduce updates by finding most recent first
    const eventSummaries = [...historyMap.values()].reverse();
    for(const {eventHash, meta} of eventSummaries) {
      const {continuity2017: {creator, generation}} = meta;
      if(creator === creatorId) {
        // do not include own head
        continue;
      }
      const head = peerHeads.get(creator);
      if(!head) {
        peerHeads.set(creator, {eventHash, generation});
        continue;
      }
      if(generation > head.generation) {
        head.eventHash = eventHash;
        head.generation = generation;
      }
    }

    return peerHeads;
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

  // called on init and whenever a new block is written
  async _updateConsensusState({blockRecord, consensusResult} = {}) {
    const {consensusState, ledgerNode} = this;

    // clear `needsGossipToMerge` flag as consensus state has changed
    this.needsGossipToMerge = false;

    if(consensusResult) {
      // remove consensus merge event hashes from recent history
      const {mergeEventHash: eventHashes} = consensusResult;
      const {historyMap} = this;
      for(const eventHash of eventHashes) {
        historyMap.delete(eventHash);
      }
      consensusState.priorityPeers = consensusResult.priorityPeers;
    } else {
      // no priority peers yet
      consensusState.priorityPeers = [];
    }

    let nextBlockInfo;
    if(!blockRecord) {
      // retrieve next block info from database
      nextBlockInfo = await _blocks.getNextBlockInfo({ledgerNode});
    } else {
      // a block record was given, use it to update state
      nextBlockInfo = {
        blockHeight: blockRecord.block.blockHeight + 1,
        previousBlockHash: blockRecord.meta.blockHash,
        previousBlockId: blockRecord.block.id
      };
    }

    // update block information
    const {blockHeight: nextBlockHeight, previousBlockHash, previousBlockId} =
      nextBlockInfo;
    consensusState.blockHeight = nextBlockHeight - 1;
    consensusState.nextBlockHeight = nextBlockHeight;
    consensusState.previousBlockHash = previousBlockHash;
    consensusState.previousBlockId = previousBlockId;

    // get next block witnesses
    const {witnesses} = await _witnesses.getBlockWitnesses(
      {blockHeight: nextBlockHeight, ledgerNode});
    consensusState.witnesses = witnesses;

    // init/re-init continuity state for running continuity algorithm for
    // the next block
    consensusState.continuityState = {
      init: false,
      eventMap: new Map(),
      blockHeight: -1,
      hashToMemo: new Map(),
      symbolToMemo: new Map(),
      supportCache: new Map()
    };
  }

  // called whenever new valid peer events have been added to the database
  async _updatePeerState({events}) {
    const {peerChildlessMap} = this;

    // process merge events included in the batch, looking for new heads
    // and childless events so cached state can be updated
    const creatorChildren = new Map();
    const _parentHashes = [];
    for(const {event, meta} of events) {
      if(meta.continuity2017.type !== 'm') {
        continue;
      }
      // potentially new head
      const {eventHash} = meta;
      const {basisBlockHeight, mergeHeight, treeHash} = event;
      const {creator: creatorId, generation, localAncestorGeneration} =
        meta.continuity2017;
      const head = {
        eventHash,
        generation,
        basisBlockHeight,
        mergeHeight,
        localAncestorGeneration
      };
      // keep track of tree children as potential heads
      // FIXME: how to handle forks?
      let childMap = creatorChildren.get(creatorId);
      if(!childMap) {
        creatorChildren.set(creatorId, childMap = new Map());
      }
      childMap.set(treeHash, head);
      _parentHashes.push(...event.parentHash);
    }

    // walk `creatorChildren` for each head in the LRU cache and update it
    this.heads.forEach((head, peerId) => {
      const childMap = creatorChildren.get(peerId);
      if(!childMap) {
        return;
      }
      const oldHead = head;
      let newHead;
      do {
        newHead = childMap.get(head.eventHash);
        if(newHead) {
          head = newHead;
        }
      } while(newHead);
      if(head !== oldHead) {
        this.heads.set(peerId, head);
      }
    });

    // add any peer merge events to the peer childless map if they do not
    // appear in `parentHashes`
    const parentHashes = new Set(_parentHashes);
    for(const {event, meta} of events) {
      const {eventHash, continuity2017: {type}} = meta;
      if(type !== 'm' || parentHashes.has(eventHash)) {
        continue;
      }
      const {basisBlockHeight, mergeHeight} = event;
      const {continuity2017: {
        creator, generation, localAncestorGeneration
      }} = meta;
      peerChildlessMap.set(eventHash, {
        creatorId: creator,
        eventHash,
        generation,
        basisBlockHeight,
        mergeHeight,
        localAncestorGeneration
      });
    }

    // delete any hashes from peer childless map if they appear in
    // `parentHashes`
    for(const parentHash of parentHashes) {
      peerChildlessMap.delete(parentHash);
    }
  }
};
