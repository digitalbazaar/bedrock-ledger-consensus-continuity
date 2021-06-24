/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _consensus = require('./consensus');
const _events = require('../events');
const _localPeers = require('../localPeers');
const _stats = require('../stats');
const _witnesses = require('../witnesses');
const bedrock = require('bedrock');
const {merge} = require('./merge');
const logger = require('../logger');
const LRU = require('lru-cache');
const {runGossipCycle, sendNotification} = require('./gossip');
const {BedrockError} = bedrock.util;
const OperationQueue = require('./OperationQueue');
const PeerEventWriter = require('./PeerEventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

let _haltRequested = false;

bedrock.events.on('bedrock.exit', async () => {
  // tell all workers to halt
  _haltRequested = true;
});

module.exports = class Worker {
  constructor({session, halt = this._halt.bind(this)} = {}) {
    const {ledgerNode} = session;
    this.consensusState = {};
    this.localPeerId = null;
    this.genesisHead = null;
    this.head = null;
    this.historyMap = new Map();
    this.lastLocalContributorConsensus = false;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.operationQueue = new OperationQueue({worker: this});
    this.peerChildlessMap = new Map();
    this.peerEventWriter = new PeerEventWriter({worker: this});
    this.peerSelector = null;
    // local regular events that need to be included in the next merge event
    this.pendingLocalRegularEventHashes = new Set();
    this.mergeCommitment = null;
    this.needsGossipToMerge = false;
    this.nextLocalEventNumber = 0;
    this.session = session;
    this.storage = ledgerNode.storage;
    this.withheld = null;
    this._customHalt = halt;
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

    while(!this.halt()) {
      // try to compute consensus
      const consensusResult = await this._findConsensus();

      // no consensus found
      if(!consensusResult.consensus) {
        // update priority peers and report number of blocks created
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
        const {nextBlockHeight: blockHeight} = this.consensusState;
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

  halt() {
    if(_haltRequested) {
      return true;
    }
    return this._customHalt();
  }

  // return whether or not any events have been withheld
  hasWithheldEvents() {
    return !!this.withheld || this.withheldCache.itemCount > 0;
  }

  async init() {
    // clear any caches
    this._clearCaches();

    // initialize `localPeerId`
    const {ledgerNode, ledgerNodeId} = this;
    const localPeerId = await _localPeers.getPeerId({ledgerNodeId});
    this.localPeerId = localPeerId;

    // get current head
    this.head = await this._getHead({peerId: localPeerId});
    // no withheld merge event yet
    this.withheld = null;

    // init peer selector and peer event writer
    this.peerSelector = new GossipPeerSelector({worker: this});
    this.peerEventWriter = new PeerEventWriter({worker: this});

    // initialize recent history
    await this._initRecentHistory();

    // reinitialize consensus state
    this.consensusState = {};
    await this._updateConsensusState();

    // if a merge commitment has been set, ensure that it is not for a
    // peer that became a witness in the block following its creation; this
    // is not a protocol violation, but it is unnecessary to keep the
    // commitment and this is an opportunity to clean up state
    const {mergeCommitment, consensusState: {nextBlockHeight}} = this;
    if(mergeCommitment) {
      const {basisBlockHeight, creator} = mergeCommitment.committedTo;
      const blockHeight = basisBlockHeight + 2;
      if(blockHeight <= nextBlockHeight) {
        const {witnesses} = await _witnesses.getBlockWitnesses(
          {blockHeight, ledgerNode});
        if(witnesses.has(creator)) {
          this.mergeCommitment = null;
          this._clearWithheld();
        }
      }
    }
  }

  // if a peer has directly contributed a non-witness event without a valid
  // commitment and that peer is currently not a witness, then the peer is
  // considered withheld until that commitment reaches consensus or until the
  // worker resets due to a work session cycle w/o storing the `withheld`
  // information in the cross-work-session cache
  isPeerWithheld({peerId} = {}) {
    const {withheld, withheldCache} = this;
    return (withheld &&
      withheld.mergeEvent.meta.continuity2017.creator === peerId) ||
      withheldCache.has(peerId);
  }

  // allow `witnesses` to be overridden in tests
  async merge({witnesses, peerWitnessParentThreshold} = {}) {
    const {blockHeight: basisBlockHeight, priorityPeers} = this.consensusState;
    if(!witnesses) {
      witnesses = this.consensusState.witnesses;
    }
    const result = await merge({
      worker: this, priorityPeers, witnesses, basisBlockHeight,
      peerWitnessParentThreshold
    });
    this.needsGossipToMerge = result.status.needsGossip;
    return result;
  }

  async runGossipCycle({needsGossip = this.needsGossipToMerge} = {}) {
    const {mergeEventsReceived, busy} = await runGossipCycle(
      {worker: this, needsGossip});
    if(this.halt()) {
      // ran out of time to write the events
      return {mergeEventsReceived: 0, busy: false};
    }
    return {mergeEventsReceived, busy};
  }

  async notifyPeers() {
    const {ledgerNodeId, peerSelector} = this;
    return sendNotification({ledgerNodeId, peerSelector});
  }

  async writePeerEvents() {
    // clear `this.withheld` and any records in `withheldCache` that match
    // events to be written
    const {peerEventWriter: {eventMap}, withheld, withheldCache} = this;
    for(const [eventHash, eventRecord] of eventMap) {
      const {
        meta: {continuity2017: {creator, type}}
      } = eventRecord;
      if(type !== 'm') {
        continue;
      }
      if(withheld && withheld.mergeEvent.meta.eventHash === eventHash) {
        this._clearWithheld();
      }
      const wh = withheldCache.get(creator);
      if(wh && wh.mergeEvent.meta.eventHash === eventHash) {
        withheldCache.del(creator);
      }
    }

    // write events
    return this.peerEventWriter.flush();
  }

  /**
   * Adds a local merge event to storage.
   *
   * @param event {Object} - The event to store.
   * @param meta {Object} - The meta data for the event.
   * @param peerHeadCommitment {Object} - The peer head representing the
   *   the non-witness event that was committed to, if any.
   *
   * @returns {Promise} resolves once the operation completes.
   */
  async _addLocalMergeEvent({event, meta, peerHeadCommitment = null} = {}) {
    const {
      historyMap, ledgerNode, pendingLocalRegularEventHashes, peerChildlessMap
    } = this;

    // update history, childless head tracking info, and local head
    const {
      basisBlockHeight, parentHash, parentHashCommitment, mergeHeight, treeHash
    } = event;
    const {
      creator, isLocalContributor, generation,
      lastLocalContributor, localAncestorGeneration
    } = meta.continuity2017;
    const {eventHash} = meta;
    historyMap.set(eventHash, {
      eventHash,
      event: {basisBlockHeight, parentHash, treeHash},
      // generation is needed for computing non-consensus peer heads, it isn't
      // used in the continuity algorithm; `localReplayNumber` and
      // `replayDetectedBlockHeight` are always `0` and `-1`, respectively, for
      // local merge events
      meta: {
        continuity2017: {
          creator, generation,
          localReplayNumber: 0, replayDetectedBlockHeight: -1
        }
      }
    });
    // remove parents from peer childless map
    for(const hash of parentHash) {
      peerChildlessMap.delete(hash);
    }
    // clear pending regular event set
    pendingLocalRegularEventHashes.clear();
    // if a new commitment has been made, track it
    if(peerHeadCommitment) {
      this.mergeCommitment = {
        committedBy: {
          eventHash,
          generation,
          parentHashCommitment
        },
        committedTo: {
          basisBlockHeight: peerHeadCommitment.basisBlockHeight,
          creator: peerHeadCommitment.creator,
          eventHash: peerHeadCommitment.eventHash,
          mergeHeight: peerHeadCommitment.mergeHeight
        },
        consensus: false
      };
      // if commitment is to a withheld event, keep a reference to it in
      // case it gets cleared from the cache *and* update the
      // cross-work-session cache with it so it can be preserved across work
      // sessions
      const withheld = this.withheldCache.get(peerHeadCommitment.creator);
      if(withheld &&
        withheld.mergeEvent.meta.eventHash === peerHeadCommitment.eventHash) {
        this.withheld = withheld;
        _events.setWithheld({ledgerNode, withheld});
      }
    }
    // set new head
    this.head = {
      basisBlockHeight,
      eventHash,
      generation,
      isLocalContributor,
      lastLocalContributor,
      localAncestorGeneration,
      mergeHeight
    };
    // if merge event is a local contributor, clear flag
    if(isLocalContributor) {
      this.lastLocalContributorConsensus = false;
    }

    // add the event to storage
    return this.storage.events.add({event, meta});
  }

  async _addPeerEvents({events} = {}) {
    // update recent history
    const {historyMap, ledgerNodeId} = this;
    for(const {event, meta} of events) {
      const {
        eventHash,
        continuity2017: {
          creator, type, generation,
          localReplayNumber, replayDetectedBlockHeight
        }
      } = meta;
      if(type === 'r') {
        // Note: peer regular events have `operationRecords` not `operations` at
        // this point
        _stats.incrementOpsCounter({
          ledgerNodeId, type: 'peer', count: event.operationRecords.length
        });
      }
      if(type === 'm') {
        const {basisBlockHeight, parentHash, treeHash} = event;
        historyMap.set(eventHash, {
          eventHash,
          event: {basisBlockHeight, parentHash, treeHash},
          // `generation` needed for getting non-consensus peer heads; fork
          // info needed for determining if a fork has been detected once
          // consensus is reached
          meta: {
            continuity2017: {
              creator, generation, localReplayNumber, replayDetectedBlockHeight
            }
          }
        });
      }
    }

    await this._updatePeerState({events});
  }

  async _addWithheld({withheld} = {}) {
    if(!withheld) {
      throw new Error('"withheld" must be a non-null object.');
    }
    const {mergeEvent: {meta: {continuity2017: {creator}}}} = withheld;
    this.withheldCache.set(creator, withheld);
  }

  _clearCaches() {
    this.heads = new LRU({max: 1000});
    this.withheldCache = new LRU({max: 1000});
  }

  _clearWithheld() {
    const {ledgerNode} = this;
    this.withheld = null;
    _events.setWithheld({ledgerNode, withheld: null});
  }

  // returns which event hashes from the given array are not already
  // accounted for in the recent history map; this call is used during gossip
  _difference({eventHashes} = {}) {
    // Note: We can only rely on what's in the `historyMap`, all other state
    // is ephemeral and it is not safe to say we "have" any of those events.

    // check history map for events
    const notFound = [];
    const {historyMap} = this;
    for(const eventHash of eventHashes) {
      if(!historyMap.has(eventHash)) {
        notFound.push(eventHash);
      }
    }
    return notFound;
  }

  async _findConsensus() {
    // Note: DO NOT LOG RESULTS OF FIND CONSENSUS
    logger.verbose('Starting worker._findConsensus.');
    const consensusResult = await _consensus.find({worker: this});
    logger.verbose('worker._findConsensus complete.');
    return consensusResult;
  }

  // pick a withheld event to commit to
  _selectWithheld({witnesses} = {}) {
    const {withheldCache} = this;
    if(withheldCache.itemCount === 0) {
      return null;
    }
    const keys = withheldCache.keys();
    for(const creator of keys) {
      // skip any current witnesses
      if(witnesses.has(creator)) {
        continue;
      }
      return withheldCache.get(creator);
    }
    return null;
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

  async _getGenesisHead() {
    if(this.genesisHead) {
      return this.genesisHead;
    }
    const {ledgerNode} = this;
    const {getHead} = ledgerNode.storage.events.plugins['continuity-storage'];
    const [{meta}] = await getHead({generation: 0});
    if(!meta) {
      throw new BedrockError(
        'The genesis merge event was not found.',
        'InvalidStateError', {
          httpStatusCode: 400,
          public: true,
        });
    }
    // all fields other than creator, eventHash, and lastLocalContributor
    // are always constants
    this.genesisHead = {
      basisBlockHeight: 0,
      creator: meta.continuity2017.creator,
      eventHash: meta.eventHash,
      generation: 0,
      isLocalContributor: true,
      lastLocalContributor: meta.eventHash,
      localAncestorGeneration: 0,
      mergeHeight: 0,
      parentHashCommitment: null
    };
    return this.genesisHead;
  }

  async _getHead({peerId} = {}) {
    // return self-head if already initialized
    if(peerId === this.localPeerId && this.head !== null) {
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
        event: {basisBlockHeight, mergeHeight, parentHashCommitment},
        meta: {
          eventHash,
          continuity2017: {
            generation, isLocalContributor, lastLocalContributor,
            localAncestorGeneration
          }
        }
      }] = records;
      head = {
        basisBlockHeight,
        creator: peerId,
        eventHash,
        generation,
        isLocalContributor,
        lastLocalContributor,
        localAncestorGeneration,
        mergeHeight,
        parentHashCommitment
      };
      if(peerId === this.localPeerId) {
        this.head = head;
      } else {
        this.heads.set(peerId, head);
      }
      return head;
    }

    // *still* no head, so use a shallow copy of genesis head; do not use the
    // same instance to ensure each peer has its "own" head -- and to enable
    // faster head comparisons
    head = {...await this._getGenesisHead()};

    // set `this.head` for local peer and use heads cache for non-local
    if(peerId === this.localPeerId) {
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

  // gets latest non-consensus peer heads checking non-consensus events
  // previously loaded from the database (this method uses memory only; it
  // does not hit the database)
  // Note: This method is used to get "filters" to send to a remote peer during
  // gossip to help make it more efficient.
  async _getNonConsensusPeerHeads({countPerPeer = 1, peerLimit = 1000} = {}) {
    const {localPeerId, historyMap} = this;
    const headsMap = new Map();

    /* Note: We don't have to care about forks here. If there are forks, then
    the gossip protocol's cursor will ensure we always make progress when
    downloading from a peer that honors the protocol; there is no need to
    ensure we provide any filters at all ensure this happens (so there is no
    risk to running out of space to send filters due to forks -- or due to
    many participants, for that matter). However, sending filters makes gossip
    more efficient because the server can decide not to send us hashes it
    expects us to have. */

    // iterate in reverse to reduce updates by finding most recent first;
    // most recent are not necessarily last here, however as the history map
    // may not be built in topological order at the start of a work session,
    // thereafter it should be
    const eventSummaries = [...historyMap.values()].reverse();
    for(const {eventHash, meta} of eventSummaries) {
      const {continuity2017: {creator, generation}} = meta;
      if(creator === localPeerId) {
        // do not include own head
        continue;
      }
      const heads = headsMap.get(creator);
      if(!heads) {
        if(headsMap.size === peerLimit) {
          continue;
        }
        headsMap.set(creator, [{eventHash, generation}]);
      } else if(heads.length < countPerPeer) {
        heads.push({eventHash, generation});
      } else {
        // replace the first head with a lower generation in the list; it
        // doesn't matter which one, we don't know what the server has that
        // we will be sending these heads to and trying to sort the heads
        // and be intelligent here may not be worth the cost
        for(const head of heads) {
          if(generation > head.generation) {
            head.eventHash = eventHash;
            head.generation = generation;
          }
        }
      }
    }

    return headsMap;
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

  async _initRecentHistory() {
    // worker must be initialized *after* the genesis event which uses
    // `localEventNumber` `0` for the initial config and `1` for the first
    // merge event, so the minimum value here is 2
    this.nextLocalEventNumber = 2;

    // get non-consensus events to init recent merge history for continuity
    // algorithm and childless event information
    const {head, ledgerNode, localPeerId} = this;
    const records = await _events.getNonConsensusEvents(
      {ledgerNode, basisBlockHeight: this.head.basisBlockHeight});

    /* Determine if the last local operations that were merged have reached
    consensus or not. To do this, if `head` merged local operations, check
    to see if `head` is a non-consensus event or not. If `head` did not
    contribute local events, check to see if head's `lastLocalContributor`
    is a non-consensus event or not. */
    const lastLocalContributor = head.isLocalContributor ?
      head.eventHash : head.lastLocalContributor;
    // start with `true` and set to false if non-consensus match is found
    this.lastLocalContributorConsensus = true;

    // when a worker is reinitialized, see if a previous merge commitment can
    // be restored; only restore `mergeCommitment` if a previous commitment can
    // be found *and* the event that was committed to is either in the
    // cross-work-session withheld cache (_events.getWithheld()) or if it can
    // be found in non-consensus events (which means it is in the database
    // *and* it was not created by a detected replayer as
    // `getNonConsensusEvents` does not return these)
    this.mergeCommitment = null;
    let mergeCommitment;

    // build recent history and event summary information for determining
    // the hashes of childless events
    const historyMap = this.historyMap = new Map();
    const eventSummaryMap = new Map();
    for(const {event, meta} of records) {
      const {
        eventHash,
        continuity2017: {
          creator, type, generation,
          localAncestorGeneration,
          localReplayNumber, replayDetectedBlockHeight,
          localEventNumber
        }
      } = meta;
      // if event is `lastLocalContributor` then the last local ops
      // that were merged have not yet reached consensus
      if(lastLocalContributor === eventHash) {
        this.lastLocalContributorConsensus = false;
      }
      // update local event number to latest
      if(localEventNumber >= this.nextLocalEventNumber) {
        this.nextLocalEventNumber = localEventNumber + 1;
      }
      const {
        basisBlockHeight, mergeHeight,
        parentHash, parentHashCommitment, treeHash
      } = event;
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
        // algorithm; it must also include `generation` for computing
        // non-consensus peer heads
        historyMap.set(eventHash, {
          eventHash,
          event: {basisBlockHeight, parentHash, treeHash},
          meta: {
            continuity2017: {
              creator, generation, localReplayNumber, replayDetectedBlockHeight
            }
          }
        });
        // get last local non-consensus merge commitment
        if(creator === localPeerId && parentHashCommitment &&
          (!mergeCommitment ||
            generation > mergeCommitment.committedBy.generation)) {
          mergeCommitment = {
            committedBy: {eventHash, generation, parentHashCommitment},
            committedTo: null,
            consensus: false
          };
        }
      }
    }

    // if no non-consensus merge commitment found, get latest consensus one
    // that was for a non-witness that has not been detected as a replayer
    if(!mergeCommitment) {
      // stop searching `7` generations back; rationale for picking `7`
      // is that even after ~6 generations of `2f+1` empty merge events,
      // consensus should have been found; there's no reason to look further
      // back than that for the latest commitment
      const minGeneration = Math.max(1, head.generation - 7);
      const committedBy = await _events.getLatestParentHashCommitment(
        {ledgerNode, creator: localPeerId, minGeneration});
      if(committedBy) {
        // note that this commitment could have already been merged by this
        // peer in the past; if so, it will only be remerged by a witness
        // and it will not be a protocol violation -- it does not seem to be
        // worth trying to avoid remerging for the rare case where it may occur
        mergeCommitment = {committedBy, committedTo: null, consensus: true};
      }
    }

    // compute the number of children for each event and concurrently update
    // `mergeCommitment` with `committedTo` if it is a non-consensus event
    for(const [eventHash, eventSummary] of eventSummaryMap) {
      for(const p of eventSummary.parentHash) {
        const parent = eventSummaryMap.get(p);
        if(parent) {
          parent.children++;
        }
      }
      if(mergeCommitment && !this.mergeCommitment) {
        // if this is the event most recently committed to, save its head
        // information so it can be merged and also so it can be checked and
        // cleared if necessary (if the creator is detected as a replayer)
        const {committedBy: {parentHashCommitment}} = mergeCommitment;
        if(eventHash === parentHashCommitment[0]) {
          mergeCommitment.committedTo = {
            basisBlockHeight: eventSummary.basisBlockHeight,
            creator: eventSummary.creator,
            eventHash,
            mergeHeight: eventSummary.mergeHeight
          };
          // event that was committed to was found, so keep the commitment
          this.mergeCommitment = mergeCommitment;
        }
      }
    }

    // if merge commitment still not set, see if the cross-work-session
    // withheld cache has it
    if(mergeCommitment && !this.mergeCommitment) {
      const withheld = _events.getWithheld({ledgerNode});
      const {committedBy: {parentHashCommitment}} = mergeCommitment;
      if(withheld &&
        withheld.mergeEvent.meta.eventHash === parentHashCommitment[0]) {
        this.withheld = withheld;
        mergeCommitment.committedTo = {
          basisBlockHeight: withheld.mergeEvent.event.basisBlockHeight,
          creator: withheld.mergeEvent.meta.continuity2017.creator,
          eventHash: withheld.mergeEvent.meta.eventHash,
          mergeHeight: withheld.mergeEvent.event.mergeHeight
        };
        this.mergeCommitment = mergeCommitment;
      }
    }

    // determine which events are childless
    const peerChildlessMap = this.peerChildlessMap = new Map();
    const pendingLocalRegularEventHashes =
      this.pendingLocalRegularEventHashes = new Set();
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
          creator,
          eventHash,
          generation,
          basisBlockHeight,
          mergeHeight,
          localAncestorGeneration
        });
      } else if(eventSummary.creator === localPeerId) {
        // must be a pending local event
        pendingLocalRegularEventHashes.add(eventHash);
      }
    }
  }

  // called on init and whenever a new block is written
  async _updateConsensusState({blockRecord, consensusResult} = {}) {
    const {
      consensusState, head, ledgerNode, peerChildlessMap, withheldCache
    } = this;

    // clear `needsGossipToMerge` flag as consensus state has changed
    this.needsGossipToMerge = false;

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

    if(consensusResult) {
      // if the last merge event to merge non-merge events has reached
      // consensus, set `lastLocalContributorConsensus` flag to `true`
      const lastLocalContributor = head.isLocalContributor ?
        head.eventHash : head.lastLocalContributor;

      // handle newly-detected replayers
      const {replayerSet} = consensusResult;
      const {historyMap} = this;
      if(replayerSet.size > 0) {
        // drop event summaries from recent history, childless peer heads,
        // and withheld events for all replayers, these events will never
        // reach consensus because they can no longer be merged
        for(const [eventHash, {meta}] of historyMap) {
          const {continuity2017: {creator}} = meta;
          if(replayerSet.has(creator)) {
            historyMap.delete(eventHash);
            peerChildlessMap.delete(eventHash);
          }
        }
        for(const replayer of replayerSet) {
          withheldCache.del(replayer);
        }
        // clear merge commitment (so it won't be used) if the commitment was
        // for an event created by a now-detected replayer
        if(this.mergeCommitment &&
          replayerSet.has(this.mergeCommitment.committedTo.creator)) {
          this.mergeCommitment = null;
          this._clearWithheld();
        }
      }

      // remove consensus merge event hashes from recent history and update
      // commitment/withheld state
      const {mergeEventHash: eventHashes} = consensusResult;
      for(const eventHash of eventHashes) {
        if(eventHash === lastLocalContributor) {
          // last local contributor has reached consensus
          this.lastLocalContributorConsensus = true;
        }
        // if there's a merge commitment and the event that committed has now
        // reached consensus, mark it as such and write any `withheld`
        // merge event (and referenced regular events) that it committed to
        if(this.mergeCommitment &&
          eventHash === this.mergeCommitment.committedBy.eventHash) {
          this.mergeCommitment.consensus = true;
          const blockHeight = nextBlockInfo.blockHeight - 1;
          await this._writeWithheld({blockHeight});
        }
        historyMap.delete(eventHash);
      }

      // remove new witnesses from withheld cache/withheld
      const {withheld} = this;
      for(const witness of witnesses) {
        withheldCache.del(witness);
        if(withheld &&
          withheld.mergeEvent.meta.continuity2017.creator === witness) {
          this.mergeCommitment = null;
          this._clearWithheld();
        }
      }

      // update priority peers
      consensusState.priorityPeers = consensusResult.priorityPeers;
    } else {
      // no priority peers yet
      consensusState.priorityPeers = [];
    }

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
      const {
        creator, generation, isLocalContributor, lastLocalContributor,
        localAncestorGeneration
      } = meta.continuity2017;
      const head = {
        basisBlockHeight,
        creator,
        eventHash,
        generation,
        isLocalContributor,
        lastLocalContributor,
        localAncestorGeneration,
        mergeHeight
      };
      // keep track of tree children as potential heads
      // FIXME: how to handle forks?
      let childMap = creatorChildren.get(creator);
      if(!childMap) {
        creatorChildren.set(creator, childMap = new Map());
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
        creator,
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

  async _writeWithheld({blockHeight} = {}) {
    if(!this.withheld) {
      // nothing to write
      return;
    }

    // add all of the regular events first and then add merge event
    const {ledgerNode, withheld: {regularEvents, mergeEvent}} = this;
    for(const {event, meta} of regularEvents) {
      meta.continuity2017.localEventNumber = this.nextLocalEventNumber++;
      meta.continuity2017.requiredBlockHeight = blockHeight;
      await this.peerEventWriter.add({event, meta});
    }
    mergeEvent.meta.continuity2017.localEventNumber =
      this.nextLocalEventNumber++;
    mergeEvent.meta.continuity2017.requiredBlockHeight = blockHeight;
    await this.peerEventWriter.add(mergeEvent);
    await this.writePeerEvents();

    // clear cross-worker-session withheld event cache
    _events.setWithheld({ledgerNode, withheld: null});
  }
};
