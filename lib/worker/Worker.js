/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _cacheKey = require('../cache/cacheKey');
const _consensus = require('./consensus');
const _events = require('../events');
const _peers = require('../peers');
const _witnesses = require('../witnesses');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const {merge} = require('./merge');
const logger = require('../logger');
const LRU = require('lru-cache');
const {runGossipCycle, sendNotification} = require('./gossip');
const {BedrockError} = bedrock.util;
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
    this.config = config['ledger-consensus-continuity'].writer;
    this.consensusState = {};
    this.localPeerId = null;
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.genesisHead = null;
    this.head = null;
    this.historyMap = new Map();
    this.lastLocalContributorConsensus = false;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
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

  async init() {
    // clear any caches
    this._clearCaches();

    // initialize `localPeerId`
    const {ledgerNodeId} = this;
    const {id: localPeerId} = await _peers.get({ledgerNodeId});
    this.localPeerId = localPeerId;

    // get current head
    this.head = await this._getHead({peerId: localPeerId});
    // no withheld merge event yet
    this.withheld = null;

    // init peer selector and peer event writer
    this.peerSelector = new GossipPeerSelector(
      {peerId: localPeerId, worker: this});
    this.peerEventWriter = new PeerEventWriter({worker: this});

    // initialize recent history
    await this._initRecentHistory();

    // reinitialize consensus state
    this.consensusState = {};
    await this._updateConsensusState();
  }

  // allow `witnesses` to be overridden in tests
  async merge({witnesses, nonEmptyThreshold, emptyThreshold} = {}) {
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
    const {mergeEventsReceived, busy} = await runGossipCycle(
      {worker: this, needsGossip});
    if(this.halt()) {
      // ran out of time to write the events
      return {mergeEventsReceived: 0, busy: false};
    }
    return {mergeEventsReceived, busy};
  }

  async notifyPeers() {
    const {localPeerId, peerSelector} = this;
    const {priorityPeers} = this.consensusState;
    return sendNotification({localPeerId, priorityPeers, peerSelector});
  }

  async writePeerEvents() {
    const result = await this.peerEventWriter.flush();

    // FIXME: clear `this.withheld` and potentially clean up `withheldCache`
    // based on events written

    return result;
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
      historyMap, pendingLocalRegularEventHashes, peerChildlessMap
    } = this;

    // add the event to storage
    const record = await this.storage.events.add({event, meta});

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
          eventHash: this.head.eventHash,
          generation: this.head.generation,
          parentHashCommitment
        },
        committedTo: peerHeadCommitment,
        consensus: false
      };
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

    return record;
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
        // FIXME: Create a Stats API to call
        // Note: peer regular events have `operationRecords` not `operation` at
        // this point
        const txn = cache.client.multi();
        const opCountKey = _cacheKey.opCountPeer(
          {ledgerNodeId, second: Math.round(Date.now() / 1000)});
        txn.incrby(opCountKey, event.operationRecords.length);
        txn.expire(opCountKey, 6000);
        // we do not await the promise intentionally
        txn.exec().catch(e => logger.debug('Non-critical peer ops stat error', {
          error: e
        }));
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

  _clearCaches() {
    this.heads = new LRU({max: 1000});
    // FIXME: pass a function to determine the size of this cache by
    // measuring the size of the events therein; as it stands, this cache
    // could hold up to ~235MiB for MAX_OPS per merge event = 60 with 4KiB ops
    // plus ~1600 bytes (conservative) of overhead per merge event
    this.withheldCache = new LRU({max: 1000});
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

  async _findConsensus() {
    // Note: DO NOT LOG RESULTS OF FIND CONSENSUS
    logger.verbose('Starting worker._findConsensus.');
    const consensusResult = await _consensus.find({worker: this});
    logger.verbose('worker._findConsensus complete.');
    return consensusResult;
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

  // gets latest non-consensus peer heads checking both valid but unwritten
  // events in the queue and non-consensus events previously loaded from the
  // database (this method uses memory only; it does not hit the database)
  // Note: This method is used to get "filters" to send to a remote peer during
  // gossip to help make it more efficient.
  async _getNonConsensusPeerHeads({countPerPeer = 1, peerLimit = 1000} = {}) {
    const {localPeerId, historyMap, peerEventWriter} = this;
    const headsMap = new Map();

    /* Note: We don't have to care about forks here. If there are forks, then
    the gossip protocol's cursor will ensure we always make progress when
    downloading from a peer that honors the protocol; there is no need to
    ensure we provide any filters at all ensure this happens (so there is no
    risk to running out of space to send filters due to forks -- or due to
    many participants, for that matter). However, sending filters makes gossip
    more efficient because the server can decide not to send us hashes it
    expects us to have. */

    // first check `peerEventWriter` for valid but unflushed events, it is
    // safe to gossip about these; they will *always* be written to disk before
    // any events received in a gossip session that used them as filters
    const {eventMap} = peerEventWriter;
    // iterate in reverse to reduce updates by finding most recent first;
    // most recent are always first in `peerEventWriter` because they are
    // added in topological order
    const records = [...eventMap.values()].reverse();
    for(const {meta} of records) {
      // skip non-merge events and events created by the local peer
      const {continuity2017: {type, creator, generation}, eventHash} = meta;
      if(type !== 'm' || creator === localPeerId) {
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
      }
    }

    // iterate in reverse to reduce updates by finding most recent first;
    // most recent are not necessarily last here, however as the history map
    // not be built in topological order if it was built at the start of a
    // session from events that came out of the database in an arbitrary order
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

    // when a worker is reinitialized, any merge commitment is dropped (and any
    // withheld event is not stored anywhere); so only set `mergeCommitment` if
    // a previous commitment can be found *and* the event that was committed to
    // can be found in non-consensus events (which means it is in the database
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
        if(creator === localPeerId && event.parentHashCommitment &&
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
          mergeCommitment.committedTo = {eventHash, ...eventSummary};
          // event that was committed to was found, so keep the commitment
          this.mergeCommitment = mergeCommitment;
        }
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
    const {consensusState, head, ledgerNode, peerChildlessMap} = this;

    // clear `needsGossipToMerge` flag as consensus state has changed
    this.needsGossipToMerge = false;

    if(consensusResult) {
      // if the last merge event to merge non-merge events has reached
      // consensus, set `lastLocalContributorConsensus` flag to `true`
      const lastLocalContributor = head.isLocalContributor ?
        head.eventHash : head.lastLocalContributor;

      // handle newly-detected replayers
      const {replayerSet} = consensusResult;
      const {historyMap} = this;
      if(replayerSet.size > 0) {
        // drop event summaries from recent history and childless peer heads
        // for all replayers, these events will never reach consensus because
        // they can no longer be merged
        for(const [eventHash, {meta}] of historyMap) {
          if(replayerSet.has(meta.continuity2017.creator)) {
            historyMap.delete(eventHash);
            peerChildlessMap.delete(eventHash);
          }
        }
        // clear merge commitment (so it won't be used) if the commitment was
        // for an event created by a now-detected replayer
        if(this.mergeCommitment &&
          replayerSet.has(this.mergeCommitment.committedTo.creator)) {
          this.mergeCommitment = null;
        }
      }

      // remove consensus merge event hashes from recent history
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
          await this._writeWithheld();
        }
        historyMap.delete(eventHash);
      }

      // update priority peers
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

  async _writeWithheld() {
    if(!this.withheld) {
      // nothing to write
      return;
    }

    // add all of the regular events first and then add merge event
    const {withheld: {regularEvents, mergeEvent}} = this;
    for(const {event, meta} of regularEvents) {
      await this.peerEventWriter.add({event, meta});
    }
    await this.peerEventWriter.add(mergeEvent);
    await this.writePeerEvents();

    // FIXME: this should happen within `writePeerEvents` automatically
    // clear withheld
    this.withheld = null;
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
};
