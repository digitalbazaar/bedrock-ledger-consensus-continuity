/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _cache = require('../cache');
const _cacheKey = require('../cache/cache-key');
const _election = require('../election');
const _events = require('../events');
const _voters = require('../voters');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('../logger');
const {BedrockError} = bedrock.util;
const ContinuityAgent = require('./continuity-agent');

module.exports = class ConsensusAgent extends ContinuityAgent {
  constructor({agentName, ledgerNode}) {
    agentName = agentName || 'consensus';
    super({agentName, ledgerNode});
    this.debounce = config['ledger-consensus-continuity'].consensus.debounce;
    this.messageListener = null;
    this.subscriber = new cache.Client().client;
    this.priorityPeers = new Set();
  }

  _onQuit() {
    this.subscriber.quit();
  }

  async _workLoop() {
    this.messageListener = this._onMessage.bind(this);
    const {ledgerNodeId} = this;
    try {
      await this.subscriber.subscribe(`continuity2017|event|${ledgerNodeId}`);
    } catch(e) {
      return this._quit(e);
    }
    this.subscriber.on('message', this.messageListener);
    this.peerId = (await _voters.get({ledgerNodeId})).id;
    this.priorityPeersCacheKey = _cacheKey.priorityPeers(ledgerNodeId);
    // important to start worker right away to catch events that
    // may have already been added
    this._work();
  }

  _onMessage(channel, message) {
    // `merge` event is emitted whenever *any* merge event comes in from any
    // peer ... we want to see if any of these peers are in the `priorityPeers`
    // set to determine if we should run the consensus algorithm
    if(message.startsWith('merge|')) {
      // parse creators of new merge events and check for a match in
      // `priorityPeers` (`merge|JSON ARRAY`)... if the new merge events
      // are not from priority peers, ignore the message
      const creators = JSON.parse(message.substr(6));
      if(!creators.some(id => this.priorityPeers.has(id))) {
        return;
      }

      if(this.working) {
        // already working, notify new priority merge event exists
        this.newPriorityMergeEvent = true;
      } else if(!this.halt) {
        this.working = true;
        setTimeout(() => this._work(), this.debounce);
      }
    }
  }

  async _work() {
    this.working = true;
    let error;
    try {
      while(!this.halt) {
        await this._extendBlockchain();
        // consensus not reached... if no new priority merge events have
        // arrived, break out; note this is done here because the guard for
        // setting `newPriorityMergeEvent` is the `working` flag which is
        // set synchronously in this function... `newPriorityMergeEvent`
        // may have been set to true while awaiting `_extendBlockchain`
        if(!this.newPriorityMergeEvent) {
          break;
        }
      }
    } catch(e) {
      error = e;
    }
    this.working = false;
    if(error) {
      return this._quit(error);
    }
    if(this.halt) {
      return this._quit();
    }
  }

  /**
   * Continually attempts to achieve consensus and write new blocks until
   * consensus can't be reached because more merge events are needed.
   */
  async _extendBlockchain() {
    const state = {};
    let recoveryMode = false;
    const {ledgerNodeId, peerId, priorityPeersCacheKey} = this;

    while(!this.halt) {
      await this._updateState({recoveryMode, state});
      let result;
      try {
        // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
        this.priorityPeers.clear();
        result = await this._findConsensus(state);
      } catch(error) {
        if(error.name !== 'NewElectorsRequiredError') {
          logger.error('Error in _findConsensus', {error});
          throw error;
        }
        // signal to _updateState that recoveryElectors should be used
        recoveryMode = true;
        continue;
      }
      // findConsensus checks for WebLedgerConfigurationEvents in the
      // new block. If one exists, then further processing in this a
      // agent should stop. By returning an error here, all the other
      // agents will be stopped as well and the work session will
      // end. Before the next work session, a new LedgerNode instance
      // will be created using the new ledger configuration.
      if(result.writeBlock &&
        result.writeBlock.hasEffectiveConfigurationEvent) {
        throw new BedrockError(
          'Ledger configuration change detected.',
          'LedgerConfigurationChangeError', {
            blockHeight: result.writeBlock.blockHeight
          });
      }
      if(result.consensus) {
        // immediately loop as there's no known need to merge
        // yet and there may be more consensus to be found
        // (i.e. more blocks to be written)
        // clear recoverMode flag
        recoveryMode = false;
        // write electors that participated as priority peers
        await _cache.consensus.setPriorityPeers({
          priorityPeers: result.consensus.creators,
          ledgerNodeId
        });
        continue;
      }
      if(this.newPriorityMergeEvent) {
        // more merge events came in, run consensus again... note that
        // `priorityPeers` is empty to prevent triggering consensus to
        // run again via a `merge` notification; also, priorityPeers in cache
        // is unchanged
        continue;
      }
      // no consensus reached -- update priority peers
      this.priorityPeers = new Set(result.priorityPeers);
      const multi = cache.client.multi();
      // write `this.priorityPeers` set to cache
      multi.del(priorityPeersCacheKey);
      multi.sadd(priorityPeersCacheKey, [...this.priorityPeers]);
      // inform other agents/listeners that a merge is required
      if(this.priorityPeers.has(peerId)) {
        multi.publish(`continuity2017|needsMerge|${ledgerNodeId}`, 'consensus');
      }
      await multi.exec();
      break;
    }
  }

  /**
   * Gets the latest consensus block and returns the new proposed block height
   * for the ledger (i.e. the current `blockHeight + 1`) and the latest block
   * hash as what would become the next `previousBlockHash`.
   *
   * @param state options that govern the consensus process.
   * @returns {Promise}
   */
  async _findConsensus(state) {
    const {ledgerNode, ledgerNodeId} = this;
    const creator = await _voters.get({ledgerNodeId});
    // clear flag indicating any priority merge events have been received
    // right before grabbing history; this ensures any new notifications
    // that come in while we are getting the history won't be missed
    this.newPriorityMergeEvent = false;
    const history = await _events.getRecentHistory({
      creatorId: creator.id,
      excludeLocalRegularEvents: true,
      ledgerNode,
    });
    logger.verbose('Starting _extendBlockchain.findConsensus.');
    // Note: DO NOT LOG RESULTS OF FINDCONSENSUS
    const consensusResult = await _election.findConsensus({
      ledgerNode,
      history,
      blockHeight: state.blockHeight,
      electors: state.electors,
      recoveryElectors: state.recoveryElectors,
      recoveryGenerationThreshold: state.recoveryGenerationThreshold,
      mode: state.mode
    });

    logger.verbose('_extendBlockchain.findConsensus complete.');
    if(!consensusResult.consensus) {
      return {
        consensus: false,
        writeBlock: false,
        priorityPeers: consensusResult.priorityPeers
      };
    }
    logger.verbose('Found consensus.');
    const writeBlock = await _blocks.write(
      {consensusResult, ledgerNode, state});
    return {consensus: consensusResult, writeBlock};
  }

  async _updateState({recoveryMode, state}) {
    const {ledgerNode, ledgerNodeId} = this;
    const {blockHeight: nextBlockHeight, previousBlockHash, previousBlockId} =
      await _blocks.getNextBlockInfo(ledgerNode);
    const cachedBlockHeight = await _cache.blocks.blockHeight(ledgerNodeId);
    const blockHeight = nextBlockHeight - 1;
    if(blockHeight !== cachedBlockHeight) {
      // this should never happen
      if((blockHeight - cachedBlockHeight) !== 1) {
        throw new BedrockError(
          'Cache is behind by more than one block.', 'InvalidStateError',
          {blockHeight, cachedBlockHeight, ledgerNodeId});
      }
      // inconsistency needs to be repaired
      logger.debug('BLOCKS REPAIRCACHE');
      await _blocks.repairCache({blockHeight, ledgerNode});
    }
    const blockElectors = await _election.getBlockElectors(
      {blockHeight: nextBlockHeight, ledgerNode, recoveryMode});

    state.electors = blockElectors.electors;
    state.recoveryElectors = blockElectors.recoveryElectors;
    state.recoveryGenerationThreshold =
      blockElectors.recoveryGenerationThreshold;
    state.mode = 'first';
    state.blockHeight = nextBlockHeight;
    state.previousBlockHash = previousBlockHash;
    state.previousBlockId = previousBlockId;
  }
};
