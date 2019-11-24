/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _continuityConstants = require('../continuityConstants');
const _events = require('../events');
const _voters = require('../voters');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const logger = require('../logger');
const {BedrockError} = bedrock.util;
const ContinuityAgent = require('./continuity-agent');

module.exports = class MergeAgent extends ContinuityAgent {
  constructor({agentName, gossipAgent, ledgerNode}) {
    agentName = agentName || 'merge';
    super({agentName, ledgerNode});
    this.creatorId = null;
    this.config = config['ledger-consensus-continuity'].merge;
    this.gossipAgent = gossipAgent;
    this.messageListener = null;
    this.subscriber = new cache.Client().client;
    // initialize needing a merge event to force agent to merge in
    // any information that arrived while the agent was not running
    this.needsMerge = true;
  }

  _onQuit() {
    this.subscriber.quit();
  }

  async _workLoop() {
    this.messageListener = this._onMessage.bind(this);
    const {ledgerNodeId} = this;
    try {
      this.creatorId = (await _voters.get({ledgerNodeId})).id;
      await this.subscriber.subscribe(
        `continuity2017|needsMerge|${ledgerNodeId}`);
    } catch(e) {
      return this._quit(e);
    }
    this.subscriber.on('message', this.messageListener);
    // important to start worker right away to catch regular events or
    // operations that may have already been added
    this._work();
  }

  async _onMessage() {
    // this node should only merge local regular events and whenever a merge is
    // needed by another agent
    if(this.working) {
      // already working, notify new merge is needed
      this.needsMerge = true;
    } else if(!this.halt) {
      this.working = true;
      this.needsMerge = true;
      // used a fixed debounce interval from the config
      // TODO: remove debounce, use setImmediate()
      return setTimeout(() => this._work(), this.config.fixedDebounce);
    }
  }

  async _validateCache() {
    const {creatorId, ledgerNode, ledgerNodeId} = this;
    const [cacheHead, mongoHead] = await Promise.all([
      _events.getHead({creatorId, ledgerNode}),
      _events.getHead({creatorId, ledgerNode, useCache: false})
    ]);
    if(_.isEqual(cacheHead, mongoHead)) {
      // success
      return;
    }
    // this should never happen
    if((mongoHead.generation - cacheHead.generation) !== 1) {
      throw new BedrockError(
        'Cache is behind by more than one merge event.',
        'InvalidStateError',
        {cacheHead, mongoHead, ledgerNodeId});
    }
    const {eventHash} = mongoHead;
    await _events.repairCache({eventHash, ledgerNode});
  }

  async _work() {
    this.working = true;
    const {creatorId, ledgerNode, ledgerNodeId} = this;
    const {mergeEvents: {maxEvents}} = _continuityConstants;
    const maxPeerEvents = Math.ceil(maxEvents / 2);

    while(!this.halt && this.needsMerge) {
      let result;
      let notifyFlag;
      try {
        // consume `needsMerge` flag before doing any asynchronous calls to
        // enable concurrent notifications to keep this loop going
        this.needsMerge = false;

        // get gossip notification flag to see if we haven't notified
        // peers yet
        notifyFlag = await _cache.gossip.notifyFlag({ledgerNodeId});

        // ensure cache and mongo are in sync
        await this._validateCache();

        // get merge status
        const mergeStatus = await _events.getMergeStatus(
          {ledgerNode, creatorId, checkOperations: true});
        const {
          mergeable, peerChildlessHashes, localChildlessHashes
        } = mergeStatus;

        // if can't merge yet, continue; we'll run again when another agent
        // (e.g., the gossip agent) indicates a merge is needed
        if(!mergeable) {
          continue;
        }

        // ready to merge... get a permit to merge
        const {received} = await _cache.events.requestMergePermits(
          {ledgerNodeId, permits: 1});
        if(received === 0) {
          // can't merge yet, so loop
          continue;
        }

        // up to 50% of parent spots are for peer merge events, one spot is
        // for `treeHash` and remaining spots will be filled with regular
        // events; creating them on demand here
        const peerEventCount = Math.min(
          maxPeerEvents, peerChildlessHashes.length);
        let maxRegularEvents = maxEvents - peerEventCount - 1 -
          localChildlessHashes.length;
        while(maxRegularEvents > 0 && !this.halt) {
          const {hasMore} = await _events.create({ledgerNode});
          if(!hasMore) {
            break;
          }
          maxRegularEvents--;
        }

        // do the merge
        if(!this.halt) {
          result = await _events.merge({creatorId, ledgerNode, mergeStatus});
        }
      } catch(e) {
        this.working = false;
        return this._quit(e);
      }
      // there is a new merge event, or the last notification attempt
      // did not result in a gossip session
      // it is imperative that notifications eventually result in a gossip
      // session in order for the local node to have its events included
      // in the ledger
      if(result || notifyFlag !== null) {
        try {
          await Promise.all([
            _cache.gossip.notifyFlag({add: true, ledgerNodeId}),
            this.gossipAgent.sendNotification()
          ]);
        } catch(e) {
          // just log the error, another attempt will be made on the next cycle
          logger.error(
            'An error occurred while attempting to send merge notification.',
            {error: e});
        }
      }
    }
    this.working = false;
    if(this.halt) {
      return this._quit();
    }
  }
};
