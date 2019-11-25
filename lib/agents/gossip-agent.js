/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _client = require('../client');
const _events = require('../events');
const _gossip = require('../gossip');
const _voters = require('../voters');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config, util: {callbackify, delay, BedrockError}} = bedrock;
const logger = require('../logger');
const ContinuityAgent = require('./continuity-agent');
const GossipPeerSelector = require('./GossipPeerSelector');

const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;

module.exports = class GossipAgent extends ContinuityAgent {
  constructor({agentName, ledgerNode}) {
    agentName = agentName || 'gossip';
    super({agentName, ledgerNode});
    this.creatorId = null;
    this.peerSelector = null;
    this.sendNotification = callbackify(this.sendNotification.bind(this));
    this.messageListener = null;
    this.subscriber = new cache.Client().client;
    this.resumeGossip = null;
    this.gossipRequested = false;
  }

  _onQuit() {
    this.subscriber.quit();
  }

  async _workLoop() {
    this.working = true;
    this.messageListener = this._onMessage.bind(this);
    const {ledgerNode, ledgerNodeId} = this;
    this.creatorId = (await _voters.get({ledgerNodeId})).id;
    this.peerSelector = new GossipPeerSelector(
      {creatorId: this.creatorId, ledgerNode});
    try {
      await this.subscriber.subscribe(
        `continuity2017|needsGossip|${ledgerNodeId}`);
      this.subscriber.on('message', this.messageListener);
      await this._gossipCycle();
    } catch(err) {
      this.working = false;
      return this._quit(err);
    }
    this.working = false;
    this._quit();
  }

  _onMessage() {
    if(this.resumeGossip) {
      this.gossipRequested = true;
      this.resumeGossip();
    }
  }

  async _gw({peer}) {
    const {ledgerNode, ledgerNodeId} = this;
    let result;
    let err;
    do {
      try {
        result = await _gossip.gossipWith(
          {callerId: this.creatorId, ledgerNode, peer});
      } catch(e) {
        err = e;
        // if there is an error with one peer, do not stop cycle
        logger.debug('non-critical error in gossip', {err, peer});
      }
      // process events acquired from peer
      if(result && result.events) {
        const {events, needed} = result;
        try {
          // hash events and validate ancestry of all the events in the DAG
          const {eventMap} = await _events.prepBatch(
            {events, ledgerNode, needed});
          let deferredEvents = events;
          let processedEventHashes;
          // validate events and operations using ledger validators deferring
          // processing of events until the local node has generated the
          // blockHeight required by the event's basisBlockHeight
          let previousDeferredEvents;
          let tries = 0;
          const maxRetries = 5;
          do {
            const {eventBlock: {block: {blockHeight}}} =
              await ledgerNode.blocks.getLatestSummary();
            ({deferredEvents, processedEventHashes} = await _events.addBatch({
              blockHeight, events: deferredEvents, eventMap, ledgerNode,
              processedEventHashes
            }));

            const deferredEventsLength = deferredEvents.length;
            if(deferredEventsLength > 0) {
              if(previousDeferredEvents === undefined) {
                previousDeferredEvents = deferredEventsLength;
              }
              if(previousDeferredEvents === deferredEventsLength) {
                // no progress, increment tries
                tries++;
              } else {
                // progress has been made, reset tries
                tries = 1;
              }
              if(tries > maxRetries) {
                throw new BedrockError('addBatch maxRetries exceeded.',
                  'TimeoutError', {
                    blockHeight,
                    firstDeferredEvent: deferredEvents[0],
                  });
              }
              previousDeferredEvents = deferredEventsLength;

              // if events were deferred, wait for a new block and retry
              // TODO: it may be possible to listen for a
              // bedrock-ledger-storage.block.add event, but possible race
              // conditions may make this unnecessarily complicated
              await delay(250);
            }
          } while(deferredEvents.length > 0 && !this.halt);
        } catch(error) {
          logger.error(
            'An error occurred in gossip batch processing.', {error});
          result = {done: true, err: error};
          continue;
        }
      }
      result = result || {done: true, err};
    } while(!(result.done || this.halt));

    if(err && err.name !== 'TimeoutError') {
      await peer.fail(err);
    } else {
      // TODO: do not fail peers exceeding maxRetries, but in the future
      // back-off peers that repeatedly fail max retries
      await peer.success();
    }

    if(result.done && !err) {
      // clear gossipBehind
      return _cache.gossip.gossipBehind({ledgerNodeId, remove: true});
    }
    // set gossipBehind
    return _cache.gossip.gossipBehind({ledgerNodeId});
  }

  async _gossipCycle() {
    const {ledgerNodeId} = this;
    while(!this.halt) {
      this.gossipRequested = false;

      // grab all merge permits to delay local merges until after gossip
      const {received} = await _cache.events.requestMergePermits(
        {ledgerNodeId, permits: Infinity});
      if(received === 1) {
        // release merge permit back for merge agent to use; always ensure
        // a merge can happen so consensus will run again
        await _cache.events.releaseMergePermits({ledgerNodeId, permits: 1});
      }
      if(received <= 1) {
        // no permits available, create a promise to wait until gossip is
        // needed again (timing out and checking again every 250ms)
        await new Promise(resolve => {
          this.resumeGossip = resolve;
          setTimeout(() => resolve(), 250);
        });
        continue;
      }

      const peer = await this.peerSelector.getPeer();
      if(peer) {
        // TODO: pass in `permits` to limit received merge events
        await this._gw({peer});
      }

      // FIXME: permits should instead be used in `this._gw` and only those
      // that are not consumed should be added back here
      await _cache.events.releaseMergePermits(
        {ledgerNodeId, permits: received - 2});

      // notify merge agent that more gossip is available for merging
      await cache.client.publish(
        `continuity2017|needsMerge|${ledgerNodeId}`, 'gossip');

      if(!peer && !this.gossipRequested) {
        // no peers to gossip with and no request for gossip, cool off
        await delay(coolDownPeriod);
      }
    }
  }

  async sendNotification() {
    const notified = new Set();
    let sent = 0;
    let attempts = 0;
    const maxRetries = 10;
    // attempt to send notifications to two distinct peers
    while(sent < 2 && attempts < maxRetries) {
      attempts++;
      const peer = await this.peerSelector.getPeer({toNotify: true});
      if(!peer) {
        // either there are no peers, or they are all currently failed out
        break;
      }
      const {creatorId: peerId} = peer;
      if(!notified.has(peerId)) {
        notified.add(peerId);
        try {
          await _client.notifyPeer({callerId: this.creatorId, peerId});
          // FIXME: need to track success/fail network requests separate from
          // success/fail related to gossip validation, for now, do not reset
          // the peer on a successful notification
          // await peer.success();
          sent++;
        } catch(e) {
          await peer.fail(e);
        }
      }
    }
  }
};
