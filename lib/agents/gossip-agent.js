/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('../cache');
const _client = require('../client');
const _events = require('../events');
const _gossip = require('../gossip');
const _voters = require('../voters');
const bedrock = require('bedrock');
const {config, util: {callbackify, delay}} = bedrock;
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
  }

  async _workLoop() {
    this.working = true;
    const {ledgerNode} = this;
    const ledgerNodeId = ledgerNode.id;
    this.creatorId = (await _voters.get({ledgerNodeId})).id;
    this.peerSelector = new GossipPeerSelector(
      {creatorId: this.creatorId, ledgerNode});
    try {
      await this._gossipCycle();
    } catch(err) {
      this.working = false;
      return this._quit(err);
    }
    this.working = false;
    this._quit();
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
          await _events.addBatch({events, ledgerNode, needed});
        } catch(e) {
          logger.error('An error occurred in `addBatch`', {error: e});
          throw e;
        }
      }
      result = result || {done: true, err};
    } while(!(result.done || this.halt));
    if(result.done && !err) {
      // clear gossipBehind
      return _cache.gossip.gossipBehind({ledgerNodeId, remove: true});
    }
    // set gossipBehind
    return _cache.gossip.gossipBehind({ledgerNodeId});
  }

  async _gossipCycle() {
    while(!this.halt) {
      const peer = await this.peerSelector.getPeer();
      if(peer) {
        await this._gw({peer});
      } else {
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
          await peer.success();
          sent++;
        } catch(e) {
          await peer.fail(e);
        }
      }
    }
  }
};
