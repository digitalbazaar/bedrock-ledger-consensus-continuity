/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const _cacheKey = require('./cache-key');
const _election = require('./election');
const _events = require('./events');
const _voters = require('./voters');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const logger = require('./logger');
const subscriber = new cache.Client().client;
const BedrockError = bedrock.util.BedrockError;

module.exports = class ConsensusAgent {
  constructor({gossipAgent, ledgerNode}) {
    this.consensusLockKey = _cacheKey.lockConsensus(ledgerNode.id);
    this.creatorId = null;
    this.gossipAgent = gossipAgent;
    this.ledgerNode = ledgerNode;
    this.halt = false;
    this.onQuit = null;
    this.shuttingDown = false;
    this.messageListener = null;
  }

  start() {
    // subscriber.on('message', this._onMessage.bind(this));
    this.messageListener = this._onMessage.bind(this);
    subscriber.on('message', this.messageListener);

    async.auto({
      creator: callback => _voters.get(
        {ledgerNodeId: this.ledgerNode.id}, (err, result) => {
          if(err) {
            return callback(err);
          }
          this.creatorId = result.id;
          callback();
        }),
      subscribe: ['creator', (results, callback) => subscriber.subscribe(
        'continuity2017.event', callback)]
    }, err => {
      if(err) {
        return logger.debug('Error in consensus agent', {
          err,
          ledgerNodeId: this.ledgerNode.id
        });
      }
      // kick off consensus
      this._lock();
    });
  }

  stop(callback) {
    if(!(callback && typeof callback === 'function')) {
      throw new TypeError('`callback` is required.');
    }

    // unsubscribe listener
    subscriber.removeListener('continuity2017.event', this.messageListener);
    // if a lock is successful, no running process exit immediately
    // otherwise setup for process to exit on completion
    cache.client.setnx(this.consensusLockKey, '', (err, result) => {
      if(result === 1) {
        // call the stop callback
        return cache.client.del(this.consensusLockKey, callback);
      }
      this.onQuit = callback;
      this.halt = true;
    });
  }

  _onMessage(channel, message) {
    if(channel === 'continuity2017.event' && message === 'write') {
      this._lock();
    }
  }

  _quit() {
    this.shuttingDown = true;
    logger.debug('Stopping consensus agent.');
    this.onQuit();
  }

  _lock() {
    async.auto({
      lock: callback => cache.client.setnx(
        this.consensusLockKey, '', (err, result) => {
          if(err) {
            return callback(err);
          }
          if(result === 0) {
            return callback(new BedrockError(
              'Consensus operation in process. Try again.', 'LockError'));
          }
          // lock was sucssful
          callback();
        }),
      extend: ['lock', (results, callback) =>
        this._extendBlockchain(callback)],
      unlock: ['extend', (results, callback) =>
        cache.client.del(this.consensusLockKey, callback)]
    }, err => {
      if(err && err.name === 'LockError') {
        // ignore LockError
      } else if(err) {
        logger.debug('Error in consensus agent.', {
          err,
          ledgerNodeId: this.ledgerNode.id
        });
      }
      if(this.halt && this.onQuit && !this.shuttingDown) {
        return this._quit();
      }
      // try again
      this._lock();
    });
  }

  /**
   * Continually attempts to achieve consensus on existing events, write blocks,
   * gossip with electors, and write new merge events until the work session
   * expires or until there are no events left to achieve consensus on.
   *
   * @param session the current work session.
   * @param ledgerNode the ledger node being worked on.
   * @param voter the voter information for the ledger node.
   * @param callback(err) called once the operation completes.
   */
  _extendBlockchain(callback) {
    const {creatorId, ledgerNode} = this;
    const state = {};
    let done = false;
    const condition = () => {
      return this.halt || done;
    };
    async.until(condition, loop => {
      async.auto({
        initState: callback => {
          if(state.init) {
            return callback();
          }
          state.init = true;
          this._updateState({ledgerNode, state}, callback);
        },
        // finish any incomplete block from the previous cycle
        finishBlock: ['initState', (results, callback) => {
          _blocks.finishIncompleteBlock({ledgerNode, state}, callback);
        }],
        // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
        consensus: ['finishBlock', (results, callback) => this._findConsensus(
          {state}, (err, consensus) => {
            if(err) {
              logger.error('Error in _findConsensus', {error: err});
              return callback(err);
            }
            if(consensus) {
              // immediately loop as there's no known need to merge
              // yet and there may be more consensus to be found
              // (i.e. more blocks to be written)
              return loop();
            }
            // no consensus yet
            done = true;
            callback();
          })],
        merge: ['consensus', (results, callback) => {
          _events.merge({creatorId, ledgerNode}, callback);
        }],
        notify: ['merge', (results, callback) => {
          if(!results.merge) {
            return callback();
          }
          this.gossipAgent.sendNotification(callback);
        }]
      }, loop);
    }, callback);
  }

  /**
   * Gets the latest consensus block and returns the new proposed block height
   * for the ledger (i.e. the current `blockHeight + 1`) and the latest block
   * hash as what would become the next `previousBlockHash`.
   *
   * @param ledgerNode the ledger node to get the latest block for.
   * @param callback(err, {blockHeight, previousBlockHash}) called once the
   *          operation completes.
   */
  _findConsensus({state}, callback) {
    const {creatorId, ledgerNode} = this;
    async.auto({
      history: callback => _events.getRecentHistory(
        {creatorId, ledgerNode, excludeLocalRegularEvents: true},
        callback),
      consensus: ['history', (results, callback) => {
        logger.debug('Starting _extendBlockchain.findConsensus.');
        // Note: DO NOT LOG RESULTS OF FINDCONSENSUS
        _election.findConsensus({
          ledgerNode,
          history: results.history,
          blockHeight: state.blockHeight,
          electors: state.electors
        }, (err, result) => {
          logger.debug('_extendBlockchain.findConsensus complete.');
          if(result) {
            logger.debug('Found consensus.');
            // console.log('CONSENSUS', JSON.stringify(result, null, 2));
          }
          callback(err, result);
        });
      }],
      writeBlock: ['consensus', (results, callback) => {
        if(!results.consensus) {
          return callback(null, false);
        }
        _blocks.write({
          ledgerNode,
          state,
          consensusResult: results.consensus
        }, callback);
      }],
      updateState: ['writeBlock', (results, callback) => {
        this._updateState({ledgerNode, state}, callback);
      }]
    }, (err, results) => err ? callback(err) :
      callback(null, results.consensus));
  }

  /**
   * Get all peers to gossip with. This population will be the electors plus
   * an additional peers associated with the ledger node.
   *
   * @param ledgerNode the ledger node.
   * @param electors the electors.
   * @param callback(err, peers) called once the operation completes.
   */
  _getPeers(ledgerNode, electors, callback) {
    // TODO: in parallel, contact ledgerNode.peerLedgerAgent (and potentially
    // a cache) to get their continuity voter IDs
    callback(null, electors);
  }

  _hasNewRegularEvents(callback) {
    // TODO: need a better check than this -- or we need to ensure that
    //   bogus events will get deleted so they won't get returned here
    //   as valid "new" events for a block
    const collection = this.ledgerNode.storage.events.collection;
    const query = {
      'event.type': {$ne: 'ContinuityMergeEvent'},
      'meta.consensus': {$exists: false},
      'meta.deleted': {$exists: false}
    };
    collection.findOne(query, {_id: 1}, (err, result) =>
      callback(err, !!result));
  }

  _updateState({ledgerNode, state}, callback) {
    async.auto({
      voter: callback => _voters.get({ledgerNodeId: ledgerNode.id}, callback),
      nextBlock: callback => _blocks.getNextBlockInfo(ledgerNode, callback),
      getElectors: ['nextBlock', (results, callback) =>
        _election.getBlockElectors(
          ledgerNode, results.nextBlock.blockHeight, callback)],
      getPeers: ['getElectors', (results, callback) =>
        this._getPeers(ledgerNode, results.getElectors, callback)],
      update: ['getElectors', 'voter', (results, callback) => {
        const voterId = results.voter.id;
        state.electors = results.getElectors;
        state.blockHeight = results.nextBlock.blockHeight;
        state.previousBlockHash = results.nextBlock.previousBlockHash;
        state.previousBlockId = results.nextBlock.previousBlockId;
        state.peers = results.getPeers;

        // track contacted electors
        state.contacted = {};
        const isElector = state.electors.some(e => e.id === voterId);
        if(isElector) {
          // do not contact self
          state.contacted[voterId] = true;
        }
        callback();
      }]
    }, callback);
  }

};
