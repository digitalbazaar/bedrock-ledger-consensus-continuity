/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('../blocks');
const _cacheKey = require('../cache/cache-key');
const _election = require('../election');
const _events = require('../events');
const _voters = require('../voters');
const async = require('async');
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
  }

  _onQuit() {
    this.subscriber.quit();
  }

  _workLoop() {
    this.messageListener = this._onMessage.bind(this);
    async.auto({
      // subscribing, but not adding an event handler
      subscribe: callback => this.subscriber.subscribe(
        `continuity2017|event|${this.ledgerNode.id}`, callback)
    }, err => {
      if(err) {
        return this._quit(err);
      }
      // important to start worker right away to catch regular events that
      // may have already been added
      this._work();
    });
  }

  _onMessage(channel, message) {
    if(message === 'merge' && !(this.working || this.halt)) {
      this.working = true;
      this.subscriber.removeListener('message', this.messageListener);
      setTimeout(() => this._work(), this.debounce);
    }
  }

  _work() {
    this.working = true;
    this._extendBlockchain(err => {
      this.working = false;
      if(err) {
        return this._quit(err);
      }
      if(this.halt) {
        return this._quit();
      }
      this.subscriber.on('message', this.messageListener);
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
    const state = {};
    let done = false;
    const condition = () => {
      return this.halt || done;
    };
    async.until(condition, loop => {
      async.auto({
        initState: callback => {
          // if(state.init) {
          //   return callback();
          // }
          // state.init = true;
          this._updateState({state}, callback);
        },
        // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
        consensus: ['initState', (results, callback) => this._findConsensus(
          {state}, (err, result) => {
            if(err) {
              logger.error('Error in _findConsensus', {error: err});
              return callback(err);
            }
            if(result.writeBlock && result.writeBlock.hasLedgerConfigEvent) {
              return callback(new BedrockError(
                'Ledger configuration change detected.',
                'LedgerConfigurationChangeError', {
                  blockHeight: result.writeBlock.blockHeight
                }));
            }
            if(result.consensus) {
              // immediately loop as there's no known need to merge
              // yet and there may be more consensus to be found
              // (i.e. more blocks to be written)
              return loop();
            }
            // no consensus yet
            done = true;
            callback();
          })],
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
    const {ledgerNode, ledgerNodeId} = this;
    async.auto({
      creator: callback => _voters.get({ledgerNodeId}, callback),
      history: ['creator', (results, callback) => _events.getRecentHistory({
        creatorId: results.creator.id,
        excludeLocalRegularEvents: true,
        ledgerNode,
      }, callback)],
      consensus: ['history', (results, callback) => {
        logger.verbose('Starting _extendBlockchain.findConsensus.');
        // Note: DO NOT LOG RESULTS OF FINDCONSENSUS
        _election.findConsensus({
          ledgerNode,
          history: results.history,
          blockHeight: state.blockHeight,
          electors: state.electors
        }, (err, result) => {
          logger.verbose('_extendBlockchain.findConsensus complete.');
          if(result) {
            logger.verbose('Found consensus.');
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
      // updateState: ['writeBlock', (results, callback) => {
      //   this._updateState({ledgerNode, state}, callback);
      // }]
    }, (err, results) => err ? callback(err) : callback(null, {
      consensus: results.consensus,
      writeBlock: results.writeBlock
    }));
  }

  _updateState({state}, callback) {
    const {ledgerNode, ledgerNodeId} = this;
    async.auto({
      nextBlock: callback => _blocks.getNextBlockInfo(ledgerNode, callback),
      cachedBlockHeight: callback => {
        const blockHeightKey = _cacheKey.blockHeight(ledgerNodeId);
        cache.client.get(blockHeightKey, (err, result) => {
          if(err) {
            return callback(err);
          }
          if(result === null) {
            return callback(new BedrockError(
              'Block height key is missing from the cache.',
              'InvalidStateError', {ledgerNodeId}));
          }
          callback(null, parseInt(result, 10));
        });
      },
      consistencyCheck: [
        'cachedBlockHeight', 'nextBlock', (results, callback) => {
          const {cachedBlockHeight, nextBlock} = results;
          const {blockHeight: nextBlockHeight} = nextBlock;
          const blockHeight = nextBlockHeight - 1;
          if(blockHeight === cachedBlockHeight) {
            // success
            return callback();
          }
          // this should never happen
          if((blockHeight - cachedBlockHeight) !== 1) {
            return callback(new BedrockError(
              'Cache is behind by more than one block.', 'InvalidStateError',
              {blockHeight, cachedBlockHeight, ledgerNodeId}));
          }
          // inconsistency needs to be repaired
          logger.debug('BLOCKS REPAIRCACHE');
          _blocks.repairCache({blockHeight, ledgerNode}, callback);
        }],
      getElectors: ['consistencyCheck', (results, callback) =>
        _election.getBlockElectors(
          {ledgerNode, blockHeight: results.nextBlock.blockHeight}, callback)],
      update: ['getElectors', (results, callback) => {
        state.electors = results.getElectors;
        state.blockHeight = results.nextBlock.blockHeight;
        state.previousBlockHash = results.nextBlock.previousBlockHash;
        state.previousBlockId = results.nextBlock.previousBlockId;
        callback();
      }]
    }, callback);
  }

};
