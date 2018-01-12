/*
 * Web Ledger Continuity2017 consensus worker.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brLedgerNode = require('bedrock-ledger-node');
const logger = require('./logger');
const BedrockError = bedrock.util.BedrockError;
const _gossip = require('./gossip');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._election = require('./election');
api._events = require('./events');
api._hasher = brLedgerNode.consensus._hasher;
api._storage = require('./storage');
api._voters = require('./voters');
// exposed for testing
api._gossipWith = _gossip.gossipWith;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('./temporaryLedgerNodeMeta');

api.scheduleWork = (session) => {
  // start a consensus session for ledgers
  const maxTime =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  session.start(maxTime, _guardedSync, err => {
    if(err) {
      logger.error('Error starting consensus job.', err);
    }
  });
};

// Note: exposed for testing
let _testMode = false;
api._run = (ledgerNode, callback) => {
  _testMode = true;
  _sync({
    ledgerNode: ledgerNode,
    isExpired: () => false,
    timeRemaining: () => Infinity
  }, callback);
};

function _guardedSync(session, callback) {
  // do not allow sync until `waitUntil` time
  _ledgerNodeMeta.get(session.ledgerNode.id, (err, meta) => {
    if(err) {
      return callback(err);
    }
    const waitUntil = _.get(meta, 'consensus-continuity.waitUntil');
    if(waitUntil && waitUntil > Date.now()) {
      // do not run consensus yet
      logger.verbose('consensus job delaying until ' + new Date(waitUntil),
        {ledgerNodeId: session.ledgerNode.id, waitUntil: waitUntil});
      return callback();
    }
    // ready to run consensus
    _sync(session, callback);
  });
}

function _sync(session, callback) {
  const ledgerNode = session.ledgerNode;
  logger.verbose('consensus job running', {ledgerNodeId: ledgerNode.id});
  logger.debug('consensus job ledgerNode', {ledgerNodeId: ledgerNode.id});
  logger.debug(
    'consensus job blockCollection',
    {blockCollection: ledgerNode.storage.blocks.collection.s.name});
  async.auto({
    voter: callback => api._voters.get(ledgerNode.id, callback),
    extend: ['voter', (results, callback) =>
      _extendBlockchain({
        session,
        ledgerNode,
        voter: results.voter
      }, callback)]
  }, err => {
    logger.debug('Work session completed.', {
      session: session.id
    });
    callback(err);
  });
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
function _getNextBlockInfo(ledgerNode, callback) {
  // Note: This consensus method assumes that `blockHeight` will always exist
  // on the previous block because it cannot be used on a blockchain that
  // does not have that information. There has presently been no mechanism
  // devised for switching consensus methods between hashgraph-like blocks
  // and typical blockchains with block heights.
  ledgerNode.storage.blocks.getLatestSummary((err, block) => {
    if(err) {
      return callback(err);
    }
    const previousBlockHash =
      _.get(block, 'eventBlock.meta.blockHash');
    const last = _.get(block, 'eventBlock.block.blockHeight');
    if(last === undefined) {
      return callback(new BedrockError(
        'blockHeight is missing from latest block.', 'NotFoundError', {
          block
        }));
    }
    callback(null, {blockHeight: last + 1, previousBlockHash});
  });
}

/**
 * Get all peers to gossip with. This population will be the electors plus
 * an additional peers associated with the ledger node.
 *
 * @param ledgerNode the ledger node.
 * @param electors the electors.
 * @param callback(err, peers) called once the operation completes.
 */
function _getPeers(ledgerNode, electors, callback) {
  // TODO: in parallel, contact ledgerNode.peerLedgerAgent (and potentially
  // a cache) to get their continuity voter IDs
  callback(null, electors);
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
function _extendBlockchain({session, ledgerNode, voter}, callback) {
  // continue extending the blockchain until the work session expires or until
  // there is no more data in the system to achieve consensus on where we will
  // delay future work scheduling until the given gossip interval
  const gossipInterval =
    config['ledger-consensus-continuity'].worker.election.gossipInterval;

  // condition for stopping work is some forced abort (aborted=true) or
  // the work session has insufficient time remaining to do any of its tasks
  // where if such a task were run concurrently, it would cause a consistency
  // failure that must be avoided (therefore, 10000ms is reserved as more
  // than enough time for its longest such task)
  let aborted = false;
  const condition = () =>
    aborted ||
    // must provide enough time for longest non-concurrent task
    session.timeRemaining() < 10000;

  // will contain blockHeight and electors
  const state = {};

  // create callback that will abort current `loop` and postpone further
  // extending of the block chain; scheduling of the next work session
  // will be contingent upon the presence of new events that require
  // consensus
  let consensusRequired = false;
  const abort = loop => {
    aborted = true;

    if(_testMode) {
      // in test mode we call the loop callback (which will cause us to break
      // out since the break condition has been met) without writing any
      // future scheduling information to the database
      return loop();
    }

    if(consensusRequired) {
      // consensus required (e.g. new events have been found that need to
      // get into a block) so reschedule as soon as possible, do not wait for
      // `gossipInterval`
      logger.debug(
        'New events detected; rescheduling as soon as possible.', {
          ledgerNodeId: ledgerNode.id,
          blockHeight: state.blockHeight
        });
      return loop();
    }

    // schedule the next work session for after the gossip interval and then
    // call loop callback to break out
    logger.debug(
      'Consensus not required (e.g., no new events detected) and ' +
      'work session expired; idling until gossip interval expires.', {
        blockHeight: state.blockHeight,
        gossipInterval,
        timeStamp: Date.now()
      });
    _ledgerNodeMeta.setWaitUntil(
      ledgerNode.id, Date.now() + gossipInterval, loop);
  };

  // Note: Keep gossiping until:
  // 1. Work session expires.
  // 2. No consensus found, contacted every elector, at least one merge event
  //    has been attempted, and either:
  //    2.1. There are no regular events that require consensus.
  //    2.2. Test mode is engaged, so the loop should terminate to allow
  //         the test to run other workers.
  let mergeAttempted = false;
  async.until(condition, loop => {
    // loop:
    // 1. Continually find consensus and write blocks until consensus
    //    cannot be found.
    // 2. Run gossip.
    // 3. Create a merge event.
    async.auto({
      // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
      consensus: callback => _findConsensus(
        {ledgerNode, voter, state, condition}, (err, consensus) => {
          if(err) {
            logger.error('Error in _findConsensus', {error: err});
            return callback(err);
          }

          if(consensus) {
            // consensus found, clear required check
            consensusRequired = false;
            // immediately loop as there's no known need to gossip or merge
            // merge yet and there may be more consensus to be found
            // (i.e. more blocks to be written)
            return loop();
          }

          // if all electors have been contacted and at least one merge
          // event has been attempted...
          const contacted = Object.keys(state.contacted).length;
          if(contacted === state.electors.length && mergeAttempted) {
            if(!consensusRequired) {
              // no consensus required, so abort
              logger.debug(
                'gossiped with all electors and no consensus required, ' +
                'aborting session');
              return abort(loop);
            }
            // in test mode, we always abort after no consensus has been found,
            // all electors have been contacted, and at least one merge was
            // attempted... this approach allows tests to run other workers
            //if(_testMode) {
              return abort(loop);
            //}
          }
          // no consensus yet and it is either required or we haven't talked
          // with all the electors yet, so continue to gossip
          callback();
        }),
      pullGossip: ['consensus', (results, callback) => _contactElectors({
        blockHeight: state.blockHeight,
        condition,
        contacted: state.contacted,
        creatorId: voter.id,
        electors: state.electors,
        ledgerNode,
        peers: state.peers,
        previousBlockHash: state.previousBlockHash,
      }, callback)],
      consensusRequired: ['pullGossip', (results, callback) => {
        // consensus already required, return true
        if(consensusRequired) {
          return callback(null, true);
        }
        // this check determines if there are any outstanding regular events
        // that need to get into a block (i.e. we need to find consensus); if
        // we don't need to find consensus and we have chatted with every
        // elector, we will stop trying to extend the blockchain and not
        // create any unnecessary merge events
        _hasNewRegularEvents({ledgerNode}, (err, newRegularEvents) => {
          if(err) {
            return callback(err);
          }
          callback(null, consensusRequired = newRegularEvents);
        });
      }],
      merge: ['consensusRequired', (results, callback) => {
        if(condition()) {
          // insufficient time to merge
          logger.debug('insufficient time remaining to merge');
          return abort(loop);
        }
        mergeAttempted = true;
        if(!consensusRequired) {
          // do not create a merge event if no consensus is required
          return callback();
        }
        _merge({ledgerNode, voter, blockHeight: state.blockHeight}, callback);
      }],
      pushGossip: ['merge', (results, callback) => {
        logger.debug('Starting push gossip.');
        // FIXME: EXPERIMENTAL GOSSIP IN SERIES
        // async.series(results.pullGossip, callback);
        async.parallel(results.pullGossip, callback);
      }]
    }, loop);
  }, callback);
}

function _findConsensus({ledgerNode, voter, state, condition}, callback) {
  async.auto({
    initState: callback => {
      if(state.init) {
        return callback();
      }
      state.init = true;
      _updateState({ledgerNode, state, voter}, callback);
    },
    history: callback => api._events.getRecentHistory(
      {creatorId: voter.id, ledgerNode, excludeLocalRegularEvents: true},
      callback),
    consensus: ['initState', 'history', (results, callback) => {
      logger.debug('Starting _extendBlockchain.findConsensus.');
      // Note: DO NOT LOG RESULTS OF FINDCONSENSUS
      api._election.findConsensus({
        ledgerNode,
        history: results.history,
        blockHeight: state.blockHeight,
        electors: state.electors
      }, (err, result) => {
        logger.debug('_extendBlockchain.findConsensus complete.');
        if(result) {
          logger.debug('Found consensus.');
          /*console.log('FOUND CONSENSUS', ledgerNode.id,
            'AT BLOCK', blockHeight,
            'events:', result.event.length + '\n\n');*/
        }
        callback(err, result);
      });
    }],
    writeBlock: ['consensus', (results, callback) => {
      if(!results.consensus || condition()) {
        return callback(null, false);
      }
      _writeBlock({
        ledgerNode,
        blockHeight: state.blockHeight,
        consensusResult: results.consensus
      }, callback);
    }],
    updateState: ['writeBlock', (results, callback) => {
      if(!results.writeBlock) {
        return callback();
      }
      _updateState({ledgerNode, state, voter}, callback);
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.consensus));
}

function _updateState({ledgerNode, state, voter}, callback) {
  async.auto({
    nextBlock: callback => _getNextBlockInfo(ledgerNode, callback),
    getElectors: ['nextBlock', (results, callback) =>
      api._election.getBlockElectors(
        ledgerNode, results.nextBlock.blockHeight, callback)],
    getPeers: ['getElectors', (results, callback) =>
      _getPeers(ledgerNode, results.getElectors, callback)],
    update: ['getElectors', (results, callback) => {
      state.electors = results.getElectors;
      state.blockHeight = results.nextBlock.blockHeight;
      state.previousBlockHash = results.nextBlock.previousBlockHash;
      state.peers = results.getPeers;

      // track contacted electors
      state.contacted = {};
      const isElector = state.electors.some(e => e.id === voter.id);
      if(isElector) {
        // do not contact self
        state.contacted[voter.id] = true;
      }
      callback();
    }]
  }, callback);
}

function _merge({ledgerNode, voter}, callback) {
  async.auto({
    history: callback => api._events.getRecentHistory(
      {creatorId: voter.id, ledgerNode}, callback),
    // NOTE: mergeBranches mutates history
    merge: ['history', (results, callback) => api._events.mergeBranches(
      {history: results.history, ledgerNode}, callback)]
  }, (err, results) => err ? callback(err) : callback(err, results.merge));
}

function _hasNewRegularEvents({ledgerNode}, callback) {
  // TODO: need a better check than this -- or we need to ensure that
  //   bogus events will get deleted so they won't get returned here
  //   as valid "new" events for a block
  const collection = ledgerNode.storage.events.collection;
  const query = {
    'event.type': {$ne: 'ContinuityMergeEvent'},
    'meta.consensusDate': {$exists: false},
    'meta.deleted': {$exists: false}
  };
  collection.findOne(query, {_id: 1}, (err, result) => callback(err, !!result));
}

function _contactElectors({
  blockHeight, condition, contacted = {}, electors, ledgerNode,
  previousBlockHash, creatorId
}, callback) {
  // maximum number of peers to communicate with at once
  const limit =
    bedrock.config['ledger-consensus-continuity'].gossip.concurrentPeers;

  // TODO: track electors that are consistently hard to get in contact with
  // and avoid them

  const isElector = electors.some(e => e.id === creatorId);

  // restrict electors to contact to 1/3 of total or limit
  const toContact = electors.filter(e => !(e.id in contacted));
  const maxLength = Math.min(
    toContact.length, Math.ceil(electors.length / 3), limit);
  toContact.length = maxLength;

  // 1. Contact N electors at random, in parallel.
  const random = _.shuffle(toContact);
  logger.debug('ELECTORS TO BE CONTACTED', {random});
  const pushFunctions = [];
  // FIXME: trying gossip in series
  // async.eachSeries(random, (elector, nextElector) => {
  async.eachLimit(random, limit, (elector, nextElector) => {
    // no need to gossip if condition met
    if(condition()) {
      return nextElector();
    }
    // 2. Gossip with a single elector.
    _gossip.gossipWith({
      blockHeight, isElector, ledgerNode, peerId: elector.id, previousBlockHash
    }, (err, result) => {
      // ignore communication error from a single voter
      if(err) {
        // TODO: revert to verbose
        // logger.verbose('Non-critical error in _gossipWith.', err);
        logger.debug('Non-critical error in _gossipWith.', {err});
        contacted[elector.id] = err;
      } else {
        pushFunctions.push(result.pushGossip);
        contacted[elector.id] = true;
      }
      nextElector();
    });
  }, err => err ? callback(err) : callback(null, pushFunctions));
}

// TODO: document
// consensusResult = {event: [event records], consensusProof: [event records]}
function _writeBlock({ledgerNode, blockHeight, consensusResult}, callback) {
  async.auto({
    config: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) => {
        if(err) {
          return callback(err);
        }
        const config = result.event.ledgerConfiguration;
        if(config.consensusMethod !== 'Continuity2017') {
          return callback(new BedrockError(
            'Consensus method must be "Continuity2017".',
            'InvalidStateError', {
              consensusMethod: config.consensusMethod
            }));
        }
        callback(null, config);
      }),
    updateEvents: callback =>
      async.each(consensusResult.event, (record, callback) => {
        const now = Date.now();
        ledgerNode.storage.events.update(record.eventHash, [{
          op: 'unset',
          changes: {
            meta: {
              pending: true
            }
          }
        }, {
          op: 'set',
          changes: {
            meta: {
              consensus: true,
              consensusDate: now,
              updated: now
            }
          }
        }], callback);
      }, callback),
    keys: callback => {
      const signatureCreators = _.uniq(
        // FIXME: there are regular events included here that do not include
        // signatures, using filter now, better way?
        consensusResult.event.filter(r => r.event.signature)
          .map(r => r.event.signature.creator)
          .concat(consensusResult.consensusProof
            .map(r => r.signature.creator)));
      async.map(
        signatureCreators, (keyId, callback) => api._storage.keys.getPublicKey(
          ledgerNode.id, {id: keyId}, callback), (err, result) => {
          if(err) {
            return callback(err);
          }
          const publicKey = result.map(key => key.seeAlso ?
            {id: key.id, seeAlso: key.seeAlso} : {
              id: key.id,
              type: key.type,
              owner: key.owner,
              publicKeyPem: key.publicKeyPem
            });
          callback(null, publicKey);
        });
    },
    previousBlock: callback =>
      ledgerNode.storage.blocks.getLatestSummary(callback),
    createBlock: ['config', 'keys', 'previousBlock', 'updateEvents',
      (results, callback) => {
        const block = {
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          id: results.config.ledger + '/blocks/' + blockHeight,
          blockHeight,
          consensusMethod: 'Continuity2017',
          type: 'WebLedgerEventBlock',
          event: consensusResult.event.map(r => r.event),
          consensusProof: consensusResult.consensusProof,
          previousBlock: results.previousBlock.eventBlock.block.id,
          previousBlockHash: results.previousBlock.eventBlock.meta.blockHash,
          publicKey: results.keys
        };
        api._hasher(block, (err, blockHash) => {
          if(err) {
            return callback(err);
          }
          // convert events to event hashes
          block.event = consensusResult.event.map(r => r.eventHash);

          // TODO: ensure storage supports `consensusProof` event hash lookup
          // FIXME: the aforementioned TODO has not been completed, and
          // election.getBlockElectors is expecting full events, removing
          // the hash substitution for now
          // block.consensusProof = consensusResult.consensusProofHash;

          callback(null, {
            block: block,
            meta: {blockHash}
          });
        });
      }],
    store: ['createBlock', (results, callback) =>
      ledgerNode.storage.blocks.add(
        results.createBlock.block, results.createBlock.meta, callback)],
    updateKey: ['store', (results, callback) => {
      const toUpdate = results.keys
        .filter(key => !key.seeAlso)
        .map(key => {
          key.seeAlso = results.createBlock.block.id;
          return key;
        });
      async.each(toUpdate, (key, callback) =>
        api._storage.keys.updatePublicKey(
          ledgerNode.id, key, callback), callback);
    }],
    updateBlock: ['updateKey', (results, callback) => {
      const patch = [{
        op: 'set',
        changes: {meta: {consensus: true, consensusDate: Date.now()}}
      }];
      const blockHash = results.createBlock.meta.blockHash;
      ledgerNode.storage.blocks.update(blockHash, patch, callback);
    }]
  }, err => callback(err, err ? false : true));
}
