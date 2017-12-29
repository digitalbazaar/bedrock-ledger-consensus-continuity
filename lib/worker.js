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
    nextBlock: callback => _getNextBlockInfo(ledgerNode, callback),
    getElectors: ['nextBlock', (results, callback) => {
      api._election.getBlockElectors(
        ledgerNode, results.nextBlock.blockHeight, callback);
    }],
    getPeers: ['getElectors', (results, callback) => {
      _getPeers(ledgerNode, results.getElectors, callback);
    }],
    election: ['voter', 'getElectors', (results, callback) =>
      _runElection({
        session,
        ledgerNode,
        voter: results.voter,
        previousBlockHash: results.nextBlock.previousBlockHash,
        blockHeight: results.nextBlock.blockHeight,
        electors: results.getElectors,
        peers: results.getPeers
      }, callback)],
    writeBlock: ['election', (results, callback) => {
      if(!results.election.consensus || session.isExpired()) {
        return callback(null, false);
      }
      _writeBlock({
        ledgerNode,
        blockHeight: results.nextBlock.blockHeight,
        consensusResult: results.election.consensus
      }, callback);
    }]
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
 * Runs an election to decide the events and votes to write into the next block.
 *
 * This function will determine both an event manifest and a roll call manifest.
 * An event manifest is an ordered list of event hashes and a roll call manifest
 * is an ordered list of elector hashes. It is possible for a `null` manifest to
 * be returned. This occurs when the work session has expired before the
 * election was over. Another work session will have to run the election later
 * to determine the winning manifest.
 *
 * Running the election involves:
 *
 * 1. Gossiping events with a voting population,
 * 2. Gathering their votes until a 2/3rds majority agrees upon the next
 *    event manifest,
 * 3. Agreeing on who voted to form the elector roll call.
 *
 * Once an event manifest has been elected, the corresponding events will need
 * to be run through any appropriate event validators to produce the final
 * consensus block, which is work that is not performed by this function.
 *
 * @param session the current work session.
 * @param ledgerNode the ledger node being worked on.
 * @param voter the voter information for the ledger node.
 * @param previousBlockHash the hash for the block to build on.
 * @param blockHeight the height of the block for the next manifest.
 * @param electors the voting population that will select the next manifest.
 * @param peers all peers to gossip with (all electors plus any additional
 *          peers associated with the ledger node).
 * @param callback(err, electionResult) called once the operation completes.
 */
function _runElection({
  session,
  ledgerNode,
  voter,
  previousBlockHash,
  blockHeight,
  electors,
  peers
}, callback) {
  // continue gossiping with the current electors until consensus
  // is achieved or the work session expires
  const electionResult = {
    consensus: null
  };

  const gossipInterval =
    config['ledger-consensus-continuity'].worker.election.gossipInterval;

  // Note: Loop is only used in test mode, in non-test mode, every code
  // path will terminate the work session and then a scheduler will start
  // a new work session later
  let expired = false;
  const condition = () =>
    expired || electionResult.consensus ||
    // must provide enough time for block to be written afterwards
    session.timeRemaining() < 10000;

  // create callback that will expire current `loop` and postpone further
  // consensus work by the gossip interval
  let newEventsFound = false;
  const expire = loop => {
    expired = true;

    if(newEventsFound) {
      // new events have been found so reschedule as soon as possible to
      // get them into a block, do not wait for `gossipInterval` to expire
      logger.debug(
        'New events detected; rescheduling as soon as possible.', {
          ledgerNodeId: ledgerNode.id,
          blockHeight
        });
      return loop();
    }
    logger.debug(
      'No new events detected and work session expired; ' +
      'idling until gossip interval expires.', {
        blockHeight,
        gossipInterval,
        timeStamp: Date.now()
      });
    if(_testMode) {
      return loop();
    }
    _ledgerNodeMeta.setWaitUntil(
      ledgerNode.id, Date.now() + gossipInterval, loop);
  };

  // Note: Keep gossipping until:
  // 1. Consensus is found.
  // 2. Contacted every elector and found no new non-consensus regular events.
  // 3. Contacted every elector and no consensus was found.
  // 4. Work session expires.

  // track contacted electors
  const contacted = {};
  const isElector = !!_.find(electors, e => e.id === voter.id);
  if(isElector) {
    // do not contact self
    contacted[voter.id] = true;
  }
  async.until(condition, loop => {
    // loop:
    // 1. run gossip
    // 2. if there are regular events, find consensus
    async.auto({
      gossip: callback => {
        _contactElectors(
          {ledgerNode, voter, previousBlockHash, blockHeight, electors,
            contacted, peers, condition, electionResult}, callback);
      },
      newRegularEvents: ['gossip', (results, callback) => {
        if(newEventsFound) {
          return callback(null, newEventsFound);
        }
        // this extra check for non-consensus regular events is used to
        // skip running `_findConsensus`, preventing the generation of
        // unnecessary merge events
        _hasNewRegularEvents({ledgerNode}, callback);
      }],
      consensus: ['newRegularEvents', (results, callback) => {
        newEventsFound = results.newRegularEvents;
        if(!newEventsFound) {
          if(Object.keys(contacted).length === electors.length) {
            // gossipped with everyone and no new regular events, so expire
            return expire(loop);
          }
          // nothing to find consensus on, loop to continue gossipping
          return loop();
        }
        if(condition()) {
          // insufficient time to find consensus
          logger.debug('L289 insufficient time remaining to find consensus');
          return expire(loop);
        }
        logger.debug('Starting check for consensus _findConsensus.');
        // NOTE: ***DO NOT LOG CONSENSUS TO DEBUG OR CONSOLE, IT CAN BE HUGE***
        _findConsensus({ledgerNode, electors}, (err, consensus) => {
          logger.debug('_findConsensus complete.');
          if(err) {
            logger.error('Error in _findConsensus', {error: err});
            return callback(err);
          }
          if(consensus) {
            logger.debug('Found consensus.');
            electionResult.consensus = consensus;
          } else if(Object.keys(contacted).length === electors.length) {
            // gossipped with everyone and no consensus found, expire
            logger.debug('gossipped, no consensus found, expiring session');
            return expire(loop);
          }
          callback();
        });
      }]
    }, loop);
  }, err => err ? callback(err) : callback(err, electionResult));
}

function _findConsensus({ledgerNode, electors}, callback) {
  async.auto({
    history: callback => api._events.getRecentHistory({ledgerNode}, callback),
    // NOTE: mergeBranches mutates history
    merge: ['history', (results, callback) => api._events.mergeBranches(
      {history: results.history, ledgerNode}, callback)],
    // TODO: remove `history2` as unnecessary when `mergeBranches` history
    //   updates are fixed
    history2: ['merge', (results, callback) => api._events.getRecentHistory(
      {ledgerNode}, callback)],
    consensus: ['history2', (results, callback) => {
      logger.debug('Starting election.findConsensus.');
      // DO NOT LOG RESULTS OF FINDCONSENSUS
      api._election.findConsensus(
        {electors, history: results.history2, ledgerNode}, (err, result) => {
          logger.debug('election.findConsensus complete.');
          callback(err, result);
        });
    }]
  }, (err, results) => callback(err, results ? results.consensus : null));
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
  ledgerNode, voter, previousBlockHash, blockHeight, electors, contacted = {},
  peers, condition
}, callback) {
  // maximum number of peers to communicate with at once
  const limit =
    bedrock.config['ledger-consensus-continuity'].gossip.concurrentPeers;

  // TODO: track electors that are consistently hard to get in contact with
  // and avoid them

  // restrict electors to contact to 1/3 of total or limit
  const toContact = electors.filter(e => !(e.id in contacted));
  const max = Math.min(
    toContact.length - 1, Math.min(limit, Math.ceil(electors.length / 3)));
  toContact.length = max + 1;

  // 1. Contact N electors at random, in parallel.
  const random = _.shuffle(toContact);
  logger.debug('ELECTORS TO BE CONTACTED', {random});
  async.eachLimit(random, limit, (elector, nextElector) => {
    // no need to gossip if condition met
    if(condition()) {
      return nextElector();
    }
    // 2. Gossip with a single elector.
    // FIXME: review arguments to _gossipWith
    _gossip.gossipWith(
      {ledgerNode, peerId: elector.id,
        previousBlockHash, blockHeight, elector}, err => {
        // ignore communication error from a single voter
        if(err) {
          // TODO: revert to verbose
          // logger.verbose('Non-critical error in _gossipWith.', err);
          logger.debug('Non-critical error in _gossipWith.', {err});
          contacted[elector.id] = err;
        } else {
          contacted[elector.id] = true;
        }
        nextElector();
      });
  }, callback);
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
