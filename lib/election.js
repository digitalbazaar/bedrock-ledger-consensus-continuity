/*
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const crypto = require('crypto');
const jsonld = bedrock.jsonld;
const jsigs = require('jsonld-signatures')();
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const logger = require('./logger');
const util = require('util');
const BedrockError = bedrock.util.BedrockError;

// load config defaults
require('./config');

jsigs.use('jsonld', brDidClient.jsonld);

// maximum number of electors if not specified in the ledger configuration
const MAX_ELECTOR_COUNT = 10;

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._hasher = brLedgerNode.consensus._hasher;
api._storage = require('./storage');
api._voters = require('./voters');
// exposed for testing
api._recommendElectors = _recommendElectors;
api._getElectorBranches = _getElectorBranches;
api._findMergeEventProof = _findMergeEventProof;

/**
 * Determine if any new merge events have reached consensus in the given
 * history summary of merge events w/o consensus.
 *
 * @param ledgerNode the local ledger node.
 * @param history recent history rooted at the ledger node's local branch.
 * @param callback(err, result) called once the operation completes.
 *          result.event the merge events that have reached consensus or `null`.
 *          result.consensusProof the merge events proving consensus or `null`.
 */
api.findConsensus = (ledgerNode, history, callback) => {
  // TODO: Note: once computed, merge event Y+X candidates for each
  //   elector can be cached for quick retrieval in the future without
  //   the need to recompute them (they never change) for a given block...
  //   so the next blockHeight, elector, and X pair (its hash) could be
  //   stored in the continuity2017 meta for each candidate merge event Y

  // TODO: _getRecentHistory(..., (err, history) => { ...

  // TODO: const result = _findMergeEventProof(
  //   {ledgerNode, history.tail, electors})
};

/**
 * Converts the given view of history from one particular ledger node's
 * perspective into the views for each of the given electors.
 *
 * @param history recent history.
 * @param electors the current electors.
 *
 * @return a map of containing electorId => an array containing the elector's
 *           branch of history starting at its earliest merge event, i.e.
 *           the array contains the tail event created by the elector (but an
 *           array is used because there may be more than tail, to account for
 *           byzantine behavior).
 */
function _getElectorBranches({history, electors}) {
  const electorTails = {};

  // FIXME: remove below
  const byElector = {};
  // FIXME: remove above

  // find elector tails and build _treeParent index
  for(const e of history.events) {
    const creator = _getCreator(e);
    if(electors.includes(creator)) {
      // find parent from the same branch
      const treeHash = e.event.treeHash;
      e._treeParent = e._parents.filter(
        p => p.eventHash === treeHash)[0] || null;
      // FIXME: remove below
      if(creator in byElector) {
        byElector[creator].push(e);
      } else {
        byElector[creator] = [e];
      }
      // FIXME: remove above
      if(e._treeParent) {
        e._treeParent._treeChild = e;
      }
      if(!e._treeParent && !_isHistoryEntryRegularEvent(e)) {
        // event has no tree parent, so it is a tail (the earliest event in
        // recent history created by the elector)
        if(creator in electorTails) {
          // note that there is only one tail for correct nodes but we must
          // account here for byzantine nodes reporting more than one
          electorTails[creator].push(e);
        } else {
          electorTails[creator] = [e];
        }
      }
    }
  }

  // set generations for each branch
  for(let elector in electorTails) {
    for(const tail of electorTails[elector]) {
      let generation = 1;
      let next = tail;
      while(next) {
        // `next` should never be visited twice except in byzantine case
        if(next._generation) {
          next._generation = Math.min(next._generation, generation++);
        } else {
          next._generation = generation++;
        }
        next = next._treeChild;
      }
    }
  }

  // FIXME: remove below
  for(let elector in byElector) {
    console.log('TOTAL ELECTOR EVENTS', elector, byElector[elector].length, byElector[elector]);
  }
  // FIXME: remove above

  return electorTails;
}

function _getCreator(event) {
  let creator = _.get(event, 'meta.continuity2017.creator');
  if(!creator) {
    creator = event._children[0].meta.continuity2017.creator;
    event.meta = {continuity2017: {creator}};
  }
  return creator;
}

/**
 * Find consensus merge event Ys that have ancestors from a supermajority of
 * electors that are descendants of merge event Xs, where merge event Xs include
 * ancestors from a supermajority of electors. This indicates that merge event
 * Xs have both endorsed a merge events from a supermajority of electors and
 * they have been shared with a supermajority of electors because merge event Ys
 * include endorsements of merge event Xs from a supermajority of electors. For
 * each Y and X combo, it means that "merge event Y proves that merge event X
 * has been endorsed by a supermajority of electors".
 *
 * To have reached consensus, there must be at least a supermajority (a number
 * that constitutes 2/3rds + 1 of the current electors) of merge event Y
 * candidates where the candidates that have reached consensus are the ones
 * that do not have any merge event Y candidates as ancestors.
 *
 * @param ledgerNode the local ledger node.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *          by elector ID.
 * @param electors the current set of electors.
 *
 * @return an array of merge event Ys that prove merge event Xs have been
 *         endorsed by a super majority of electors.
 */
function _findMergeEventProof({ledgerNode, tails, electors}) {
  const yCandidatesByElector = _findMergeEventProofCandidates(
    {ledgerNode, tails, electors});
  if(!yCandidatesByElector) {
    // no Y candidates yet
    return [];
  }

  // TODO: build a list of all Y candidates that do not have other Y candidates
  //   as ancestors -- this is the list of merge event Ys.
  const result = _.values(yCandidatesByElector);
  const supermajority = api.twoThirdsMajority(electors.length);
  if(result.length < supermajority) {
    // no Y candidates yet, supermajority not reached
    return [];
  }

  // TODO: filter out any in `result` that have ancestors that are also in
  //   result

  // TODO: find consensus merge events for each elector

  // TODO: filter out any in `result` that has a consensus merge event that
  //   is a descendant of another consensus merge event

  return result;
}

/**
 * Find the next merge events Y candidates for each elector that has ancestors
 * from a supermajority of electors that are descendants of merge events X,
 * where merge events X include ancestors from a supermajority of electors.
 * These merge events provide proof that other merge events have been
 * approved by a consensus of electors. In order to be a candidate, a merge
 * event must also have descendants from a supermajority of electors,
 * demonstrating that it could achieve consensus as proof.
 *
 * For a given Y and X, X has both endorsed merge events from a supermajority
 * of electors and it has been shared with a supermajority of electors because
 * Y includes endorsements of X from a supermajority of electors. It means that
 * "merge event Y proves that merge event X has been endorsed by a
 * supermajority of electors".
 *
 * Both Y and X must be created by each elector ("branch-native"). Therefore,
 * each elector will produce a single unique Y and X combination (or none at
 * all).
 *
 * @param ledgerNode the local ledger node.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *          by elector ID.
 * @param electors the current set of electors.
 *
 * @return a map of electors to merge event Y that proves a merge event X has
 *           been endorsed by a super majority of electors, where X and Y are
 *           branch-native.
 */
function _findMergeEventProofCandidates({ledgerNode, tails, electors}) {
  const supermajority = api.twoThirdsMajority(electors.length);

  //console.log('TAILS', util.inspect(tails, {depth:10}));

  const electorsWithTails = Object.keys(tails);
  logger.verbose('Continuity2017 electors with tails for ledger node ' +
    ledgerNode.id + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNode.id, electorsWithTails});
  console.log('Continuity2017 electors with tails for ledger node ' +
    ledgerNode.id + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNode.id, electorsWithTails});
  if(electorsWithTails.length < supermajority) {
    // non-consensus events from a supermajority of electors have not yet
    // been collected, so return early
    return null;
  }

  /* Algorithm:

  For each elector, find the earliest branch-native merge event X that includes
  ancestors from a supermajority of electors. Then, for each elector, find
  merge event Y that ensures a supermajority of other merge events endorse its
  merge event X.

  If a supermajority of electors find a merge event Y, then an algorithm
  must be run such that the electors will pick the same merge event Y. */

  const xByElector = {};
  const yByElector = {};

  // find merge event X candidate for each elector
  for(const elector of electorsWithTails) {
    // iterate over each `tail` merge event for each elector and
    // find earliest `x` candidate using the same search cache
    const cache = {};
    let earliest = null;
    console.log('FINDING X for', elector);
    for(const tail of tails[elector]) {
      // FIXME: remove below
      const electorEvents = [];
      let foo = tail;
      do { electorEvents.push(foo); foo = foo._treeChild; } while(foo);
      console.log('ALL TREE EVENTS', electorEvents.length);
      // FIXME: remove above
      const result = _findDiversePedigreeMergeEvent(
        {x: tail, branch: elector, electors, supermajority, cache});
      if(!earliest || result._generation < earliest._generation) {
        earliest = result;
      }
    }
    if(earliest) {
      console.log('***X found for', elector, ' at generation ',
        earliest._generation, earliest);
      console.log('cache', cache);
      console.log('TOTAL ENDORSEMENTS', cache[earliest.eventHash].length);
      // FIXME: remove below
      let min = 0;
      let foobar;
      for(let z in cache) {
        const len = cache[z].length;
        if(min < supermajority) {
          foobar = z;
          min = len;
        } else if(len >= supermajority && len < min) {
          foobar = z;
          min = len;
        }
      }
      console.log('ACTUAL MINIMUM', min, foobar);
      // FIXME: remove above
      xByElector[elector] = earliest;
      //process.exit(1);
    } else {
      console.log('***NO X found for ' + elector);
    }
  }

  logger.verbose('Continuity2017 X merge events found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, xByElector});
  console.log('Continuity2017 X merge events found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, xByElector});

  if(Object.keys(xByElector).length < supermajority) {
    // non-consensus events X from a supermajority of electors have not yet
    // been collected, so return early
    return null;
  }

  // find merge event Y candidate for each elector
  for(const elector in xByElector) {
    const x = xByElector[elector];
    console.log('FINDING Y FOR X', x, elector);
    const result = _findDiversePedigreeMergeEvent(
      {x, branch: elector, electors, supermajority});
    if(result) {
      yByElector[elector] = result;
    }
  }

  logger.verbose(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, yByElector});
  console.log(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, yByElector});

  return yByElector;
}

/**
 * Recursively find the earliest merge event for an elector that includes
 * an ancestry of merge events from at least a supermajority of electors. This
 * merge event is said to have a "diverse pedigree". The search starts at the
 * oldest event in history on a particular elector branch (this constitutes
 * generation `1`) and proceeds forward through history.
 *
 * @param x the event in history to begin searching at.
 * @param branch the branch (elector ID) to search on.
 * @param electors all current electors (elector IDs).
 * @param supermajority the number that constitutes a supermajority of electors.
 * @param cache a search result cache.
 * @param endorsements an array of unique endorsing electors (elector IDs).
 * @param candidate the earliest candidate found so far.
 *
 * @return the earliest merge event with a diverse pedigree.
 */
function _findDiversePedigreeMergeEvent({
  x, branch, electors, supermajority,
  endorsements = [_getCreator(x)],
  cache = {},
  candidate = null
}) {
  console.log('EVENT', x.eventHash);
  // TODO: consider optimizing tracking unique endorsements using maps and
  // counters instead of arrays

  // if event creator is an elector, add its endorsement
  const creator = _getCreator(x);
  console.log('CREATOR OF EVENT', creator);

  if(creator !== branch) {
    if(electors.includes(creator) && !endorsements.includes(creator)) {
      endorsements.push(creator);
      console.log('NEW ENDORSEMENT ON BRANCH', branch, endorsements);
    }
  } else {
    if(candidate && x._generation > candidate._generation) {
      // generation threshold passed, no earlier candidate can be found
      return candidate;
    }

    // update total endorsements for the current event
    let totalEndorsements = cache[x.eventHash] || [];
    totalEndorsements.push(...endorsements);
    cache[x.eventHash] = totalEndorsements = _.uniq(totalEndorsements);
    // FIXME: remove below
    if(!cache._gen) {
      cache._gen = {};
    }
    cache._gen[x.eventHash] = x._generation;
    // FIXME: remove above

    console.log('GENERATION', x._generation, 'EVENT', x.eventHash, 'ON BRANCH', branch);
    console.log('totalEndorsements', totalEndorsements);
    //console.log('supermajority', supermajority);
    if(totalEndorsements.length === 3 && x._generation === 1) {
      console.log('FOUND TOTAL ENDS OF 3!!!!', x, 'supermajority', 3);
    }

    if(totalEndorsements.length >= supermajority) {
      // new candidate found
      console.log('old candidate', candidate);
      return candidate = x;
    }
  }

  // iterate through children of `x`
  for(let child of x._children) {
    // TODO: there may be an opportunity to optimize this search in the
    //   common case -- the first child is usually local so we won't pick
    //   up additional endorsements by going all the way down a path of
    //   first children; we may see improvments by checking the children
    //   in reverse or another order

    // FIXME: can we remove regular events from the recent history view
    // entirely? do they serve a useful purpose?

    // if child is a regular event, recurse using its child merge event
    if(_isHistoryEntryRegularEvent(child)) {
      console.log('regular event detected at', child);
      child = child._children[0];
      console.log('using new event', child);
    }

    // recurse into child
    console.log('RECURSING into child', child.eventHash, 'created by',
      _getCreator(child), 'from parent', x.eventHash);
    candidate = _findDiversePedigreeMergeEvent({
      x: child, branch, electors, supermajority,
      endorsements: endorsements.slice(), cache,
      candidate});
    console.log('RECURSION DONE for child', child.eventHash, 'created by',
      _getCreator(child), 'of parent', x.eventHash);
  }

  return candidate;
}

function _isHistoryEntryRegularEvent(x) {
  return !jsonld.hasValue(x.event, 'type', 'ContinuityMergeEvent');
}

/**
 * Get the electors for the given ledger node and block height.
 *
 * The electors will be passed to the given callback using the given
 * data structure:
 *
 * [{id: voter_id, sameAs: previous_voter_id}, ... ]
 *
 * @param ledgerNode the ledger node API to use.
 * @param blockHeight the height of the block.
 * @param callback(err, electors) called once the operation completes.
 */
api.getBlockElectors = (ledgerNode, blockHeight, callback) => {
  async.auto({
    config: callback => _getLatestConfig(ledgerNode, callback),
    // NOTE: events *must* be expanded here
    latestBlock: callback => ledgerNode.storage.blocks.getLatest(
      (err, result) => {
        if(err) {
          return callback(err);
        }
        const expectedBlockHeight = result.eventBlock.block.blockHeight + 1;
        if(expectedBlockHeight !== blockHeight) {
          return callback(new BedrockError(
            'Invalid `blockHeight` specified.', 'InvalidStateError', {
              blockHeight,
              expectedBlockHeight
            }));
        }
        callback(null, result);
      }),
    electors: ['config', 'latestBlock', (results, callback) => {
      // get previous consensus events
      const previousEvents = results.latestBlock.eventBlock.block.event;

      // FIXME: this uses the key ID for the elector ID ... which is not
      //   the same as the voter ID ... either these need to be made the
      //   same or we need to look up `event.signature.creator` to find
      //   the voter ID

      // aggregate recommended electors
      let electors = [];
      previousEvents.forEach(event => {
        // TODO: is `e.signature.creator` check robust enough? Can it assume
        //   a single signature and that it's by the voter?
        electors.push(...event.signature.creator);
        // TODO: support recommended electors?
        /*const recommended = jsonld.getValues(event, 'recommendedElector');
        // only accept a recommendation if there is exactly 1
        if(recommended.length === 1) {
          // TODO: recommended elector needs to be validated -- only
          //   previous participants (those that have generated signed merge
          //   events) can be recommended
          electors.push(recommended[0]);
        }*/
      });

      // TODO: simply count consensus event signers once and proof signers
      //   twice for now -- add comprehensive elector selection and
      //   recommended elector vote aggregating algorithm in v2
      const aggregate = {};
      electors = _.uniq(electors).forEach(
        e => aggregate[e] = {id: e, weight: 1});
      // TODO: weight previous electors more heavily to encourage continuity
      const consensusProof =
        results.latestBlock.eventBlock.block.consensusProof;
      _.uniq(consensusProof.map(e => e.signature.creator))
        .forEach(id => {
          if(id in aggregate) {
            aggregate[id].weight = 3;
          } else {
            aggregate[id].weight = 2;
          }
        });
      electors = Object.keys(aggregate).map(k => aggregate[k]);

      // get elector count, defaulting to MAX_ELECTOR_COUNT if not set
      // (hardcoded, all nodes must do the same thing -- but ideally this would
      // *always* be set)
      const electorCount = results.config.electorCount || MAX_ELECTOR_COUNT;

      // it's possible `electors.length` will be less than `electorCount` if
      // few events received consensus or a config change happened -- which we
      // allow here
      if(electors.length > electorCount) {
        // TODO: could optimize by only sorting tied electors if helpful
        /*
        // fill positions
        let idx = -1;
        for(let i = 0; i < electorCount; ++i) {
          if(electors[i].weight > electors[i + 1].weight) {
            idx = i;
          }
        }
        // fill positions with non-tied electors
        const positions = electors.slice(0, idx + 1);
        if(positions.length < electorCount) {
          // get tied electors
          const tied = electors.filter(
            e => e.weight === electors[idx + 1].weight);
          // TODO: sort tied electors
        }
        }*/

        // break ties via sorting
        electors.sort((a, b) => {
          // 1. sort descending by count
          if(a.count !== b.count) {
            return b.count - a.count;
          }

          // generate and cache hashes
          // the hash of the previous block is combined with the elector id to
          // prevent any elector from *always* being sorted to the top
          a.hash = a.hash || _sha256(
            results.latestBlock.eventBlock.meta.blockHash + _sha256(a.id));
          b.hash = b.hash || _sha256(
            results.latestBlock.eventBlock.meta.blockHash + _sha256(b.id));

          // 2. sort by hash
          return a.hash.localeCompare(b.hash);
        });

        // select first `electorCount` electors
        electors = electors.slice(0, electorCount);
      }

      // TODO: if there were no electors chosen or insufficient electors,
      // add electors from config

      electors.map(e => {
        // only include `id` and `sameAs`
        const elector = {id: e.id};
        if(e.sameAs) {
          elector.sameAs = e.sameAs;
        }
        return elector;
      });

      logger.verbose(
        'Continuity2017 electors for ledger node ' + ledgerNode.id +
        ' at block height ' + blockHeight,
        {ledgerNode: ledgerNode.id, blockHeight, electors});

      callback(null, electors);
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.electors));
};

/**
 * Determines if the given voter is in the passed voting population.
 *
 * @param voter the voter to check for.
 * @param electors the voting population.
 *
 * @return true if the voter is in the voting population, false if not.
 */
api.isBlockElector = (voter, electors) => {
  return electors.some(v => v.id === voter.id);
};

/**
 * Calculate a two thirds majority of electors. When electors <= 3, every
 * elector must agree.
 *
 * @param electorCount the total number of electors.
 *
 * @return the number of electors that constitute a two thirds majority.
 */
api.twoThirdsMajority = electorCount => (electorCount <= 3) ? electorCount :
  Math.floor(electorCount / 3) * 2 + 1;

/**
 * Recommends the next set of electors.
 *
 * @param ledgerNode the ledger node.
 * @param electors all electors for the next block.
 * @param callback(err, recommendedElectors) called once the operation
 *          completes.
 */
function _recommendElectors(ledgerNode, electors, callback) {
  // TODO: determine if a better deterministic algorithm (better == more secure,
  // performant) could be used here that uses given parameters and blockchain
  // data to recommend electors for the next block

  let recommendedElectors = [];

  // TODO: implement option to add all nodes that sent in events to the
  // previous block to the elector pool that can be contacted

  async.auto({
    config: callback => _getLatestConfig(ledgerNode, callback),
    // NOTE: events *must* be expanded here
    latestBlock: callback => ledgerNode.storage.blocks.getLatest(callback),
    latestVoters: ['latestBlock', (results, callback) => {
      // there may not be an event block yet
      const voters =
        _.get(results, 'latestBlock.eventBlock.block.event', [])
          .map(e => ({id: e.signature.creator}));
      recommendedElectors.push(...voters);
      // TODO: do we also want to support an optional `recommendedElector`
      //   field on merge events that we can draw from as well?
      callback();
    }],
    eventPeers: ['latestVoters', (results, callback) => {
      if(!results.latestBlock.eventBlock.block) {
        return callback(null, []);
      }
      const events = results.latestBlock.eventBlock.block.event || [];
      async.map(events, _getEventPeers.bind(null, ledgerNode), callback);
    }],
    addElectors: ['config', 'eventPeers', (results, callback) => {
      // add event peers
      const eventPeers = [].concat(...results.eventPeers);
      recommendedElectors.push(...eventPeers);

      // add previous electors
      recommendedElectors.push(...electors.map(elector => ({id: elector.id})));

      // remove duplicates from the list of electors
      recommendedElectors = _.uniqWith(
        recommendedElectors, (a, b) => a.id === b.id);

      // restrict the number of electors
      // get elector count, defaulting to MAX_ELECTOR_COUNT if not set
      // (hardcoded, all nodes must do the same thing -- but ideally this would
      // *always* be set)
      const electorCount = results.config.electorCount || MAX_ELECTOR_COUNT;
      recommendedElectors.splice(electorCount - 1);
      callback();
    }]
  }, err => callback(err, recommendedElectors));
}

/**
 * Gets peer voters from an event, based on its signatures. If the event
 * has no signature from a peer voter, then an empty array will be returned in
 * the callback.
 *
 * @param ledgerNode the ledger node.
 * @param event the event to check.
 * @param callback(err, peers) called once the operation completes.
 */
function _getEventPeers(ledgerNode, event, callback) {
  // TODO: optimize
  const owners = [];
  jsigs.verify(event, {
    checkKeyOwner: (owner, key, options, callback) => {
      if(jsonld.hasValue(
        owner, 'type', 'https://w3id.org/wl#Continuity2017Peer')) {
        owners.push({id: owner.id});
      }
      callback(null, true);
    }
  }, err => {
    if(err) {
      // ignore bad or missing signature; no event peer can be found
      // TODO: revert to verbose
      logger.debug('Non-critical error in _getEventPeers.', err);
      //logger.verbose('Non-critical error in _getEventPeers.', err);
      return callback(null, []);
    }
    // TODO: do a more robust check to ensure that the peer is up-to-date
    // with the current blockHeight (block status phase is `consensus`)

    // FIXME: skipping this check because there are restriction on getting
    // blockHeight = 0.  See api.getBlockElectors(L#107) above
    // const blockHeight = 0;
    // async.filter(_.uniq(owners), (owner, callback) =>
    //   api._client.getBlockStatus(blockHeight, owner.id, (err, status) => {
    //     console.log('EEEEEEEEE', err);
    //     console.log('SSSSSSSSSSS', status);
    //     callback(null, !err && status.ledger === ledgerNode.ledger);
    //   }), callback);
    callback(null, _.uniq(owners));
  });
}

function _getLatestConfig(ledgerNode, callback) {
  ledgerNode.storage.events.getLatestConfig((err, result) => {
    if(err) {
      return callback(err);
    }
    // `getLatestConfig` returns an empty object before genesis block is written
    if(_.isEmpty(result)) {
      return callback(null, {});
    }
    const config = result.event.ledgerConfiguration;
    if(config.consensusMethod !== 'Continuity2017') {
      return callback(new BedrockError(
        'Consensus method must be "Continuity2017".', 'InvalidStateError', {
          consensusMethod: config.consensusMethod
        }));
    }
    callback(null, config);
  });
}

function _sha256(x) {
  return crypto.createHash('sha256').update(x).digest('hex');
}

// TODO: remove this or use it where?
function _validateEvents(ledgerNode, hashes, blockHeight, callback) {
  async.auto({
    getEvents: callback => async.map(hashes, (eventHash, callback) =>
      ledgerNode.storage.events.get(eventHash, callback), callback),
    getConfig: ['getEvents', (results, callback) => {
      if(blockHeight > 0) {
        return ledgerNode.storage.events.getLatestConfig(callback);
      }
      // genesis block
      callback(null, results.getEvents.filter(e =>
        e.event.type === 'WebLedgerConfigurationEvent')[0]);
    }],
    validate: ['getConfig', 'getEvents', (results, callback) => {
      const configEvent = results.getConfig.event.ledgerConfiguration;
      if(!(configEvent.eventValidator &&
        configEvent.eventValidator.length > 0)) {
        // no validators for this ledger, pass all events
        return callback(null, results.getEvents);
      }
      const requireEventValidation =
        configEvent.requireEventValidation || false;
      async.filter(results.getEvents, (e, callback) =>
        brLedgerNode.consensus._validateEvent(
          e.event, configEvent.eventValidator, {requireEventValidation},
          err => {
            if(err) {
              // TODO: the event did not pass validation, should the event
              // be retried? marked for deletion?
              // failed events will forever be candidates for inclusion in
              // future blocks until this TODO is addressed
              return callback(null, false);
            }
            callback(null, true);
          }
        ), callback);
    }]
  }, (err, results) => err ? callback(err) : callback(null, {
    hashes: results.validate.map(e => e.meta.eventHash),
    events: results.validate.map(e => e.event)
  }));
}
