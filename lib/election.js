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
api._getElectorBranches = _getElectorBranches;
api._getAncestors = _getAncestors;
api._findMergeEventProof = _findMergeEventProof;

/**
 * Determine if any new merge events have reached consensus in the given
 * history summary of merge events w/o consensus.
 *
 * @param ledgerNode the local ledger node.
 * @param history recent history rooted at the ledger node's local branch.
 * @param electors the current electors.
 * @param callback(err, result) called once the operation completes where
 *          `result` is null if no consensus has been reached or where it
 *          is an object if it has, where:
 *          result.event the merge events that have reached consensus.
 *          result.consensusProof the merge events proving consensus.
 */
api.findConsensus = ({ledgerNode, history, blockHeight, electors}, callback) => {
  // TODO: Note: once computed, merge event Y+X candidates for each
  //   elector can be cached for quick retrieval in the future without
  //   the need to recompute them (they never change) for a given block...
  //   so the next blockHeight, elector, and X pair (its hash) could be
  //   stored in the continuity2017 meta for each candidate merge event Y
  logger.debug('Start sync _getElectorBranches branches', {electors});
  const tails = _getElectorBranches({history, electors});
  logger.debug('End sync _getElectorBranches');
  const startProof = Date.now();
  logger.debug('Start sync _findMergeEventProof');
  //console.log('Start sync _findMergeEventProof');
  const proof = _findMergeEventProof({ledgerNode, tails, blockHeight, electors});
  /*console.log('End sync _findMergeEventProof', {
    proofDuration: Date.now() - startProof
  });*/
  logger.debug('End sync _findMergeEventProof', {
    proofDuration: Date.now() - startProof
  });
  if(proof.consensus.length === 0) {
    logger.debug('findConsensus no proof found, exiting');
    return callback(null, null);
  }
  logger.debug('findConsensus proof found, proceeding...');
  const allXs = proof.consensus.map(p => p.x);
  const consensusProofHash = _.uniq(
    proof.consensus.reduce((aggregator, current) => {
      aggregator.push(...current.proof.map(r => r.eventHash));
      return aggregator;
    }, []));
  async.auto({
    xAncestors: callback => _getAncestors({ledgerNode, allXs}, callback),
    proof: callback => {
      const collection = ledgerNode.storage.events.collection;
      const proofEvents = [];
      const query = {
        eventHash: {$in: consensusProofHash}
      };
      const projection = {_id: 0, event: 1};
      collection.find(query, projection)
        .forEach(doc => proofEvents.push(doc.event), err => {
          if(err) {
            return callback(err);
          }
          callback(null, proofEvents);
        });
    }
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, {
      consensusProof: results.proof,
      consensusProofHash,
      event: results.xAncestors
    });
  });
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
  if(history === undefined || electors === undefined) {
    throw new TypeError('`history` and `electors` are required.');
  }
  const electorTails = {};
  const electorSet = new Set(electors.map(e => e.id));

  // find elector tails and build _treeParent index
  for(const e of history.events) {
    const creator = _getCreator(e);
    if(electorSet.has(creator)) {
      // find parent from the same branch
      const treeHash = e.event.treeHash;
      e._treeParent = _.find(e._parents, p => p.eventHash === treeHash) || null;
      if(_isHistoryEntryRegularEvent(e)) {
        continue;
      }
      if(e._treeParent) {
        if(!e._treeParent._treeChildren) {
          e._treeParent._treeChildren = [e];
        } else {
          e._treeParent._treeChildren.push(e);
        }
      } else {
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
  for(const elector in electorTails) {
    let generation = 1;
    let next = electorTails[elector];
    while(next.length > 0) {
      const current = next;
      next = [];
      for(const event of current) {
        event._generation = generation;
        next.push(...(event._treeChildren || []));
      }
      generation++;
    }
  }

  return electorTails;
}

// FIXME: documentation
function _getAncestors({allXs, ledgerNode}, callback) {
  // get all ancestor hashes from every consensus X
  const hashes = new Set();
  const descendants = new Set();
  for(const x of allXs) {
    // TODO: anything missed or different here with byzantine forks?
    let next = [x];
    while(next.length > 0) {
      const current = next;
      next = [];
      for(const event of current) {
        if(descendants.has(event)) {
          continue;
        }
        descendants.add(event);
        hashes.add(event.eventHash);
        // ensure all regular events are added
        event.event.parentHash.forEach(hash => hashes.add(hash));
        if(event._parents) {
          next.push(...event._parents);
        }
      }
    }
  }

  // look up non-consensus events by hash
  const collection = ledgerNode.storage.events.collection;
  const query = {
    eventHash: {$in: [...hashes]},
    'meta.consensus': {$exists: false}
  };
  const projection = {_id: 0, event: 1, eventHash: 1};
  collection.find(query, projection).toArray(callback);
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
 * @return a map with `consensus` and `yCandidates`; the `consensus` key's
 *         value is an array of merge event X and Y pairs where each merge
 *         event Y and its history proves its paired merge event X has been
 *         endorsed by a super majority of electors -- another key, `proof` is
 *         also included with each pair that includes `y` and its direct
 *         ancestors until `x`, these, in total, constitute endorsements of `x`.
 */
function _findMergeEventProof({ledgerNode, tails, blockHeight, electors}) {
  let startTime = Date.now();
  logger.debug('Start sync _findMergeEventProofCandidates');
  //console.log('Start sync _findMergeEventProofCandidates');
  const candidates = _findMergeEventProofCandidates(
    {ledgerNode, tails, blockHeight, electors});
  /*console.log('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });*/
  logger.debug('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });
  if(!candidates) {
    // no Y candidates yet
    return {consensus: []};
  }

  const yCandidatesByElector = candidates.yByElector;
  const supermajority = api.twoThirdsMajority(electors.length);
  if(Object.keys(yCandidatesByElector).length < supermajority) {
    // insufficient Y candidates so far, supermajority not reached
    return {consensus: []};
  }

  startTime = Date.now();
  //console.log('Start sync _findConsensusMergeEventProof');
  logger.debug('Start sync _findConsensusMergeEventProof');
  const ys = _findConsensusMergeEventProof(
    {ledgerNode, xByElector: candidates.xByElector,
      yByElector: yCandidatesByElector, blockHeight, electors});
  /*console.log('End sync _findConsensusMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  logger.debug('End sync _findConsensusMergeEventProof', {
    duration: Date.now() - startTime
  });
  if(ys.length === 0) {
    // no consensus yet
    return {consensus: []};
  }

  return {
    // pair Ys with Xs
    consensus: ys.map(y => {
      const x = candidates.xByElector[_getCreator(y)];
      let proof = _flattenDescendants(
        {ledgerNode, x, descendants: y._xDescendants});
      if(proof.length === 0 && supermajority === 1) {
        // always include single elector as proof; enables continuity of that
        // single elector when computing electors in the next block via
        // quick inspection of `block.consenusProof`
        proof = [x];
      }
      return {y, x, proof};
    }),
    // return all yCandidates for debugging purposes
    yCandidates: _.values(yCandidatesByElector)
  };
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
 * @return `null` or a map containing `yByElectors` and `xByElectors`; in
 *           `yByElectors`, each elector maps to merge event Y that proves a
 *           merge event X has been endorsed by a super majority of electors,
 *           where X and Y are branch-native.
 */
function _findMergeEventProofCandidates({ledgerNode, blockHeight, tails, electors}) {
  const supermajority = api.twoThirdsMajority(electors.length);

  //console.log('TAILS', util.inspect(tails, {depth:10}));

  const electorsWithTails = Object.keys(tails);
  // TODO: ensure logging `electorsWithTails` is not slow
  /*logger.verbose('Continuity2017 electors with tails for ledger node ' +
    ledgerNode.id + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNode.id, electorsWithTails});*/
  /*console.log('Continuity2017 electors with tails for ledger node ' +
    ledgerNode.id + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNode.id, electorsWithTails});*/
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

  If a supermajority of electors find a merge event Y, then another algorithm
  must be run such that the electors will pick the same merge events Y. */

  const xByElector = {};
  const yByElector = {};

  // find merge event X candidate for each elector
  let startTime = Date.now();
  logger.debug('Start sync _findMergeEventProofCandidates: Xs');
  for(const elector of electorsWithTails) {
    //console.log('FINDING X for', elector);
    // TODO: safely skip electors with multiple tails detected (byzantine)?
    const electorTails = tails[elector];
    if(electorTails.length !== 1) {
      continue;
    }

    // TODO: simplify code or make generic to handle N iters before selecting
    //   an `x` where 0 is the default, i.e. tail is `x`
    // use elector tail as
    const descendants = {};
    const result = electorTails[0];
    /* // find earliest `x` for the elector's tail
    const result = _findDiversePedigreeMergeEvent(
      {ledgerNode, x: electorTails[0], electors, supermajority, descendants});*/
    if(result) {
      //console.log('***X found for', elector, ' at generation ',
      //  result._generation, result);
      xByElector[elector] = result;
      // include `result` in initial descendants map, it is used to halt
      // searches for Y and in producing the set of events to include in a
      // block should an X be selected
      descendants[result.eventHash] = [];
      result._initDescendants = descendants;
    } else {
      //console.log('***NO X found for ' + elector);
    }
  }
  logger.debug('End sync _findMergeEventProofCandidates: Xs', {
    duration: Date.now() - startTime
  });

  // TODO: ensure logging `xByElector` is not slow
  /*logger.verbose('Continuity2017 X merge events found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, xByElector});*/
  /*console.log('Continuity2017 X merge events found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, xByElector});*/

  if(Object.keys(xByElector).length < supermajority) {
    // non-consensus events X from a supermajority of electors have not yet
    // been collected, so return early
    return null;
  }

  // find merge event Y candidate for each elector
  startTime = Date.now();
  logger.debug('Start sync _findMergeEventProofCandidates: Y candidates');
  for(const elector in xByElector) {
    const x = xByElector[elector];
    const descendants = {};
    //console.log('FINDING Y FOR X', x, elector);
    // pass `x._initDescendants` as the ancestry map to use to short-circuit
    // searches as it includes all ancestors of X -- which should not be
    // searched when finding a Y because they cannot lead to X
    const result = _findDiversePedigreeMergeEvent(
      {ledgerNode, x, electors, supermajority, descendants,
        ancestryMap: x._initDescendants});
    if(result) {
      yByElector[elector] = result;
      result._xDescendants = descendants;
    }
  }
  logger.debug('End sync _findMergeEventProofCandidates: Y candidates', {
    duration: Date.now() - startTime
  });

  // TODO: ensure logging `yByElector` is not slow
  /*logger.verbose(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, yByElector});*/
  /*console.log(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, blockHeight, yByElector});*/

  return {yByElector, xByElector};
}

/**
 * Find the earliest merge event for an elector that includes an ancestry of
 * merge events from at least a supermajority of electors. This merge event is
 * said to have a "diverse pedigree" and indicates that another event is
 * well-endorsed. The search starts at the oldest event in history on a
 * particular elector branch (this constitutes generation `1`) and proceeds
 * forward through history.
 *
 * @param ledgerNode the current ledger node, used for logging.
 * @param x the event in history to begin searching at.
 * @param electors all current electors.
 * @param supermajority the number that constitutes a supermajority of electors.
 * @param descendants an optional map of event hash to descendants that is
 *          populated as they are found.
 * @param ancestryMap an optional map of event hash to ancestors of `x` that is
 *          used to short-circuit searching.
 *
 * @return the earliest merge event with a diverse pedigree.
 */
function _findDiversePedigreeMergeEvent(
  {ledgerNode, x, electors, supermajority, descendants = {},
  ancestryMap = _buildAncestryMap(x)}) {
  //console.log('EVENT', x.eventHash);

  if(!(x._treeChildren && x._treeChildren.length === 1)) {
    if(!x._treeChildren && supermajority === 1) {
      // trivial case, return `x`
      return x;
    }
    // byzantine node or no children when supermajority > 1, abort
    return null;
  }

  const electorSet = new Set(electors.map(e => e.id));

  let treeDescendant = x._treeChildren[0];
  //console.log('FINDING descendant for: ', x.eventHash);
  //console.log('X creator', _getCreator(x));
  while(treeDescendant) {
    //console.log();
    //console.log('checking generation', treeDescendant._generation);
    //console.log('treeDescendant hash', treeDescendant.eventHash);
    // add all descendants of `x` that are ancestors of `treeDescendant`
    _findDescendantsInPath(
      {ledgerNode, x, y: treeDescendant, descendants, ancestryMap});

    // see if there are a supermajority of endorsements of `x` now
    if(_hasSufficientEndorsements(
      {ledgerNode, x, descendants, electorSet, supermajority})) {
      //console.log('supermajority of endorsements found at generation', treeDescendant._generation);
      //console.log();
      return treeDescendant;
    }
    //console.log('not enough endorsements yet at generation', treeDescendant._generation);
    // FIXME: remove me
    //const ancestors = _flattenDescendants({ledgerNode, x, descendants});
    //console.log('total descendants so far', ancestors.map(r=>({
    //  creator: _getCreator(r),
    //  generation: r._generation,
    //  hash: r.eventHash
    //})));
    //console.log();

    if(!(treeDescendant._treeChildren &&
      treeDescendant._treeChildren.length === 1)) {
      // byzantine node or no children, abort
      return null;
    }

    treeDescendant = treeDescendant._treeChildren[0];
  }

  return null;
}

function _findConsensusMergeEventProof(
  {ledgerNode, xByElector, yByElector, blockHeight, electors}) {
  /*logger.verbose(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id});*/
  /*console.log(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id});*/

  const allYs = _.values(yByElector);

  // if electors is 1, consensus is trivial
  if(electors.length === 1) {
    return allYs;
  }

  // build map of each Y's ancestry for quick checks of halting conditions
  const yAncestryMaps = _buildAncestryMaps(allYs);
  for(const y of allYs) {
    // include all known initial and X descendants in ancestry map
    const map = yAncestryMaps[y.eventHash];
    for(let hash in y._xDescendants) {
      map[hash] = true;
    }
    const x = xByElector[_getCreator(y)];
    for(let hash in x._initDescendants) {
      map[hash] = true;
    }
  }

  // initialize all Ys to support all self-endorsed Ys
  allYs.forEach(y => {
    const supporting = _allEndorsedYs({event: y, allYs, yAncestryMaps});
    // always include self
    supporting.add(y);
    // TODO... do not set supporting here and initialize elsewhere? what
    //   about late Ys ... do they need different init to see confirmations,
    //   etc?
    y._supporting = [...supporting];
    y._votes = {};
    //y._supporting.forEach(supported => {
    supporting.forEach(supported => {
      y._votes[_getCreator(supported)] = supported;
    });
  });

  let startTime = Date.now();
  logger.debug('Start sync _findConsensusMergeEventProof: _tallyBranches');
  //console.log('Start sync _findConsensusMergeEventProof: _tallyBranches');
  // go through each Y's branch looking for consensus
  let consensus = _tallyBranches({ledgerNode, yByElector, blockHeight, electors});
  /*console.log('End sync _findConsensusMergeEventProof: _tallyBranches', {
    duration: Date.now() - startTime
  });*/
  logger.debug('End sync _findConsensusMergeEventProof: _tallyBranches', {
    duration: Date.now() - startTime
  });

  if(consensus) {
    // TODO: ensure logging `consensus` is not slow
    /*logger.verbose(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNode.id, {ledgerNode: ledgerNode.id, proof: consensus});*/
    /*console.log(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNode.id, {ledgerNode: ledgerNode.id, proof: consensus});*/
  }

  return consensus || [];
}


/**
 * Adds all descendants found between `x` and `y`. Descendants are added to
 * a `descendants` map as they are found -- this map may contain descendants
 * that are not between `x` and `y`. To obtain the descendants that are only
 * between `x` and `y`, the `descendants` map must be traversed starting
 * with the children of `x`. Every entry in the `descendants` map is an array
 * with ancestors of `y`.
 *
 * This method may be called using a prepopulated (via a previous call of
 * this method using a different `y`) `descendants` map. This is useful for
 * iterating through the tree descendants of `x` looking for the first
 * same-tree descendant that has a supermajority of endorsements of `x`.
 *
 * TODO: Consider making `descendants` a Map, and use objects as keys rather
 * than hashes for quicker look ups and less memory usage.
 *
 * @param ledgerNode the current ledgerNode, for logging.
 * @param x the starting event to find descendants of.
 * @param y the stopping event to find ancestors of.
 * @param descendants the descendants map to use.
 * @param ancestryMap a map of the ancestry of `x` to optimize searching.
 */
function _findDescendantsInPath({
  ledgerNode, x, y, descendants = {}, ancestryMap = _buildAncestryMap(x)}) {
  // find all descendants of `x` that are ancestors of `y`
  let next = [y];
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(event.eventHash in ancestryMap) {
        //console.log('SKIPPING', event.eventHash);
        continue;
      }
      for(let parent of event._parents) {
        if(_isHistoryEntryRegularEvent(parent)) {
          if(!parent._treeParent) {
            continue;
          }
          parent = parent._treeParent;
        }
        //console.log('event.parent', {
        //  creator: _getCreator(parent),
        //  generation: parent._generation,
        //  hash: parent.eventHash
        //});
        const d = descendants[parent.eventHash];
        if(d) {
          if(!d.includes(event)) {
            d.push(event);
          }
          //console.log('parent ALREADY in descendants', parent.eventHash);
          continue;
        }
        //console.log('ADDING parent to descendants', parent.eventHash);
        descendants[parent.eventHash] = [event];
        next.push(parent);
      }
    }
  }
  //console.log('entries in descendants', Object.keys(descendants));
}

function _flattenDescendants({ledgerNode, x, descendants}) {
  const result = [];
  let next = [x];
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      //console.log('_flatten event', {
      //  creator: _getCreator(event),
      //  generation: event._generation,
      //  hash: event.eventHash
      //});
      const d = descendants[event.eventHash];
      if(d) {
        // `event` is in the computed path of descendants
        next.push(...d);
      }
    }
    // TODO: can we avoid duplicated processing by checking `result`
    // for already added events?
    next = _.uniq(next);
    result.push(...next);
  }
  return _.uniq(result);
}

function _updateToMostRecentVotes(
  {ledgerNode, y, yByElector, descendants, electorSet, votes}) {
  let next = [y];
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      //console.log('_updateToMostRecentVotes event', {
      //  creator: _getCreator(event),
      //  generation: event._generation,
      //  hash: event.eventHash
      //});
      const d = descendants[event.eventHash];
      if(d) {
        const creator = _getCreator(event);
        if(electorSet.has(creator)) {
          // only include `event` as voting if it is >= to its associated
          // Y's generation
          if(event._generation >= yByElector[creator]._generation) {
            _useMostRecentVotingEvent(
              {ledgerNode, elector: creator, votes, votingEvent: event});
          }
        }
        // `event` is in the computed path of descendants
        next.push(...d);
      }
    }
    next = _.uniq(next);
  }
}

function _useMostRecentVotingEvent({ledgerNode, elector, votes, votingEvent}) {
  // only count vote from a particular elector once, using the most
  // recent from that elector; if an elector has two voting events
  // from the same generation, it is byzantine, invalidate its vote
  if(elector in votes) {
    const existing = votes[elector];
    if(existing === false || votingEvent === false) {
      /*logger.verbose('Continuity2017 detected byzantine node ',
        {ledgerNode: ledgerNode.id, elector});*/
    } else if(votingEvent._generation > existing._generation) {
      /*logger.verbose('Continuity2017 replacing voting event', {
        ledgerNode: ledgerNode.id,
        elector,
        votingEvent: votingEvent.eventHash
      });*/
      votes[elector] = votingEvent;
    } else if(
      votingEvent._generation === existing._generation &&
      votingEvent !== existing) {
      // byzantine node!
      /*logger.verbose('Continuity2017 detected byzantine node', {
        ledgerNode: ledgerNode.id,
        elector,
        votingEvent: votingEvent.eventHash
      });*/
      votes[elector] = false;
    }
  } else {
    /*logger.verbose('Continuity2017 found new voting event', {
      ledgerNode: ledgerNode.id,
      elector,
      votingEvent: votingEvent.eventHash
    });*/
    votes[elector] = votingEvent;
  }
}

function _hasSufficientEndorsements(
  {ledgerNode, x, descendants, electorSet, supermajority}) {
  // always count `x` as self-endorsed
  const endorsements = new Set([_getCreator(x)]);
  let total = 1;
  let next = [x];
  //console.log('checking for sufficient endorsements...');
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      const d = descendants[event.eventHash];
      if(d) {
        // `event` is in the computed path of descendants
        for(const e of d) {
          const creator = _getCreator(e);
          if(!endorsements.has(creator) && electorSet.has(creator)) {
            endorsements.add(creator);
            total++;
            //console.log('total', total, 'supermajority', supermajority);
            //console.log('electors', electorSet);
            //console.log('endorsements', endorsements);
            if(total >= supermajority) {
              return true;
            }
          }
        }
        next.push(...d);
      }
    }
    next = _.uniq(next);
  }
  return false;
}

function _tallyBranches({ledgerNode, yByElector, blockHeight, electors}) {
  /* Algorithm:

  1. Iterate through each Y branch, starting at Y and moving down its
     tree children.
  2. Find all descendants between the current event and every Y.
  3. Filter the descendants into a `votes` map of elector => most recent event
     created by that elector, creating the set of events that are participating
     in an experiment to see what Y candidates the various nodes are
     supporting. If a byzantine node is detected, mark the elector's entry
     as `false` and it remains that way until consensus is reached.
  4. If all of the participants are supporting some set of Y candidates,
     then compute the current tree child's supported value. Otherwise,
     continue to the next iteration of the loop. Eventually, all
     participants will support a value and the tree child's supported
     value can be computed (or consensus will be reached and the loop
     will exit early).
  5. Once a tree child is supporting a value, move onto the next tree
     child and continue until no more remain.
  */
  const electorSet = new Set(Object.keys(yByElector));
  let next = _.values(yByElector);
  while(next.length > 0) {
    const current = next;
    next = [];
    for(let event of current) {
      if(_isHistoryEntryRegularEvent(event)) {
        if(!event._children || event._children.length !== 1) {
          continue;
        }
        event = event._children[0];
      }
      if(!event._supporting) {
        if(!event._votes) {
          // reuse and update tree parent's votes
          // FIXME: do we need to copy or can we reuse?
          event._votes = {};
          const creator = _getCreator(event);
          if(event._treeParent._votes) {
            for(let elector in event._treeParent._votes) {
              event._votes[elector] = event._treeParent._votes[elector];
            }
          } else {
            // TODO: remove `else`, was an attempt to initialize Ys differently
            // tree parent has no votes, must be a Y, initialize to self
            //event._votes[creator] = event;
          }

          // determine ancestors that will partipicate in the experiment,
          // looking at descendants of every Y
          if(!event._yDescendants) {
            if(event._treeParent._yDescendants) {
              event._yDescendants = event._treeParent._yDescendants;
            } else {
              event._yDescendants = {};
              electorSet.forEach(e => event._yDescendants[e] = {});
            }
          }
          /*let startTime = Date.now();
          console.log(
            'Start sync _findConsensusMergeEventProof: find Y descendants');*/
          for(const elector of electorSet) {
            const y = yByElector[elector];
            const descendants = event._yDescendants[elector];
            _findDescendantsInPath({
              ledgerNode,
              x: y,
              y: event,
              descendants,
              ancestryMap: y._xDescendants
            });
            /*console.log('descendants from y ' + y._generation + ' to event',
              event._generation,
              _flattenDescendants({ledgerNode, x: y, descendants}).map(r=>r._generation));*/
            _updateToMostRecentVotes(
              {ledgerNode, y, yByElector, descendants, electorSet,
                votes: event._votes});
          }
          /*console.log(
            'End sync _findConsensusMergeEventProof: find Y descendants', {
              duration: Date.now() - startTime
            });*/
        }

        const votingEvents = _.values(event._votes);
        // TODO: remove... attempt to initialize Ys differently
        /*if(votingEvents.length === 1) {
          // no ancestors other than self, initialize support for self
          event._supporting = [event];
          continue;
        }*/

        // TODO: actually, should only need `2f` votes to be supporting
        // something in order to do the tally and make a decision ... so this
        // could be optimized in the case that there are more than that
        // (at least this is true in the case of binary decision, not sure
        // if still applies here)... couldn't choose a new set of Ys in
        // the case of a tie -- but could choose all Y ancestors, either way
        // not likely worth it given all votes can be simply computed.
        // TODO: at a minimum, if a tally indicates that our vote will not be
        // switched we could avoid waiting for the other votes to come in
        if(votingEvents.some(e => !('_supporting' in e))) {
          // some votes are still outstanding, cannot tally yet
          next.push(event);
          continue;
        }
      }

      /*let startTime = Date.now();
      console.log('Start sync _findConsensusMergeEventProof: _tally');*/
      const result = _tally({ledgerNode, event, yByElector, blockHeight, electors});
      /*console.log('End sync _findConsensusMergeEventProof: _tally', {
        duration: Date.now() - startTime
      });*/
      if(result) {
        // consensus reached
        return result;
      }

      // add tree children
      next.push(...(event._treeChildren || []));
    }
  }

  return null;
}

function _tally({ledgerNode, event, yByElector, blockHeight, electors}) {
  /*
  If we see a decision (it has `_confirmation`):
    We're done, return the related confirmation's supported set.
  */
  if(event._confirmation) {
    /*console.log('DECISION DETECTED AT BLOCK', blockHeight, {
      creator: _getCreator(event),
      eventHash: event.eventHash,
      generation: event._generation
    });*/
    // consensus reached
    return event._confirmation._supporting;
  }

  // propagate confirmed status
  if(event._treeParent._confirmed) {
    event._confirmed = true;
  }

  /*
  Otherwise tally, because we have to compare things.
  Always get the top choice and see if it's a supermajority.
  */
  logger.verbose('Continuity2017 _tally finding votes seen...',
    {ledgerNode: ledgerNode.id, eventHash: event.eventHash});
  // tally votes
  const tally = [];
  _.values(event._votes).forEach(e => {
    if(e === false) {
      // do not count byzantine votes
      return;
    }
    const tallyResult = _.find(tally, _findSetInTally(e._supporting));
    if(tallyResult) {
      // ensure same instance of set is used for faster comparisons
      e._supporting = tallyResult.set;
      tallyResult.count++;
    } else {
      tally.push({
        set: e._supporting,
        count: 1
      });
    }
  });

  // TODO: remove me
  /*console.log('BLOCK HEIGHT', blockHeight);
  console.log('votes received at generation', event._generation);
  console.log('by experimenter', _getCreator(event));
  console.log('------------------');
  Object.keys(event._votes).forEach(k => {
    console.log('|');
    console.log('|-elector:', k);
    console.log('  generation:', event._votes[k]._generation);
    event._votes[k]._supporting.forEach(r => {
      console.log(
        '    Y generation:', r._generation, ', creator:', _getCreator(r));
    });
  });
  console.log('------------------');*/

  // sort tally by count
  tally.sort((a, b) => b.count - a.count);

  // TODO: ensure logging `tally` is not slow
  /*logger.verbose('Continuity2017 _tally tally',
    {ledgerNode: ledgerNode.id, tally});*/
  // TODO: remove me
  /*console.log('VOTE TALLY', ledgerNode.id, tally.map(t => ({
    count: t.count,
    set: JSON.stringify(t.set.map(r => r.eventHash))
  })));*/

  // choose first choice of sorted tallies (Note: it is always a valid choice
  // because it is entirely based on ancestry, so all Ys must be present)
  let topChoice = tally[0];

  // if top choice has supermajority support...
  const supermajority = api.twoThirdsMajority(electors.length);
  const hasSupermajority = topChoice && topChoice.count >= supermajority;
  // FIXME: remove me
  /*if(hasSupermajority) {
    console.log('SUPERMAJORITY VOTE DETECTED AT BLOCK', blockHeight,
      topChoice.set.map(r => ({
        creator: _getCreator(r),
        eventHash: r.eventHash,
        generation: r._generation
      })));
  }*/
  // FIXME: remove above

  // prepare to compute the next choice
  let nextChoice;

  // get event creator for use below
  const creator = _getCreator(event);

  /*
  Then...

  If the event is a confirmation (i.e. it has a precommit):
    Compare the top choice to the precommit and if it match and has at
    least a supermajority of support, create a diverse pedigree decision point
    in the future and return.
    Otherwise, continue...
  */
  if(event._preCommit) {
    //console.log('has precommit');
    const preCommit = _.find(
      tally, _findSetInTally(event._preCommit._supporting));
    if(preCommit === topChoice && hasSupermajority) {
      //console.log('precommit supports same set, confirmed');

      // preCommit is confirmed
      event._preCommit._confirmed = event._confirmed = true;

      // support the confirmation
      event._supporting = topChoice.set;
      event._votes[creator] = event;

      // compute a decision event
      const ancestryMap = _buildAncestryMap(event);
      const decision = _findDiversePedigreeMergeEvent(
        {ledgerNode, x: event, electors, supermajority, descendants: {},
          ancestryMap: ancestryMap});
      if(decision) {
        /*console.log('marking decision event', decision._generation,
          ' with confirmation event', event._generation);*/
        decision._confirmation = event;
      } else {
        //console.log('no decision event found yet');
      }

      return null;
    }

    event._preCommit._confirmed = event._confirmed = false;
    /*console.log('precommit supports another set',
      topChoice.set.map(r => r.eventHash),
      event._preCommit._supporting.map(r => r.eventHash));*/
  } else {
    /*
    Else if any other past event has a confirmation event that is
    pending or successful (ignoring byzantine?):
      Then set next choice to the confirmation.
      And continue...
    */
    const confirmation = _.find(_.values(event._votes), r => r._confirmed);
    if(confirmation && confirmation._confirmed !== false) {
      nextChoice = _.find(tally, _findSetInTally(confirmation._supporting));
      //console.log('other confirmation found, adopting its support');
    } else {
      //console.log('no other confirmation found');
    }
  }

  /*
  If no next choice has been set:
    Then set next choice to the union.
    And continue...
  */
  if(!nextChoice) {
    // compute the union of all supported sets
    const union = Object.keys(event._votes).map(elector => yByElector[elector]);
    //console.log('choosing union', union.map(r => r._generation));

    // set the next choice to the matching tally or create it
    nextChoice = _.find(tally, _findSetInTally(union));
    if(!nextChoice) {
      // create new choice
      nextChoice = {set: union, count: 0};
    }
  }

  /*
  Get the previous choice.

  If the previous choice is different from the new choice, increment the
  new choice count.
  */
  const previousChoice = event._votes[creator] ? _.find(
    tally, _findSetInTally(event._votes[creator]._supporting)) : null;

  // if vote has changed
  if(previousChoice !== nextChoice) {
    // increment next choice count
    nextChoice.count++;
  }

  /*
  If the next choice has a supermajority, create a precommit and a
  confirmation event.
  */
  if(nextChoice.count >= supermajority) {
    // optimize to only create another precommit if an existing precommit on
    // a tree ancestor exists with an undefined `_confirmed` status and there
    // has been no vote change since then
    let found = false;
    let parent = event._treeParent;
    while(parent) {
      if(parent._isPreCommit && !('_confirmed' in parent) &&
        _compareSupportSet(nextChoice.set, parent._supporting)) {
        found = true;
        break;
      }
      parent = parent._treeParent;
    }

    if(!found) {
      // mark event as a precommit and compute a confirmation event
      event._isPreCommit = true;
      const ancestryMap = _buildAncestryMap(event);
      const confirmation = _findDiversePedigreeMergeEvent(
        {ledgerNode, x: event, electors, supermajority, descendants: {},
          ancestryMap: ancestryMap});
      if(confirmation) {
        /*console.log('marking confirmation', confirmation._generation,
          ' with precommit', event._generation);*/
        confirmation._preCommit = event;
      }
    }
  }

  // use next choice
  event._supporting = nextChoice.set;
  event._votes[creator] = event;

  // if(topChoice && topChoice.count >= supermajority) {
  //   console.log('SUPERMAJORITY VOTE DETECTED AT BLOCK', blockHeight,
  //     topChoice.set.map(r => ({
  //       creator: _getCreator(r),
  //       eventHash: r.eventHash,
  //       generation: r._generation
  //     })));

  //   // supermajority 1, trivially decide
  //   if(supermajority === 1) {
  //     console.log('trivial single node case, consensus reached!');
  //     return topChoice.set;
  //   }

  //   // `event` has a preCommit, check it to see if the supported set is the
  //   // same, in which case we confirm it
  //   if(event._preCommit) {
  //     console.log('has precommit');
  //     const preCommit = _.find(
  //       tally, _findSetInTally(event._preCommit._supporting));
  //     if(preCommit === topChoice) {
  //       console.log('precommit supports same set, confirmed');
  //       // preCommit is confirmed
  //       preCommit._confirmed = event._confirmed = true;
  //       // support the confirmation
  //       event._supporting = tally[0].set;
  //       event._votes[creator] = event;
  //       return null;
  //     }
  //     preCommit._confirmed = event._confimred = false;
  //     console.log('precommit supports another set',
  //       topChoice.set.map(r => r.eventHash),
  //       event._preCommit._supporting.map(r => r.eventHash));
  //     //process.exit(1);
  //   }
  //   console.log('no precommit!');

  //   const creator = _getCreator(event);
  //   if(!event._supporting && event._votes[creator] !== false) {
  //     // support the winner
  //     event._supporting = tally[0].set;
  //     event._votes[creator] = event;

  //     // compute a post commit event
  //     const ancestryMap = _buildAncestryMap(event);
  //     const postCommit = _findDiversePedigreeMergeEvent(
  //       {ledgerNode, x: event, electors, supermajority, descendants: {},
  //         ancestryMap: ancestryMap});
  //     if(postCommit) {
  //       console.log('marking post commit', postCommit._generation,
  //         ' with precommit', event._generation);
  //       postCommit._preCommit = event;
  //     }
  //   }*/
  //   //return topChoice.set;
  // }

  // // event already supporting a value (initial Y case)
  // if(event._supporting) {
  //   return null;
  // }

  // // byzantine node, do not change support, it's always invalid
  // const creator = _getCreator(event);
  // if(event._votes[creator] === false) {
  //   event._supporting = false;
  //   return null;
  // }

  // // get previously supported value (there won't be one if the tree parent
  // // is not a descendant of any Y, in which case we are making our first
  // // choice)
  // const previousChoice = event._votes[creator] ? _.find(
  //   tally, _findSetInTally(event._votes[creator]._supporting)) : null;
  // if(previousChoice/* && topChoice !== previousChoice*/) {
  //   // total all votes and compute other votes
  //   const total = tally.reduce(
  //     (accumulator, currentValue) => accumulator + currentValue.count, 0);

  //   // compute whether or not the previous choice could still win the vote
  //   // (i.e. `f + 1` other electors are supporting something else)
  //   const remaining = electors.length - total;
  //   const max = previousChoice.count + remaining;
  //   const canWinVote = max >= supermajority;

  //   // if the previous choice can still win *OR* it has a cardinality of
  //   // a supermajority (`2f + 1`), then we must not change our vote
  //   if(canWinVote) {// || previousChoice.set.length === supermajority) {
  //     // continue to support previous choice
  //     event._supporting = previousChoice.set;
  //     event._votes[creator] = event;

  //     // TODO: reorganize code to avoid doing this duplicate code here
  //     /*if(previousChoice.count >= supermajority) {
  //       // compute a post commit event
  //       const ancestryMap = _buildAncestryMap(event);
  //       const postCommit = _findDiversePedigreeMergeEvent(
  //         {ledgerNode, x: event, electors, supermajority, descendants: {},
  //           ancestryMap: ancestryMap});
  //       if(postCommit) {
  //         console.log('marking post commit', postCommit._generation,
  //           ' with precommit', event._generation);
  //         postCommit._preCommit = event;
  //       }
  //     }*/

  //     /*logger.verbose('Continuity2017 _tally continuing support', {
  //       ledgerNode: ledgerNode.id,
  //       notVoted: remaining,
  //       currentVotes: previousChoice.count,
  //       requiredVotes: supermajority,
  //       eventHash: event.eventHash
  //     });*/
  //     // TODO: remove me
  //     /*console.log('continue supporting, remaining votes',
  //       remaining, 'count', previousChoice.count,
  //       'supporting', event._supporting);*/
  //     return null;
  //   }

  //   // TODO: need to define ways won't be interrupting (or we *know* that we
  //   // didn't interrupt someone else's experiment) ... and if we didn't
  //   // interrupt them, then it's like they responded to us ... which means
  //   // we can use their vote to change our mind... if anyone is interrupted,
  //   // you can't use their vote because must treat it like they canceled
  //   // their experiment in that case.

  //   // TODO: can we look at the votes our voters saw and only use the
  //   // ones that are the same (or that meet condition X) to influence
  //   // changing our mind?

  //   // FIXME: remove me
  //   // if any tally is exactly `f` (there are `3f + 1` electors), then we
  //   // must not switch our vote because we could decide concurrently
  //   /*const f = (electors.length - 1) / 3;
  //   for(const t of tally) {
  //     if(t.count === f) {
  //       event._supporting = previousChoice.set;
  //       event._votes[creator] = event;
  //       return null;
  //     }
  //     if(t.count < f) {
  //       break;
  //     }
  //   }*/
  // }

  // // TODO: throw in pseudorandomness every `g(f)` merge events on a given
  // //   branch ... where `f` is failures and `g(f)` gives the optimal number
  // //   of merge events to wait for before throwing in pseudorandom voting
  // //   (may need to put some of the code for detecting when to add entropy
  // //   higher up in _tallyBranches)
  // // TODO: it may actually be the case that the randomness used to select
  // //   nodes to gossip with is sufficient because there is no round-based
  // //   synchronization (the continuity algorithm is roundless) that would
  // //   neutralize it -- that coupled with the fact that nodes converge on
  // //   a union of endorsed Ys, may mean no extra pseudorandomness is required

  // // TODO: if top tally has 2f and we're not part of the 2f, must union
  // // to prevent a concurrnet decision from happening???
  // // `2f` === supermajority - 1, clear top choice so we will union
  // /*if(topChoice && topChoice.count === (supermajority - 1) &&
  //   previousChoice !== topChoice) {
  //   topChoice = null;
  // }*/

  // // in the case of a tie, clear the top choice so we will union
  // /*if(topChoice && tally.length > 1 && topChoice.count === tally[1].count) {
  //   topChoice = null;
  // }*/

  // // compute the union of all supported set at this point
  // const union = Object.keys(event._votes).map(elector => yByElector[elector]);

  // // find the matching top choice or create it
  // topChoice = _.find(tally, _findSetInTally(union));
  // if(!topChoice) {
  //   // create new top choice
  //   topChoice = {set: union, count: 0};
  // }

  // // if the cardinality of the union is <= a supermajority (`2f + 1`), then
  // // choose it
  // //if(union.length <= supermajority) {
  //   event._supporting = topChoice.set;
  //   event._votes[creator] = event;
  // //} else {
  //   // TODO: compute a new set that includes the previous set *and* some
  //   // pseudo-randomly chosen other Ys from the event's ancestry
  //   // TODO: _.sort(_.difference(union, previousChoice.set), (pseudorandom) {});
  //   // TODO: pick first (supermajority - sorted.length) from sorted
  // //}

  // // if vote has changed
  // if(previousChoice !== topChoice) {
  //   // increment top choice count
  //   topChoice.count++;
  // }

  // // // if there's no single top choice at this point, try to make a better
  // // // choice that includes all Ys endorsed at this point
  // // if(!topChoice) {
  // //   // TODO: might need to `.filter(elector => event._votes[elector])` to
  // //   //   eliminate byzantine events... or do we actually need to keep them?
  // //   const union = Object.keys(event._votes).map(elector => yByElector[elector]);
  // //   topChoice = _.find(tally, _findSetInTally(union));
  // //   if(!topChoice) {
  // //     // create new top choice
  // //     topChoice = {set: union, count: 0};
  // //   }

  // //   /*logger.verbose('Continuity2017 _tally event supporting all endorsed Ys', {
  // //     ledgerNode: ledgerNode.id,
  // //     eventHash: event.eventHash,
  // //     allEndorsedYs: topChoice.set.map(r => r.eventHash),
  // //     endorsedYsCount: topChoice.set.length
  // //   });*/
  // // }

  // // event._supporting = topChoice.set;
  // // event._votes[creator] = event;

  // // // if vote has changed
  // // if(previousChoice !== topChoice) {
  // //   // increment top choice count
  // //   topChoice.count++;
  // // }

  // // now, if top choice has a supermajority and cardinality of `2f + 1` ...
  // if(topChoice && topChoice.count >= supermajority) {// &&
  //   //topChoice.set.length === supermajority) {
  //   // consensus reached
  //   if(previousChoice) {
  //     console.log('previousChoice', previousChoice.set.map(r => ({
  //       creator: _getCreator(r),
  //       eventHash: r.eventHash,
  //       generation: r._generation
  //     })));
  //   }
  //   console.log('top choice count', topChoice.count);
  //   console.log('SUPERMAJORITY VOTE DETECTED AT BLOCK', blockHeight,
  //     topChoice.set.map(r => ({
  //       creator: _getCreator(r),
  //       eventHash: r.eventHash,
  //       generation: r._generation
  //     })));
  //   /*
  //   // TODO: optimize `descendants`?
  //   // compute a post commit event
  //   const ancestryMap = _buildAncestryMap(event);
  //   const postCommit = _findDiversePedigreeMergeEvent(
  //     {ledgerNode, x: event, electors, supermajority, descendants: {},
  //       ancestryMap: ancestryMap});
  //   if(postCommit) {
  //     console.log('marking post commit', postCommit._generation,
  //       ' with precommit', event._generation);
  //     postCommit._preCommit = event;
  //   }
  //   return null;*/
  //   return topChoice.set;
  // }

  return null;
}

function _isHistoryEntryRegularEvent(x) {
  return !jsonld.hasValue(x.event, 'type', 'ContinuityMergeEvent');
}

function _findSetInTally(set) {
  const a = set.map(r => r.eventHash);
  return tallyResult => {
    if(tallyResult.set === set) {
      return true;
    }
    const b = tallyResult.set.map(r => r.eventHash);
    return a.length === b.length && _.difference(a, b).length === 0;
  };
}

function _compareSupportSet(set1, set2) {
  if(set1 === set2) {
    return true;
  }
  const a = set1.map(r => r.eventHash);
  const b = set2.map(r => r.eventHash);
  return a.length === b.length && _.difference(a, b).length === 0;
}

function _buildAncestryMaps(events) {
  const ancestryMaps = {};
  events.forEach(e => {
    ancestryMaps[e.eventHash] = _buildAncestryMap(e);
  });
  return ancestryMaps;
}

function _buildAncestryMap(event) {
  const map = {};
  let next = [event];
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(!(event.eventHash in map)) {
        map[event.eventHash] = true;
        if(event._parents) {
          next.push(...event._parents);
        }
      }
    }
    next = _.uniq(next);
  }
  /*let parent = event;
  // TODO: why only adding tree parents? ... why not use all parents?
  while(parent) {
    map[parent.eventHash] = true;
    parent = parent._treeParent;
  }*/
  return map;
}

function _allEndorsedYs({event, allYs, yAncestryMaps}) {
  const found = new Set();
  _hasAncestors({
    target: event,
    candidates: allYs,
    candidateAncestryMaps: yAncestryMaps,
    found
  });
  return found;
}

/**
 * Returns `true` if the `target` has `min` ancestors from `candidates`.
 *
 * @param target the event to check the ancestry of for `candidates`.
 * @param candidates the possible ancestors of `target`.
 * @param min the minimum number of candidates that must be ancestors.
 * @param candidateAncestryMaps for the search halting positions.
 * @param found a Set for tracking which candidates have been found so far.
 *
 * @return `true` if `candidate` is an ancestor of `target`, `false` if not.
 */
function _hasAncestors(
  {target, candidates, min = candidates.length,
    candidateAncestryMaps, found}) {
  const candidateSet = new Set(candidates);
  let next = target._parents.filter(p => !_isHistoryEntryRegularEvent(p));
  let difference = [...candidateSet].filter(x => !found.has(x));
  // include `checked` as an optimization to avoid double checking ancestors
  const checked = new Set();
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const parent of current) {
      // skip already checked parents
      if(checked.has(parent)) {
        continue;
      }
      checked.add(parent);
      if(candidateSet.has(parent)) {
        if(!found.has(parent)) {
          found.add(parent);
          if(found.size >= min) {
            return true;
          }
          difference.splice(difference.indexOf(parent), 1);
        }
      }
      // determine if parent can be now be ruled out as leading to any further
      // discoveries by testing if it is not in at least one of the remaining
      // candidate ancestry maps
      const viable = difference.some(
        c => !(parent.eventHash in candidateAncestryMaps[c.eventHash]));
      if(viable && parent._parents) {
        next.push(...parent._parents.filter(
          p => !_isHistoryEntryRegularEvent(p)));
      }
    }
    next = _.uniq(next);
  }

  return found.size >= min;
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
        if(!jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
          // regular event
          return;
        }
        // TODO: is `e.signature.creator` check robust enough? Can it assume
        //   a single signature and that it's by the voter? (merge events are
        //   only meant to be signed by the voter)
        electors.push(event.signature.creator);
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

      // TODO: can we easily remove previously detected byzantine nodes from
      // electors?

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
            aggregate[id] = {id, weight: 2};
          }
        });
      electors = Object.keys(aggregate).map(k => aggregate[k]);

      // get elector count, defaulting to MAX_ELECTOR_COUNT if not set
      // (hardcoded, all nodes must do the same thing -- but ideally this would
      // *always* be set)
      const electorCount = results.config.electorCount || MAX_ELECTOR_COUNT;

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
        // 1. sort descending by weight
        if(a.weight !== b.weight) {
          // FIXME: with current weights, this prevents elector cycling
          //   if commented out, will force elector cycling, needs adjustment
          return b.weight - a.weight;
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

      // reduce electors to highest multiple of `3f + 1`, i.e.
      // `electors.length % 3 === 1` or electors < 4 ... electors MUST be a
      // multiple of `3f + 1` for BFT or 1 for trivial dictator case
      while(electors.length > 1 && (electors.length % 3 !== 1)) {
        electors.pop();
      }
      // FIXME: remove me
      if(electors.length === 1) {
        electors[0].id = consensusProof[0].signature.creator;
      }

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
