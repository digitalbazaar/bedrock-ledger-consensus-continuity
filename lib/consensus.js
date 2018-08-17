/*!
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const crypto = require('crypto');

// module API
const api = {};
module.exports = api;

// exposed for testing
api._getElectorBranches = _getElectorBranches;
api._findMergeEventProof = _findMergeEventProof;

/**
 * Determine if any new merge events have reached consensus in the given
 * history summary of merge events w/o consensus.
 *
 * @param ledgerNodeId the ID of the local ledger node.
 * @param history recent history rooted at the ledger node's local branch
 *          including ONLY merge events, it must NOT include local regular
 *          events.
 * @param electors the current electors.
 * @param logger the logger to use.
 *
 * @return `null` if no consensus was found or an object `result` if it has,
 *          where:
 *            result.eventHashes the hashes of events that have reached
 *              consensus.
 *            result.consensusProofHashes the hashes of events proving
 *              consensus.
 */
api.findConsensus = ({
  ledgerNodeId, history, blockHeight, electors,
  logger = {debug: () => {}, verbose: () => {}}
}) => {
  // TODO: Note: once computed, merge event Y+X candidates for each
  //   elector can be cached for quick retrieval in the future without
  //   the need to recompute them (they never change) for a given block...
  //   so the next blockHeight, elector, and X pair (its hash) could be
  //   stored in the continuity2017 meta for each candidate merge event Y
  logger.verbose('Start sync _getElectorBranches, branches', {electors});
  let startTime = Date.now();
  const tails = _getElectorBranches({history, electors});
  logger.verbose('End sync _getElectorBranches', {
    duration: Date.now() - startTime
  });
  logger.verbose('Start sync _findMergeEventProof');
  //console.log('Start sync _findMergeEventProof');
  startTime = Date.now();
  const proof = _findMergeEventProof(
    {ledgerNodeId, tails, blockHeight, electors, logger});
  /*console.log('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  logger.verbose('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });
  if(proof.consensus.length === 0) {
    logger.verbose('findConsensus no proof found, exiting');
    return null;
  }
  logger.verbose('findConsensus proof found, proceeding...');
  const creators = new Set();
  const allXs = proof.consensus.map(p => p.x);
  const consensusProofHashes = _.uniq(
    proof.consensus.reduce((aggregator, current) => {
      creators.add(_getCreator(current.x));
      current.proof.forEach(r => {
        creators.add(_getCreator(r));
        aggregator.push(r.eventHash);
      });
      return aggregator;
    }, []));
  const eventHashes = _getAncestorHashes({ledgerNodeId, allXs});
  // return event and consensus proof hashes and creators
  return {
    eventHashes,
    consensusProofHashes,
    creators: [...creators]
  };
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
 * Determines the maximum number of failed electors.
 *
 * @param electorCount the total number of electors.
 *
 * @return the maximum number of electors that can fail (`f`).
 */
api.maximumFailures = electorCount =>
  (api.twoThirdsMajority(electorCount) - 1) / 3;

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

  if(!history.linked) {
    // TODO: move to a utility function
    // build history links
    for(const e of history.events) {
      for(const parentHash of e.event.parentHash) {
        const parent = history.eventMap[parentHash];
        if(!parent) {
          continue;
        }
        e._parents.push(parent);
        parent._children.push(e);
      }
    }
    history.linked = true;
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
function _getAncestorHashes({allXs, ledgerNodeId}) {
  // get all ancestor hashes from every consensus X
  // TODO: `hashes` seems like it could use event references here rather
  //   than hashes themselves for faster comparisons, etc.
  const hashes = new Set();
  const hashBuffers = [];
  const descendants = [];
  const parentHashes = new Set();
  for(const x of allXs) {
    // TODO: anything missed or different here with byzantine forks?
    let next = [x];
    while(next.length > 0) {
      const current = next;
      next = [];
      for(const event of current) {
        if(hashes.has(event.eventHash)) {
          continue;
        }
        descendants.push(event);
        event._hashBuffer = _parseHash(event.eventHash);
        hashBuffers.push(event._hashBuffer);
        hashes.add(event.eventHash);
        const parents = event._parents || [];
        next.push(...parents);
        // ensure all regular events are added
        event.event.parentHash.forEach(parentHash => {
          if(!hashes.has(parentHash) && !parentHashes.has(parentHash) &&
            !event._parents.some(p => p.eventHash === parentHash)) {
            parentHashes.add(parentHash);
            // synthesize regular event with enough properties to check
            // ancestry below during sort
            descendants.push({
              eventHash: parentHash,
              meta: {
                continuity2017: {
                  creator: _getCreator(event)
                }
              },
              _parents: event._treeParent ? [event._treeParent] : [],
              _generation: event._generation - 1
            });
          }
        });
      }
    }
  }
  const mergeEventHashes = [...hashes];
  const parentHashesArray = _.difference([...parentHashes], mergeEventHashes);
  parentHashesArray.forEach(h => hashes.add(h));

  // compute base hash for mixin
  const baseHashBuffer = _createBaseHashBuffer(hashBuffers);

  // TODO: implement using Coffman–Graham or Kahn's + lexicographical ordering
  //   on hashes w/baseHash mixin, etc.

  // find total ordering of events
  const order = descendants;
  order.sort((a, b) => {
    let diff = 0;

    if(_getCreator(a) === _getCreator(b)) {
      diff = (a._generation || 0) - (b._generation || 0);
    }

    if(diff !== 0) {
      return diff;
    }

    if(!a._ancestryMap) {
      a._ancestryMap = _buildAncestryMap(a);
    }
    if(!b._ancestryMap) {
      b._ancestryMap = _buildAncestryMap(b);
    }

    // determine if `a` descends from `b` or vice versa
    if(_hasAncestors({
      target: a,
      candidates: [b],
      candidateAncestryMaps: {[b.eventHash]: b._ancestryMap},
      found: new Set()
    })) {
      // `a` descends from `b`
      diff = 1;
    } else if(_hasAncestors({
      target: b,
      candidates: [a],
      candidateAncestryMaps: {[a.eventHash]: a._ancestryMap},
      found: new Set()
    })) {
      // `b` descends from `a`
      diff = -1;
    }

    if(diff !== 0) {
      return diff;
    }

    // sort by hash augmented via base hash (base hash is not under the
    // control of `a` or `b` so they cannot use it to influence ordering)
    if(!a._consensusSortHash) {
      if(!a._hashBuffer) {
        a._hashBuffer = _parseHash(a.eventHash);
      }
      // mixin baseHashBuffer (modifies _hashBuffer in place to optimize)
      _xor(a._hashBuffer, baseHashBuffer);
      a._consensusSortHash = a._hashBuffer;
    }
    if(!b._consensusSortHash) {
      if(!b._hashBuffer) {
        b._hashBuffer = _parseHash(b.eventHash);
      }
      // mixin baseHashBuffer (modifies _hashBuffer in place to optimize)
      _xor(b._hashBuffer, baseHashBuffer);
      b._consensusSortHash = b._hashBuffer;
    }
    return Buffer.compare(a._hashBuffer, b._hashBuffer);
  });

  return {
    mergeEventHashes,
    parentHashes: parentHashesArray,
    order: order.map(r => r.eventHash)
  };
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
 * @param ledgerNodeId the ID of the local ledger node.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *          by elector ID.
 * @param electors the current set of electors.
 * @param logger the logger to use.
 *
 * @return a map with `consensus` and `yCandidates`; the `consensus` key's
 *         value is an array of merge event X and Y pairs where each merge
 *         event Y and its history proves its paired merge event X has been
 *         endorsed by a super majority of electors -- another key, `proof` is
 *         also included with each pair that includes `y` and its direct
 *         ancestors until `x`, these, in total, constitute endorsements of `x`.
 */
function _findMergeEventProof({
  ledgerNodeId, tails, blockHeight, electors,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  let startTime = Date.now();
  logger.verbose('Start sync _findMergeEventProofCandidates');
  //console.log('Start sync _findMergeEventProofCandidates');
  const candidates = _findMergeEventProofCandidates(
    {ledgerNodeId, tails, blockHeight, electors, logger});
  /*console.log('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });*/
  logger.verbose('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });
  if(!candidates) {
    // no Y candidates yet
    return {consensus: []};
  }

  const yByElector = candidates.yByElector;
  const supermajority = api.twoThirdsMajority(electors.length);
  if(Object.keys(yByElector).length < supermajority) {
    // insufficient Y candidates so far, supermajority not reached
    return {consensus: []};
  }

  startTime = Date.now();
  //console.log('Start sync _findConsensusMergeEventProof');
  logger.verbose('Start sync _findConsensusMergeEventProof');
  const ys = _findConsensusMergeEventProof(
    {ledgerNodeId, yByElector, blockHeight, electors, logger});
  /*console.log('End sync _findConsensusMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  logger.verbose('End sync _findConsensusMergeEventProof', {
    duration: Date.now() - startTime
  });
  if(ys.length === 0) {
    // no consensus yet
    return {consensus: []};
  }

  return {
    // pair Ys with Xs
    consensus: ys.map(y => {
      const x = y._x;
      let proof = _flattenDescendants(
        {ledgerNodeId, x, descendants: y._xDescendants});
      if(proof.length === 0 && supermajority === 1) {
        // always include single elector as proof; enables continuity of that
        // single elector when computing electors in the next block via
        // quick inspection of `block.consenusProof`
        proof = [x];
      }
      return {y, x, proof};
    }),
    // return all yCandidates for debugging purposes
    yCandidates: [].concat(..._.values(yByElector))
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
 * @param ledgerNodeId the ID of the local ledger node.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *          by elector ID.
 * @param electors the current set of electors.
 * @param logger the logger to use.
 *
 * @return `null` or a map containing `yByElectors` and `xByElectors`; in
 *           `yByElectors`, each elector maps to merge event Y that proves a
 *           merge event X has been endorsed by a super majority of electors,
 *           where X and Y are branch-native.
 */
function _findMergeEventProofCandidates({
  ledgerNodeId, tails, blockHeight, electors,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  const supermajority = api.twoThirdsMajority(electors.length);

  //console.log('TAILS', util.inspect(tails, {depth:10}));

  const electorsWithTails = Object.keys(tails);
  // TODO: ensure logging `electorsWithTails` is not slow
  /*logger.verbose('Continuity2017 electors with tails for ledger node ' +
    ledgerNodeId + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNodeId, electorsWithTails});*/
  /*console.log('Continuity2017 electors with tails for ledger node ' +
    ledgerNodeId + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNodeId, electorsWithTails});*/
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

  // Note: we need to allow multiple events for each elector, in the event that
  // they are byzantine -- we must allow this because we must calculate
  // support on remote merge events from their node's perspective, not our
  // own (i.e. we know a node is byzantine and has forked, but they may not
  // so they will calculate support accordingly)

  // find merge event X candidate for each elector
  let startTime = Date.now();
  logger.verbose('Start sync _findMergeEventProofCandidates: Xs');
  for(const elector of electorsWithTails) {
    //console.log('FINDING X for', elector);
    // Note: must not skip electors with multiple tails detected (byzantine),
    // as not every valid node will see the fork and we must calculate their
    // support properly
    const electorTails = tails[elector];

    // TODO: simplify code or make generic to handle N iters before selecting
    //   an `x` where 0 is the default, i.e. where tail is `x`
    const results = electorTails.map(r => ({
      mergeEvent: r,
      descendants: {}
    }));
    /* // find earliest `x` for the elector's tail
    const results = _findEndorsementMergeEvent(
      {ledgerNodeId, x: electorTails[0], electors, supermajority});*/
    if(results) {
      //console.log('***X found for', elector, ' at generation ',
      //  result._generation, result);
      xByElector[elector] = results.map(r => r.mergeEvent);
      // include `result` in initial descendants map, it is used to halt
      // searches for Y and in producing the set of events to include in a
      // block should an X be selected
      for(const result of results) {
        result.descendants[result.mergeEvent.eventHash] = [];
        result.mergeEvent._initDescendants = result.descendants;
      }
    } else {
      //console.log('***NO X found for ' + elector);
    }
  }
  logger.verbose('End sync _findMergeEventProofCandidates: Xs', {
    duration: Date.now() - startTime
  });

  // TODO: ensure logging `xByElector` is not slow
  /*logger.verbose('Continuity2017 X merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, xByElector});*/
  /*console.log('Continuity2017 X merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, xByElector});*/

  if(Object.keys(xByElector).length < supermajority) {
    // non-consensus events X from a supermajority of electors have not yet
    // been collected, so return early
    return null;
  }

  // find merge event Y candidate(s) for each elector
  // Note: at most one per elector unless elector is byzantine (then >= 1)
  startTime = Date.now();
  logger.verbose('Start sync _findMergeEventProofCandidates: Y candidates');
  for(const elector in xByElector) {
    for(const x of xByElector[elector]) {
      //console.log('FINDING Y FOR X', x, elector);
      // pass `x._initDescendants` as the ancestry map to use to short-circuit
      // searches as it includes all ancestors of X -- which should not be
      // searched when finding a Y because they cannot lead to X
      const results = _findEndorsementMergeEvent(
        {ledgerNodeId, x, electors, supermajority,
          ancestryMap: x._initDescendants});
      if(results) {
        const mergeEvents = results.map(r => r.mergeEvent);
        if(yByElector[elector]) {
          yByElector[elector] = _.uniq(yByElector[elector].concat(mergeEvents));
        } else {
          yByElector[elector] = mergeEvents;
        }
        for(const result of results) {
          result.mergeEvent._x = x;
          result.mergeEvent._xDescendants = result.descendants;
        }
      }
    }
  }
  logger.verbose('End sync _findMergeEventProofCandidates: Y candidates', {
    duration: Date.now() - startTime
  });

  // TODO: ensure logging `yByElector` is not slow
  /*logger.verbose(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, yByElector});*/
  /*console.log(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, blockHeight, yByElector});*/

  return {yByElector, xByElector};
}

/**
 * Find the earliest merge event for an elector that includes an ancestry of
 * merge events from at least a supermajority of electors. This merge event is
 * said to be an endorsement event for `x` because its ancestry includes merge
 * events from a supermajority of other nodes that descend from `x`. The search
 * starts at `x` and proceeds forward through history. It is possible to find
 * more than one merge event endorsement event if the node that created `x` is
 * byzantine.
 *
 * @param ledgerNodeId the ID of the current ledger node, used for logging.
 * @param x the event in history to begin searching at.
 * @param electors all current electors.
 * @param supermajority the number that constitutes a supermajority of electors.
 * @param descendants an optional map of event hash to descendants that is
 *          populated as they are found.
 * @param ancestryMap an optional map of event hash to ancestors of `x` that is
 *          used to short-circuit searching.
 *
 * @return `null` or an array of the earliest endorsement merge event(s)
 *           coupled with descendants maps: {mergeEvent, descendants}.
 */
function _findEndorsementMergeEvent(
  {ledgerNodeId, x, electors, supermajority, descendants = {},
    ancestryMap = _buildAncestryMap(x)}) {
  //console.log('EVENT', x.eventHash);

  if(supermajority === 1) {
    // trivial case, return `x`
    return [{mergeEvent: x, descendants: _copyDescendants(descendants)}];
  }

  if(!x._treeChildren) {
    // no tree children, so return nothing
    return null;
  }

  const electorSet = new Set(electors.map(e => e.id));
  const results = [];
  let next = x._treeChildren.map(treeDescendant => ({
    treeDescendant,
    descendants: x._treeChildren.length === 1 ?
      descendants : _copyDescendants(descendants)
  }));
  while(next.length > 0) {
    const current = next;
    next = [];
    //console.log('FINDING descendant for: ', x.eventHash);
    //console.log('X creator', _getCreator(x));
    for(const {treeDescendant, descendants} of current) {
      //console.log();
      //console.log('checking generation', treeDescendant._generation);
      //console.log('treeDescendant hash', treeDescendant.eventHash);
      // add all descendants of `x` that are ancestors of `treeDescendant`
      _findDescendantsInPath(
        {ledgerNodeId, x, y: treeDescendant, descendants, ancestryMap});

      // see if there are a supermajority of endorsements of `x` now
      if(_hasSufficientEndorsements(
        {ledgerNodeId, x, descendants, electorSet, supermajority})) {
        //console.log('supermajority of endorsements found at generation', treeDescendant._generation);
        //console.log();
        results.push({
          mergeEvent: treeDescendant,
          descendants
        });
        continue;
      }
      //console.log('not enough endorsements yet at generation', treeDescendant._generation);
      // FIXME: remove me
      //const ancestors = _flattenDescendants({ledgerNodeId, x, descendants});
      //console.log('total descendants so far', ancestors.map(r=>({
      //  creator: _getCreator(r),
      //  generation: r._generation,
      //  hash: r.eventHash
      //})));
      //console.log();

      // must iterate through all possible forks on byzantine nodes; e.g. when
      // computing `y` events, we must return all possible `y` events
      // (byzantine nodes may fork) as some nodes may only see one side of the
      // fork... and we need to properly calculate their support from *their*
      // perspective, not our own
      if(treeDescendant._treeChildren) {
        if(treeDescendant._treeChildren.length === 1) {
          next.push(...treeDescendant._treeChildren.map(treeDescendant => ({
            treeDescendant,
            descendants: descendants
          })));
        } else {
          next.push(...treeDescendant._treeChildren.map(treeDescendant => ({
            treeDescendant,
            descendants: _copyDescendants(descendants)
          })));
        }
      }
    }
  }

  return results.length === 0 ? null : results;
}

function _findConsensusMergeEventProof({
  ledgerNodeId, yByElector, blockHeight, electors,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  /*logger.verbose(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId});*/
  /*console.log(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId});*/

  const allYs = [].concat(..._.values(yByElector));

  // if electors is 1, consensus is trivial
  if(electors.length === 1) {
    return allYs;
  }

  // build map of each Y's ancestry for quick checks of halting conditions
  const yAncestryMaps = _buildAncestryMaps(allYs);
  for(const y of allYs) {
    // include all known initial and X descendants in ancestry map
    const map = yAncestryMaps[y.eventHash];
    for(const hash in y._xDescendants) {
      map[hash] = true;
    }
    for(const hash in y._x._initDescendants) {
      map[hash] = true;
    }
  }

  // initialize all Y votes
  allYs.forEach(y => {
    const supporting = _allEndorsedYs({event: y, allYs, yAncestryMaps});
    // always include self
    supporting.add(y);
    y._votes = {};
    supporting.forEach(supported => {
      const creator = _getCreator(supported);
      if(creator in y._votes) {
        // forked `y` is found, mark byzantine
        y._votes[creator] = false;
      } else {
        y._votes[creator] = supported;
      }
    });
  });

  let startTime = Date.now();
  logger.verbose('Start sync _findConsensusMergeEventProof: _tallyBranches');
  //console.log('Start sync _findConsensusMergeEventProof: _tallyBranches');
  // go through each Y's branch looking for consensus
  const consensus = _tallyBranches(
    {ledgerNodeId, yByElector, blockHeight, electors, logger});
  /*console.log('End sync _findConsensusMergeEventProof: (Branches', {
    duration: Date.now() - startTime
  });*/
  logger.verbose('End sync _findConsensusMergeEventProof: _tallyBranches', {
    duration: Date.now() - startTime
  });

  if(consensus) {
    // TODO: ensure logging `consensus` is not slow
    /*logger.verbose(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNodeId, {ledgerNode: ledgerNodeId, proof: consensus});*/
    /*console.log(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNodeId, {ledgerNode: ledgerNodeId, proof: consensus});*/
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
 * @param ledgerNodeId the ID of the current ledgerNode, for logging.
 * @param x the starting event to find descendants of.
 * @param y the stopping event to find ancestors of.
 * @param descendants the descendants map to use.
 * @param ancestryMap a map of the ancestry of `x` to optimize searching.
 * @param diff an array to populate with new descendants found.
 */
function _findDescendantsInPath(
  {ledgerNodeId, x, y, descendants = {}, ancestryMap = _buildAncestryMap(x),
    diff = null
  }) {
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
      for(const parent of event._parents) {
        //console.log('event.parent', {
        //  creator: _getCreator(parent),
        //  generation: parent._generation,
        //  hash: parent.eventHash
        //});
        const d = descendants[parent.eventHash];
        if(d) {
          if(!d.includes(event)) {
            d.push(event);
            if(diff) {
              diff.push(event);
            }
          }
          //console.log('parent ALREADY in descendants', parent.eventHash);
          continue;
        }
        //console.log('ADDING parent to descendants', parent.eventHash);
        descendants[parent.eventHash] = [event];
        if(diff) {
          diff.push(event);
        }
        next.push(parent);
      }
    }
  }
  //console.log('entries in descendants', Object.keys(descendants));
}

function _copyDescendants(descendants) {
  const copy = {};
  Object.keys(descendants).forEach(k => copy[k] = descendants[k].slice());
  return copy;
}

function _flattenDescendants({ledgerNodeId, x, descendants}) {
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

// `descendants` is a diff
function _updateToMostRecentVotes({
  ledgerNodeId, yByElector, descendants, electorSet, votes,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  const ySetByElector = {};
  for(const elector in yByElector) {
    ySetByElector[elector] = new Set(yByElector[elector]);
  }
  for(const event of descendants) {
    const creator = _getCreator(event);
    if(electorSet.has(creator)) {
      // only include `event` as voting if it is >= to its associated
      // Y's generation
      const creatorYs = ySetByElector[creator];
      let earliestY;
      let creatorY = event;
      while(creatorY) {
        if(creatorYs.has(creatorY)) {
          earliestY = creatorY;
        }
        creatorY = creatorY._treeParent;
      }
      if(earliestY && event._generation >= earliestY._generation) {
        _useMostRecentVotingEvent(
          {ledgerNodeId, elector: creator, votes, votingEvent: event,
            logger});
      }
    }
  }
}

function _useMostRecentVotingEvent({
  ledgerNodeId, elector, votes, votingEvent,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  // only count vote from a particular elector once, using the most
  // recent from that elector; if an elector has two voting events
  // from the same generation or different generations where the younger does
  // not descend from the older, then the creator node is byzantine, invalidate
  // its vote
  if(!(elector in votes)) {
    /*logger.verbose('Continuity2017 found new voting event', {
      ledgerNode: ledgerNodeId,
      elector,
      votingEvent: votingEvent.eventHash
    });*/
    votes[elector] = votingEvent;
    return;
  }

  const existing = votes[elector];
  if(existing === false || votingEvent === false) {
    /*logger.verbose('Continuity2017 detected byzantine node ',
      {ledgerNode: ledgerNodeId, elector});*/
    return;
  }

  // ensure voting events of the same generation are the same event
  if(votingEvent._generation === existing._generation) {
    if(votingEvent !== existing) {
      // byzantine node!
      /*logger.verbose('Continuity2017 detected byzantine node', {
        ledgerNode: ledgerNodeId,
        elector,
        votingEvent: votingEvent.eventHash
      });*/
      votes[elector] = false;
    }
    // voting events match, nothing to do
    return;
  }

  // ensure new voting event and existing voting event do not have forked
  // ancestry by ensuring younger generation descends from older
  let older;
  let younger;
  if(votingEvent._generation > existing._generation) {
    older = existing;
    younger = votingEvent;
  } else {
    older = votingEvent;
    younger = existing;
  }

  if(!_descendsFrom(younger, older)) {
    // byzantine node!
    /*logger.verbose('Continuity2017 detected byzantine node', {
      ledgerNode: ledgerNodeId,
      elector,
      votingEvent: votingEvent.eventHash
    });*/
    votes[elector] = false;
    return;
  }

  /*logger.verbose('Continuity2017 replacing voting event', {
    ledgerNode: ledgerNodeId,
    elector,
    votingEvent: votingEvent.eventHash
  });*/
  votes[elector] = younger;
}

function _descendsFrom(younger, older) {
  let difference = younger._generation - older._generation;
  let parent = younger;
  while(parent && difference > 0) {
    parent = parent._treeParent;
    difference--;
  }
  return parent === older;
}

function _hasSufficientEndorsements(
  {ledgerNodeId, x, descendants, electorSet, supermajority}) {
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

function _tallyBranches({
  ledgerNodeId, yByElector, blockHeight, electors,
  logger = {debug: () => {}, verbose: () => {}}
}) {
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
  let next = [].concat(..._.values(yByElector));
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(event._supporting) {
        // support already determined
        continue;
      }

      if(!event._votes) {
        // reuse and update tree parent's votes
        // FIXME: do we need to copy or can we reuse?
        event._votes = {};
        if(event._treeParent._votes) {
          for(const elector in event._treeParent._votes) {
            event._votes[elector] = event._treeParent._votes[elector];
          }
        }
      }

      // determine ancestors that will partipicate in the experiment,
      // looking at descendants of every Y
      if(!event._yDescendants) {
        if(event._treeParent._yDescendants) {
          if(event._treeParent._treeChildren.length === 1) {
            event._yDescendants = event._treeParent._yDescendants;
          } else {
            event._yDescendants = _copyDescendants(
              event._treeParent._yDescendants);
          }
        } else {
          event._yDescendants = {};
          electorSet.forEach(e => event._yDescendants[e] = {});
        }

        let diff = [];
        //let startTime = Date.now();
        /*logger.debug(
          'Start sync _findConsensusMergeEventProof: find Y descendants');*/
        for(const elector of electorSet) {
          // Note: if `event` has any voting event that has a tree ancestor
          // that has two or more tree children then the creator of that
          // voting event is byzantine; however, we must not indicate that
          // we've detected that it is byzantine unless those two or more
          // tree children are also in the ancestry of `event`, i.e. it
          // cannot be that we simply know about them from some other
          // descendant in the graph -- otherwise we will detect too early
          // calculate `event`'s support differently from other nodes)
          for(const y of yByElector[elector]) {
            // compute diff in descendants
            const descendants = event._yDescendants[elector];
            _findDescendantsInPath({
              ledgerNodeId,
              x: y,
              y: event,
              descendants,
              ancestryMap: y._xDescendants,
              diff
            });
            /*console.log('descendants from y ' + y._generation + ' to event',
              event._generation,
              _flattenDescendants({ledgerNodeId, x: y, descendants}).map(r=>r._generation));*/
          }
        }
        /*logger.debug(
          'End sync _findConsensusMergeEventProof: find Y descendants', {
            duration: Date.now() - startTime
          });*/

        //startTime = Date.now();
        //logger.debug('Start sync _updateToMostRecentVotes');
        diff = _.uniq(diff).filter(e => e !== event);
        _updateToMostRecentVotes(
          {ledgerNodeId, yByElector, descendants: diff, electorSet,
            votes: event._votes, logger});
        /*logger.debug('End sync _updateToMostRecentVotes', {
          duration: Date.now() - startTime
        });*/
      }

      const votingEvents = _.values(event._votes);
      if(votingEvents.some(e => e && e !== event && !('_supporting' in e))) {
        // some votes are still outstanding other than ourselves, cannot
        // tally yet
        next.push(event);
        continue;
      }

      /*const startTime = Date.now();
      logger.debug('Start sync _findConsensusMergeEventProof: _tally');*/
      const result = _tally({ledgerNodeId, event, blockHeight, electors});
      /*logger.debug('End sync _findConsensusMergeEventProof: _tally', {
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

function _tally({
  ledgerNodeId, event, blockHeight, electors,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  /* Choosing support:

  If precommit:
    1. If >= f+1 *other* smaller precommits (endorsed or not), support the
       largest of those.
    2. Otherwise, union *endorsed* precommits.
  If no precommit:
    1. If >= f+1 *endorsed* precommits, support the largest.
    2. Otherwise, union support.

  Rejecting precommits/Deciding:
    1. If support is not for the precommit, reject.
    2. Otherwise, if >= f+1 support something other than the precommit and
       each of those sets is a larger set and has been endorsed, reject.
    3. If not rejected, see if >= 2f+1 endorsed precommits for the same thing
       we support, decide.

  Creating a new precommit:

    1. If no existing precommit (or rejected it) and >= 2f+1 nodes support our
       next choice (endorsement not required), precommit it.
  */

  // compute maximum failures `f` and supermajority `2f+1` thresholds
  const f = api.maximumFailures(electors.length);
  const supermajority = api.twoThirdsMajority(electors.length);

  // TODO: optimize
  /*logger.verbose('Continuity2017 _tally finding votes seen...',
    {ledgerNode: ledgerNodeId, eventHash: event.eventHash});*/
  // tally votes
  /*const startTime = Date.now();
  logger.debug('Start sync _tally vote tally proper');*/
  const tally = [];
  _.values(event._votes).forEach(e => {
    if(e === false || !e._supporting) {
      // do not count byzantine votes or votes without support (initial Ys)
      return;
    }
    const tallyResult = _.find(tally, _findSetInTally(e._supporting));
    const endorsed = _hasEndorsedSwitch(e);
    const preCommit = e._preCommit;
    const endorsedPreCommit = _hasEndorsedPreCommit(e);
    if(tallyResult) {
      // ensure same instance of set is used for faster comparisons
      e._supporting = tallyResult.set;
      tallyResult.count++;
      if(endorsed) {
        tallyResult.endorsedCount++;
      }
      if(preCommit) {
        tallyResult.preCommitCount++;
      }
      if(endorsedPreCommit) {
        tallyResult.endorsedPreCommitCount++;
      }
    } else {
      tally.push({
        set: e._supporting,
        count: 1,
        endorsedCount: endorsed ? 1 : 0,
        preCommitCount: preCommit ? 1 : 0,
        endorsedPreCommitCount: endorsedPreCommit ? 1 : 0
      });
    }
  });
  /*logger.debug('End sync _tally vote tally proper', {
    duration: Date.now() - startTime
  });*/
  /*console.log('tally', tally.map(r => ({
    set: r.set.map(r=>r._generation),
    count: r.count,
    pc: r.preCommitCount
  })));*/

  // TODO: remove me
  /*console.log('==================');
  console.log('BLOCK HEIGHT', blockHeight);
  console.log('votes received at generation', event._generation);
  console.log('by experimenter', _getCreator(event).substr(-5));
  console.log('------------------');
  Object.keys(event._votes).forEach(k => {
    if(event._votes[k]._supporting) {
      console.log('|');
      console.log('|-elector:', k.substr(-5));
      console.log('  generation:', event._votes[k]._generation);
      event._votes[k]._supporting.forEach(r => {
        console.log(
          '    Y generation:', r._generation,
          ', creator:', _getCreator(r).substr(-5));
      });
    }
  });
  console.log('------------------');*/

  // prepare to compute the next choice
  let nextChoice;

  // get event creator for use below
  const creator = _getCreator(event);

  // get existing precommit on the event's branch
  let preCommit;
  if(event._treeParent) {
    preCommit = event._treeParent._preCommit;
  }

  // sort tally results by precommit count (if there is an existing precommit)
  // or by *endorsed* precommit count (if no existing precommit), breaking
  // sort ties using set size
  const countType = preCommit ? 'preCommitCount' : 'endorsedPreCommitCount';
  tally.sort((a, b) => {
    const diff = b[countType] - a[countType];
    if(diff !== 0) {
      return diff;
    }
    return b.set.length - a.set.length;
  });

  /* Choose support when there's an existing precommit:
    1. If >= f+1 *other* smaller precommits (endorsed or not), support the
       largest of those.
    2. Otherwise, union *endorsed* precommits.
  */
  if(preCommit) {
    /*console.log('EXISTING PRECOMMIT FOR',
      preCommit._supporting.map(r=>r._generation));*/

    // determine if there are f+1 smaller precommits *other* than `preCommit`
    const total = tally
      .filter(tallyResult =>
        tallyResult.preCommitCount > 0 &&
        tallyResult.set.length < preCommit._supporting.length)
      .reduce((aggregator, tallyResult) => {
        return aggregator + tallyResult.preCommitCount;
      }, 0);
    if(total >= (f + 1)) {
      //console.log('switching due to f+1 precommits at', event._generation);
      nextChoice = tally[0];
    }

    if(!nextChoice) {
      // TODO: optimize to find largest endorsed precommit faster
      const union = _findUnionPreCommitSet(event, preCommit);
      nextChoice = _.find(tally, _findSetInTally(union));
    }
  } else {
    /* Choose support when there is no existing precommit:
      1. If >= f+1 *endorsed* precommits, support the largest.
      2. Otherwise, union support.
    */
    // determine if there are f+1 endorsed precommits
    const total = tally.reduce((aggregator, tallyResult) => {
      return aggregator + tallyResult.endorsedPreCommitCount;
    }, 0);
    if(total >= (f + 1)) {
      //console.log(
      //  'switching due to f+1 endorsed precommits at', event._generation);
      nextChoice = tally[0];
    }

    if(!nextChoice) {
      // compute the union of all support
      const union = _.uniq([].concat(
        ..._.values(event._votes)
          .filter(r => r)
          .map(r => r._supporting || r)));
      //console.log('choosing union', union.map(r => r._generation));

      // set the next choice to the matching tally or create it
      nextChoice = _.find(tally, _findSetInTally(union));
      if(!nextChoice) {
        // create new choice
        nextChoice = {
          set: union,
          count: 0,
          endorsed: 0,
          preCommitCount: 0,
          endorsedPreCommitCount: 0
        };
      }
    }
  }

  /*
    Get the previous choice. If the previous choice is different from the new
    choice, update the next choice count and calculate an endorse point.
  */
  const previousChoice = event._votes[creator] ? _.find(
    tally, _findSetInTally(event._votes[creator]._supporting)) : null;

  // check if vote has changed
  if(previousChoice !== nextChoice) {
    // set last time switched
    event._lastSwitch = event;
    // increment next choice count
    nextChoice.count++;
    // TODO: optimize to only compute endorsers when necessary
    // compute endorser
    const ancestryMap = _buildAncestryMap(event);
    const endorsePoint = _findEndorsementMergeEvent(
      {ledgerNodeId, x: event, electors, supermajority,
        descendants: {}, ancestryMap: ancestryMap});
    if(endorsePoint) {
      // `endorsePoint` will be an array as byzantine nodes may fork, but
      // since we're searching our own history and we assume we aren't
      // byzantine, we don't have to worry about the array having more than
      // one entry, so just pick the first one
      const endorser = event._endorser = endorsePoint[0].mergeEvent;
      if(endorser._endorses) {
        endorser._endorses.push(event);
      } else {
        endorser._endorses = [event];
      }
      /*console.log(
        'created endorse point for', event._generation,
        'at', event._endorser._generation);*/
    }
  }

  if(event._endorses) {
    // endorse point reached
    if(preCommit && !preCommit._endorsed &&
      event._endorses.includes(preCommit)) {
      nextChoice.endorsedPreCommitCount++;
    }
    event._endorses.forEach(e => e._endorsed = true);
  }

  /*console.log('SUPPORT at ', event.eventHash, 'IS FOR',
    nextChoice.set.map(r=>r._generation));*/

  /*
  If you have an existing precommit, check rejection/decision.
  */
  if(preCommit) {
    // reject precommit if local support does not match
    let reject = !_compareSupportSet(preCommit._supporting, nextChoice.set);
    if(!reject) {
      // if `f+1` nodes support sets larger than existing precommit and their
      // choice has been endorsed, reject
      let total = 0;
      for(const tallyResult of tally) {
        if(tallyResult.set.length > nextChoice.set.length) {
          total += tallyResult.endorsedCount;
        }
      }
      reject = total >= (f + 1);
      // FIXME: remove me
      /*if(reject) {
        console.log(
          'rejecting precommit due to f+1 endorsed support at',
          event._generation);
      }*/
    }

    if(reject) {
      /*console.log('rejecting precommit for',
        preCommit._supporting.map(r=>r._generation));*/
      preCommit = null;
    }
  }

  if(preCommit) {
    /*
    Existing precommit intact, so if a supermajority of endorsed precommits
    support the same set, then consensus has been reached. Otherwise, continue
    on and hope the next event will decide...
    */
    //console.log(
    //  'endorsed precommit count', nextChoice.endorsedPreCommitCount);
    if(nextChoice.endorsedPreCommitCount >= supermajority) {
      // consensus reached
      /*console.log('DECISION DETECTED AT BLOCK', blockHeight, {
        creator: _getCreator(event).substr(-5),
        eventHash: event.eventHash,
        generation: event._generation
      });*/
      /*console.log('SUPPORT WAS FOR',
        nextChoice.set.map(r => r._generation));*/
      return nextChoice.set;
    }
  }

  // compute if the next choice has a supermajority
  const hasSupermajority = nextChoice.count >= supermajority;
  // FIXME: remove me
  /*if(hasSupermajority) {
    console.log('SUPERMAJORITY VOTE DETECTED AT BLOCK', blockHeight,
      nextChoice.set.map(r => ({
        creator: _getCreator(r),
        eventHash: r.eventHash,
        generation: r._generation
      })));
  }*/
  // FIXME: remove above

  /*
  If the next choice has a supermajority and has no existing (unrejected)
  precommit, so create a new one.
  */
  if(hasSupermajority && !preCommit) {
    /*console.log('previous precommit not found, creating new one at',
      event._generation);*/
    // no preCommit yet, use current event
    preCommit = event;
    if(!event._endorser) {
      // compute endorser
      const ancestryMap = _buildAncestryMap(event);
      const endorsePoint = _findEndorsementMergeEvent(
        {ledgerNodeId, x: event, electors, supermajority,
          descendants: {}, ancestryMap: ancestryMap});
      if(endorsePoint) {
        // `endorsePoint` will be an array as byzantine nodes may fork, but
        // since we're searching our own history and we assume we aren't
        // byzantine, we don't have to worry about the array having more than
        // one entry, so just pick the first one
        const endorser = event._endorser = endorsePoint[0].mergeEvent;
        if(endorser._endorses) {
          endorser._endorses.push(event);
        } else {
          endorser._endorses = [event];
        }
        /*console.log(
          'created endorse point for precommit', event._generation,
          'at', event._endorser._generation);*/
      }
    }
  }

  // set event's preCommit
  if(preCommit) {
    event._preCommit = preCommit;
  }

  // propagate node's last switch
  if(!event._lastSwitch) {
    event._lastSwitch = event._treeParent._lastSwitch;
  }

  // support next choice
  event._supporting = nextChoice.set;
  event._votes[creator] = event;
  return null;
}

function _hasEndorsedPreCommit(event) {
  const preCommit = event._preCommit;
  if(preCommit && preCommit._endorsed) {
    return event._generation >= preCommit._endorser._generation;
  }
  return false;
}

function _hasEndorsedSwitch(event) {
  const lastSwitch = event._lastSwitch;
  if(lastSwitch && lastSwitch._endorsed) {
    return event._generation >= lastSwitch._endorser._generation;
  }
  return false;
}

function _findSetInTally(set) {
  if(!set) {
    return () => false;
  }
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

function _findUnionPreCommitSet(event, preCommit) {
  // Note: The algorithm actually guarantees, via containment, that the
  // largest precommit will necessarily be the same as the union of all
  // previous precommits. This is because the earliest precommits are
  // created via support switches that are unions. Any two earliest concurrent
  // precommits have overlap where support must have come from a union.
  //
  // Only union precommits if the current node has made its own precommit,
  // otherwise regular support will be unioned instead (this function will
  // not be called). If the union results in a change of support at the merge
  // event (just locally not supermajority support of the system), then the
  // node's precommit will be rejected and the appropriate new support will
  // be adopted.
  let union = preCommit._supporting;
  _.values(event._votes).forEach(r => {
    // Note: other precommits must also be endorsed to prevent byzantine forks
    if(!(r && _hasEndorsedPreCommit(r))) {
      return;
    }
    const supporting = r._preCommit._supporting;
    if(supporting.length > union.length) {
      union = supporting;
    }
  });
  return union;
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
  let next = target._parents;
  const difference = [...candidateSet].filter(x => !found.has(x));
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
        next.push(...parent._parents);
      }
    }
    next = _.uniq(next);
  }

  return found.size >= min;
}

function _getCreator(event) {
  let creator = _.get(event, 'meta.continuity2017.creator');
  if(!creator) {
    creator = event._children[0].meta.continuity2017.creator;
    event.meta = {continuity2017: {creator}};
  }
  return creator;
}

function _createBaseHashBuffer(hashBuffers) {
  const buf = hashBuffers[0].slice();
  for(let i = 1; i < hashBuffers.length; ++i) {
    _xor(buf, hashBuffers[i]);
  }
  return buf;
}

function _xor(b1, b2) {
  const len = b1.length;
  for(let i = 0; i < len; ++i) {
    b1[i] ^= b2[i];
  }
}

function _parseHash(hash) {
  return Buffer.from(hash);
}
