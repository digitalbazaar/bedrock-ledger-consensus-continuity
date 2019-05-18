/*!
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
/* eslint-disable no-unused-vars */
'use strict';

const _ = require('lodash');
const noopLogger = require('./noopLogger');

// module API
const api = {};
module.exports = api;

// exposed for testing
api._getElectorBranches = _getElectorBranches;
api._findMergeEventProof = _findMergeEventProof;

// TODO: consider adding timing flag that wraps functions in timers and
// then remove commented timing code ... this would help clean up readability

// TODO: do a sweep of this code and break more of it out into functions and
// clean it up to improve readability and make more robust

/**
 * Determine if any new merge events have reached consensus in the given
 * history summary of merge events w/o consensus.
 *
 * @param ledgerNodeId the ID of the local ledger node.
 * @param history recent history rooted at the ledger node's local branch
 *          including ONLY merge events, it must NOT include local regular
 *          events.
 * @param electors the current electors.
 * @param [recoveryElectors] an optional set of recovery electors.
 * @param [recoveryGenerationThreshold] the "no progress" merge event
 *          generation threshold required to enter recovery mode; this
 *          must be given if `recoveryElectors` is given.
 * @param [recoveryDecisionThreshold] the number of recovery electors
 *          that must see a decision before accepting it as final; this
 *          must be given if `recoveryElectors` is given.
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
  recoveryElectors = [], recoveryGenerationThreshold, recoveryDecisionThreshold,
  logger = noopLogger
}) => {
  if(recoveryElectors.length > 0 &&
    !(Number.isInteger(recoveryGenerationThreshold) &&
    Number.isInteger(recoveryDecisionThreshold))) {
    throw new Error(
      '"recoveryGenerationThreshold" and "recoveryDecisionThreshold" must ' +
      'be given when recovery electors are specified.');
  }

  // TODO: Note: once computed, merge event Y+X candidates for each
  //   elector can be cached for quick retrieval in the future without
  //   the need to recompute them (they never change) for a given block...
  //   so the next blockHeight, elector, and X pair (its hash) could be
  //   stored in the continuity2017 meta for each candidate merge event Y
  //logger.verbose('Start sync _getElectorBranches, branches', {electors});
  //let startTime = Date.now();
  const tails = _getElectorBranches({history, electors});
  /*logger.verbose('End sync _getElectorBranches', {
    duration: Date.now() - startTime
  });*/
  //logger.verbose('Start sync _findMergeEventProof');
  //console.log('Start sync _findMergeEventProof');
  //startTime = Date.now();
  const proof = _findMergeEventProof({
    ledgerNodeId, history, tails, blockHeight, electors,
    recoveryElectors, recoveryGenerationThreshold,
    recoveryDecisionThreshold, logger
  });
  /*console.log('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  if(proof.consensus.length === 0) {
    //logger.verbose('findConsensus no proof found, exiting');
    return null;
  }
  //logger.verbose('findConsensus proof found, proceeding...');
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
 * Calculate a supermajority of electors (`2f+1`). When electors <= 3,
 * every elector must agree.
 *
 * @param electorCount the total number of electors.
 *
 * @return the number of electors that constitute a supermajority.
 */
api.supermajority = electorCount => (electorCount <= 3) ?
  electorCount : Math.floor(electorCount / 3) * 2 + 1;

/**
 * Determines the maximum number of failed electors. There are always either
 * `3f+1` electors or 1 elector. When there are `3f+1` electors, there can
 * be `f` failures. When there is 1 elector, the maximum number of failures
 * is zero.
 *
 * @param electorCount the total number of electors.
 *
 * @return the maximum number of electors that can fail (`f`).
 */
api.maximumFailures = electorCount => (electorCount === 1) ?
  0 : ((electorCount - 1) / 3);

/**
 * Converts the given view of history from one particular ledger node's
 * perspective into the views for each of the given electors.
 *
 * @param history recent history.
 * @param electors the current electors.
 *
 * @return a map containing electorId => an array containing the elector's
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
    const tail = electorTails[elector];
    let next = tail;
    while(next.length > 0) {
      const current = next;
      next = [];
      for(const event of current) {
        event._generation = generation;
        next.push(...(event._treeChildren || []));
      }
      generation++;
    }
    tail.forEach(e => e._headGeneration = generation - 1);
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
 * Xs have both endorsed merge events from a supermajority of electors and
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
 * @param history recent history rooted at the ledger node's local branch
 *          including ONLY merge events, it must NOT include local regular
 *          events.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *          by elector ID.
 * @param electors the current array of electors.
 * @param [recoveryElectors] an optional set of recovery electors.
 * @param [recoveryGenerationThreshold] the "no progress" merge event
 *          generation threshold required to enter recovery mode; this
 *          must be given if `recoveryElectors` is given.
 * @param [recoveryDecisionThreshold] the number of recovery electors
 *          that must see a decision before accepting it as final; this
 *          must be given if `recoveryElectors` is given.
 * @param logger the logger to use.
 *
 * @return a map with `consensus` and `yCandidates`; the `consensus` key's
 *         value is an array of merge event X and Y pairs where each merge
 *         event Y and its history proves its paired merge event X has been
 *         endorsed by a super majority of electors -- another key, `proof` is
 *         also included with each pair that includes `y` and its direct
 *         ancestors until `x`, these, in total, constitute endorsements of `x`.
 *
 * @throws `NewElectorsRequiredError` when recovery mode has been triggered
 *         because consensus was not making sufficient progress.
 */
function _findMergeEventProof({
  ledgerNodeId, history, tails, blockHeight, electors, recoveryElectors = [],
  recoveryGenerationThreshold, recoveryDecisionThreshold = 0,
  logger = noopLogger
}) {
  if(electors.length === 1) {
    // if elector count is 1, then no recovery electors permitted
    recoveryElectors = [];
  }

  // determine if recovery mode could possibly be entered
  let canEnterRecoveryMode = false;
  if(recoveryElectors.length > 0) {
    // if enough recovery electors haven't participated yet or if their tail
    // event does not have a high enough generation, then we can't possibly
    // enter recovery mode
    let count = 0;
    for(const elector of recoveryElectors) {
      const electorTails = tails[elector.id];
      if(electorTails && electorTails.some(
        r => r._headGeneration >= recoveryGenerationThreshold)) {
        count++;
      }
    }
    const threshold = recoveryElectors.length - recoveryDecisionThreshold + 1;
    canEnterRecoveryMode = (count >= threshold);
  }

  //let startTime = Date.now();
  //logger.verbose('Start sync _findMergeEventProofCandidates');
  //console.log('Start sync _findMergeEventProofCandidates');
  const candidates = _findMergeEventProofCandidates(
    {ledgerNodeId, tails, blockHeight, electors, recoveryElectors, logger});
  /*console.log('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });*/

  if(!candidates) {
    // no Y candidates yet and, in order for `candidates` to be `null`, there
    // must be no recovery electors specified, so safe to return early with
    // no consensus
    return {consensus: []};
  }

  const {xByElector} = candidates;
  let {yByElector} = candidates;

  // if recovery mode can't be entered, skip tracking it... we will still
  // ensure that decisions can only be made with the appropriate recovery
  // elector thresholds, but we know that there are not enough merge event
  // generations to create sufficient proposals to enter recovery mode
  if(!canEnterRecoveryMode && recoveryElectors.length > 0) {
    for(const {id: elector} of recoveryElectors) {
      const mergeEvents = xByElector[elector];
      if(mergeEvents) {
        mergeEvents.forEach(e => {
          e._recovery.skip = true;
        });
      }
    }
  }

  // determine if a decision can even possibly be made yet...

  // at least `12f+6` total merge events must have passed in order for a
  // decision to be made
  const f = api.maximumFailures(electors.length);
  const minMergeEvents = (electors.length === 1) ? 1 : (12 * f + 6);
  const totalMergeEvents = history.events.length;

  // in recovery mode, enough recovery electors must participate
  let possibleRecoveryDeciders = 0;
  const recoverySet = new Set(recoveryElectors.map(e => e.id));
  for(const elector in yByElector) {
    if(recoverySet.size > 0 && recoverySet.has(elector)) {
      possibleRecoveryDeciders++;
    }
  }

  // can decide if:
  // 1. There are sufficient merge events AND...
  // 2. Voters form a supermajority (2f+1) AND...
  // 2.1. Either not using recovery mode OR...
  // 2.2. There are enough possible recovery deciders to meet the threshold.
  const voters = Object.keys(yByElector);
  const supermajority = api.supermajority(electors.length);
  const canDecide = (
    totalMergeEvents >= minMergeEvents &&
    voters.length >= supermajority &&
    (recoveryElectors.length === 0 ||
      possibleRecoveryDeciders >= recoveryDecisionThreshold));
  if(!canDecide) {
    // only return early if entering recovery mode is also not possible
    if(!canEnterRecoveryMode) {
      return {consensus: []};
    }

    // clear `yByElector` as there is no point in calculating votes ...
    // as it is known that there were insufficient participants to produce
    // a decision ... thus we proceed by only checking for recovery mode
    for(const elector in yByElector) {
      const mergeEvents = yByElector[elector];
      for(const mergeEvent of mergeEvents) {
        delete mergeEvent._x;
      }
    }
    yByElector = {};
  }

  //startTime = Date.now();
  //console.log('Start sync _findConsensusMergeEventProof');
  //logger.verbose('Start sync _findConsensusMergeEventProof');
  const ys = _findConsensusMergeEventProof(
    {ledgerNodeId, xByElector, yByElector, blockHeight,
      electors, recoveryElectors, recoveryGenerationThreshold,
      recoveryDecisionThreshold, logger});
  /*console.log('End sync _findConsensusMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findConsensusMergeEventProof', {
    duration: Date.now() - startTime
  });*/
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
 * where merge events X are the first events for each elector.
 *
 * A merge event Y provides proof that its corresponding merge event X has been
 * approved by a consensus of electors. In other words, for a given Y and X,
 * "merge event Y proves that merge event X has been endorsed by a
 * supermajority of electors".
 *
 * Both Y and X must be created by each elector ("branch-native"). Therefore,
 * each elector will produce a single unique Y and X combination (or none at
 * all), unless that elector is byzantine, which is handled by canceling their
 * vote.
 *
 * @param ledgerNodeId the ID of the local ledger node.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *          by elector ID.
 * @param electors the current set of electors.
 * @param [recoveryElectors] an optional set of recovery electors.
 * @param logger the logger to use.
 *
 * @return `null` or a map containing `yByElectors` and `xByElectors`; in
 *   `xByElectors`, each elector maps to the first event(s) on that elector's
 *   branch. In `yByElectors`, each elector maps to merge event(s) Y on its
 *   branch that prove a merge event X on its branch has been endorsed by a
 *   super majority of electors.
 */
function _findMergeEventProofCandidates({
  ledgerNodeId, tails, blockHeight, electors, recoveryElectors = [],
  logger = noopLogger
}) {
  const supermajority = api.supermajority(electors.length);

  //console.log('TAILS', util.inspect(tails, {depth:10}));

  const electorsWithTails = Object.keys(tails);
  // TODO: ensure logging `electorsWithTails` is not slow
  /*logger.verbose('Continuity2017 electors with tails for ledger node ' +
    ledgerNodeId + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNodeId, electorsWithTails});*/
  /*console.log('Continuity2017 electors with tails for ledger node ' +
    ledgerNodeId + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNodeId, electorsWithTails});*/
  if(electorsWithTails.length < supermajority &&
    recoveryElectors.length === 0) {
    // non-consensus events from a supermajority of electors have not yet
    // been collected and no recovery elector info to compute, so return
    // early
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

  const recoverySet = new Set(recoveryElectors.map(e => e.id));

  // Note: we need to allow multiple events for each elector, in the event that
  // they are byzantine -- we must allow this because we must calculate
  // support on remote merge events from their node's perspective, not our
  // own (i.e. we know a node is byzantine and has forked, but they may not
  // so they will calculate support accordingly)

  // find merge event X candidate for each elector
  //let startTime = Date.now();
  //logger.verbose('Start sync _findMergeEventProofCandidates: Xs');
  for(const elector of electorsWithTails) {
    //console.log('FINDING X for', elector);
    // Note: must not skip electors with multiple tails detected (byzantine),
    // as not every valid node will see the fork and we must calculate their
    // support properly
    const electorTails = tails[elector];

    // `x` is the always the tail (oldest event) for a given elector
    //console.log('***X found for', elector, ' at generation ',
    //  result._generation, result);
    const mergeEvents = electorTails.slice();
    xByElector[elector] = mergeEvents;
    // compute ancestry map for each merge event; it is used to determine
    // ancestry and halt searches for Y and in producing the set of events to
    // include in a block should an X be selected... also init recovery info
    // if elector is a recovery elector
    const isRecoveryElector = recoverySet.has(elector);
    for(const mergeEvent of mergeEvents) {
      mergeEvent._index = {
        ancestryMap: _buildAncestryMap(mergeEvent)
      };
      if(isRecoveryElector) {
        mergeEvent._recovery = {
          noProgressCounter: 0,
          electorsSeen: new Set()
        };
      }
    }
  }
  /*logger.verbose('End sync _findMergeEventProofCandidates: Xs', {
    duration: Date.now() - startTime
  });*/

  // TODO: ensure logging `xByElector` is not slow
  /*logger.verbose('Continuity2017 X merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, xByElector});*/
  /*console.log('Continuity2017 X merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, xByElector});*/

  if(Object.keys(xByElector).length < supermajority &&
    recoveryElectors.length === 0) {
    // non-consensus events X from a supermajority of electors have not yet
    // been collected and no recovery info to compute, so return early
    return null;
  }

  // find merge event Y candidate(s) for each elector
  // Note: at most one per elector unless elector is byzantine (then >= 1)
  //startTime = Date.now();
  //logger.verbose('Start sync _findMergeEventProofCandidates: Y candidates');
  for(const elector in xByElector) {
    for(const x of xByElector[elector]) {
      //console.log('FINDING Y FOR X', x, elector);
      // pass `x._index.ancestryMap` as the ancestry map to short-circuit
      // searches as it includes all ancestors of X -- which should not be
      // searched when finding a Y because they cannot lead to X
      const results = _findEndorsementMergeEvent(
        {ledgerNodeId, x, electors, supermajority,
          ancestryMap: x._index.ancestryMap});
      if(results) {
        const mergeEvents = [];
        for(const result of results) {
          result.mergeEvent._x = x;
          result.mergeEvent._xDescendants = result.descendants;
          mergeEvents.push(result.mergeEvent);
        }
        if(yByElector[elector]) {
          yByElector[elector] = _.uniq(yByElector[elector].concat(mergeEvents));
        } else {
          yByElector[elector] = mergeEvents;
        }
      }
    }
  }
  /*logger.verbose('End sync _findMergeEventProofCandidates: Y candidates', {
    duration: Date.now() - startTime
  });*/

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
    return [{mergeEvent: x, descendants: _copyMapOfArrays(descendants)}];
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
      descendants : _copyMapOfArrays(descendants)
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
        {x, descendants, electorSet, supermajority})) {
        //console.log('supermajority of endorsements found at generation',
        //  treeDescendant._generation);
        //console.log();
        results.push({
          mergeEvent: treeDescendant,
          descendants
        });
        continue;
      }
      //console.log('not enough endorsements yet at generation',
      //  treeDescendant._generation);
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
            descendants
          })));
        } else {
          next.push(...treeDescendant._treeChildren.map(treeDescendant => ({
            treeDescendant,
            descendants: _copyMapOfArrays(descendants)
          })));
        }
      }
    }
  }

  return results.length === 0 ? null : results;
}

function _findConsensusMergeEventProof({
  ledgerNodeId, xByElector, yByElector, blockHeight, electors,
  recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold,
  logger = noopLogger
}) {
  /*logger.verbose(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId});*/
  /*console.log(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId});*/

  const allYs = [].concat(...Object.values(yByElector));

  // if electors is 1, consensus is trivial
  if(electors.length === 1) {
    return allYs;
  }

  //let startTime = Date.now();
  //logger.verbose('Start sync _findConsensusMergeEventProof: _tallyBranches');
  //console.log('Start sync _findConsensusMergeEventProof: _tallyBranches');
  // go through each Y's branch looking for consensus
  const consensus = _tallyBranches({
    ledgerNodeId, xByElector, yByElector, blockHeight,
    electors, recoveryElectors, recoveryGenerationThreshold,
    recoveryDecisionThreshold, logger});
  /*console.log('End sync _findConsensusMergeEventProof: (Branches', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findConsensusMergeEventProof: _tallyBranches', {
    duration: Date.now() - startTime
  });*/

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
function _findDescendantsInPath({
  ledgerNodeId, x, y, descendants = {}, ancestryMap = _buildAncestryMap(x),
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

// shallow copy a map of arrays (arrays will be shallow copied)
function _copyMapOfArrays(mapOfArrays) {
  const copy = {};
  Object.keys(mapOfArrays).forEach(k => copy[k] = mapOfArrays[k].slice());
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

/**
 * This helper function determines the most recent events from each influential
 * elector (i.e. electors that are either voting or are recovery electors) and
 * stores them in `event._index.mostRecentAncestor`.
 */
function _computeMostRecentAncestors({
  ledgerNodeId, event, influentialElectorSet, influentialElectorTails}) {
  // compute `diff` that includes all new events between `event._treeParent`
  // and `event` that descend from influential electors
  const diff = _computeInfluentialElectorPedigree({
    ledgerNodeId, event, influentialElectorSet,
    influentialElectorTails});

  // update most recent ancestor for each elector in `influentialElectorSet`
  let mostRecentAncestors;
  if(!event._treeParent) {
    // no ancestors yet computed, first event on branch
    mostRecentAncestors = {};
  } else {
    // copy parent ancestors
    mostRecentAncestors = {...event._treeParent._index.mostRecentAncestors};
    // add parent as most recent ancestor if creator is influential as it
    // won't show up in the `diff` below
    const creator = _getCreator(event);
    if(influentialElectorSet.has(creator)) {
      mostRecentAncestors[creator] = event._treeParent;
    }
  }
  event._index.mostRecentAncestors = mostRecentAncestors;

  // add most recent ancestors from `diff`
  for(const ancestorEvent of diff) {
    const elector = _getCreator(ancestorEvent);

    // only consider events from influential electors
    if(!influentialElectorSet.has(elector)) {
      continue;
    }

    _useMostRecentAncestorEvent({
      ledgerNodeId, elector, index: mostRecentAncestors, ancestorEvent});
  }
}

/**
 * This helper function progressively computes descendants from the first merge
 * event from each elector that is being tracked to `event` and returns a diff
 * of newly added descendants since `event._treeParent`. These are the
 * ancestors of `event` that descend from "influential electors". This function
 * builds up an index of `electorPedigree` (indexing a subset of the electors
 * that are important to track for consensus, as defined by `electorSet`). That
 * index is used by another function to produce the most recent events from
 * each of those electors for the given `event`.
 */
function _computeInfluentialElectorPedigree({
  ledgerNodeId, event, influentialElectorSet,
  influentialElectorTails}) {
  // init index as needed
  if(!event._index) {
    event._index = {};
  }

  // copy pedigree already computed from parents as these are inherited
  const parentElectorPedigree = (event._treeParent &&
    event._treeParent._index.electorPedigree);
  if(parentElectorPedigree) {
    if(event._treeParent._treeChildren.length === 1) {
      // can reuse and overwrite parent elector pedigree
      event._index.electorPedigree = parentElectorPedigree;
    } else {
      // must copy due to fork
      event._index.electorPedigree = _copyMapOfArrays(parentElectorPedigree);
    }
  } else {
    // initialize elector pedigree
    event._index.electorPedigree = {};
    influentialElectorSet.forEach(e => event._index.electorPedigree[e] = []);
  }

  const diff = [];
  //const startTime = Date.now();
  /*logger.debug(
    'Start sync _findConsensusMergeEventProof: ' +
    '_computeInfluentialElectorPedigree');*/
  for(const elector of influentialElectorSet) {
    /* Note: The first events by an elector should be an array of size 1 unless
    the elector is byzantine (it has forked). However, just because we have
    found the fork does not mean that the event we're working on (`event`)
    creating an index for has seen the fork. We must not indicate that `event`
    has detected a byzantine elector unless two or more tree children are also
    in the ancestry of `event`. It cannot be that we simply know about them
    from some other descendant in the graph. Otherwise, we will detect the
    fork too early and calculate `event`'s elector descendants differently
    from other nodes. In any event, this function does not mark byzantine
    nodes, rather, a subsequent function that produces the most recent
    ancestor index will do this. This is just an informative note regarding
    a potential pitfall. */
    const mergeEvents = influentialElectorTails.get(elector);
    if(!mergeEvents) {
      // no merge events yet for this elector
      continue;
    }
    // should just be one loop through here in the common case, but have to
    // account for byzantine electors that have forked
    for(const ancestorEvent of mergeEvents) {
      // compute diff in descendants (`descendants` will be updated and all
      // the new entries will appear in `diff`)
      const {ancestryMap} = ancestorEvent._index;
      // elector pedigree is all `descendants` of `ancestorEvent` until
      // `event._treeParent` ... and will be updated by this call to include
      // additional descendants until `event`
      const descendants = event._index.electorPedigree[elector];
      _findDescendantsInPath({
        ledgerNodeId,
        x: ancestorEvent,
        y: event,
        descendants,
        ancestryMap,
        diff
      });
      /*console.log('descendants from first ancestor merge event ' +
        ancestorEvent._generation + ' to event',
        event._generation,
        _flattenDescendants({ledgerNodeId, x: ancestorEvent, descendants}).map(
          r=>r._generation));*/
    }
  }
  /*logger.debug(
    'End sync _findConsensusMergeEventProof: ' +
    '_computeInfluentialElectorPedigree', {
      duration: Date.now() - startTime
    });*/

  return _.uniq(diff).filter(e => e !== event);
}

function _useMostRecentAncestorEvent({
  ledgerNodeId, elector, index, ancestorEvent,
  logger = noopLogger
}) {
  // only count most recent ancestor event from a particular elector; if
  // an elector has two events from the same generation or different
  // generations where the younger does not descend from the older, then the
  // elector is byzantine and their tracking info will be invalidated
  if(!(elector in index)) {
    /*logger.verbose('Continuity2017 found new most recent ancestor event', {
      ledgerNode: ledgerNodeId,
      elector,
      ancestorEvent: ancestorEvent.eventHash
    });*/
    index[elector] = ancestorEvent;
    return;
  }

  const existing = index[elector];
  if(existing === false || ancestorEvent === false) {
    logger.warning('Continuity2017 detected byzantine node fork',
      {ledgerNode: ledgerNodeId, elector});
    return;
  }

  // ensure ancestor events of the same generation are the same event
  if(ancestorEvent._generation === existing._generation) {
    if(ancestorEvent !== existing) {
      // byzantine node!
      logger.warning('Continuity2017 detected byzantine node fork', {
        ledgerNode: ledgerNodeId,
        elector,
        ancestorEvent: ancestorEvent.eventHash
      });
      index[elector] = false;
    }
    // ancestor events match, nothing to do
    return;
  }

  // ensure new ancestor event and existing ancestor event do not have forked
  // ancestry by ensuring younger generation descends from older
  let older;
  let younger;
  if(ancestorEvent._generation > existing._generation) {
    older = existing;
    younger = ancestorEvent;
  } else {
    older = ancestorEvent;
    younger = existing;
  }

  if(!_descendsFrom(younger, older)) {
    // byzantine node!
    logger.warning('Continuity2017 detected byzantine node fork', {
      ledgerNode: ledgerNodeId,
      elector,
      ancestorEvent: ancestorEvent.eventHash
    });
    index[elector] = false;
    return;
  }

  /*logger.verbose('Continuity2017 replacing most recent ancestor event', {
    ledgerNode: ledgerNodeId,
    elector,
    ancestorEvent: ancestorEvent.eventHash
  });*/
  index[elector] = younger;
}

function _updateRecoveryInfo({electors, yByElector, event}) {
  if(event._recovery.decision) {
    // no need to compute further, decision already reached, can't move
    // from a decision to a recovery proposal per rules
    return;
  }

  // find newly seen electors since `event._treeParent`
  let newEvents;
  if(event._treeParent) {
    newEvents = [];
    const parentAncestors = event._treeParent._index.mostRecentAncestors;
    for(const elector in parentAncestors) {
      const parentAncestorEvent = parentAncestors[elector];
      const ancestorEvent = event._index.mostRecentAncestors[elector];
      if(parentAncestorEvent !== ancestorEvent) {
        // event is new
        newEvents.push(ancestorEvent);
      }
    }
  } else {
    newEvents = Object.values(event._index.mostRecentAncestors);
  }

  // for new events, only add to "new electors seen" if events are from voting
  // electors (i.e. electors that can make progress) and aren't byzantine
  const newElectorsSeen = new Set();
  for(const newEvent of newEvents) {
    if(!newEvent) {
      // byzantine node detected, `newEvent` does not advance consensus
      continue;
    }
    // need to make sure that `newEvent` comes at or after its creator's Y
    const creator = _getCreator(newEvent);
    const creatorYs = yByElector[creator];
    if(!creatorYs) {
      // `newEvent` does not come from a voting elector
      continue;
    }

    // if a decision has been seen by an ancestor, accept it; this covers the
    // case where a recovery node does not have a Y but has seen a decision
    // from another node -- so it's ok to reach consensus
    if(newEvent._decision) {
      event._decision = event._recovery.decision = newEvent._decision;
    }

    // if `newEvent` descends from creator's Y then it counts as helping
    // advance consensus...

    // common case (non-byzantine creator)
    if(creatorYs.length === 0) {
      if(creatorYs[0]._generation <= newEvent._generation) {
        newElectorsSeen.add(creator);
      }
      continue;
    }

    // creator has forked but `event` has not seen it yet; can only use
    // `newEvent` if it occurs after one of the Ys
    let creatorY = newEvent;
    while(creatorY) {
      if(creatorYs.includes(creatorY)) {
        // `newEvent` occurs after a Y event (so it is a voting event)
        newElectorsSeen.add(creator);
        break;
      }
      creatorY = creatorY._treeParent;
    }
  }

  // calculate threshold that indicates progress
  // ... there are always 3f+1 electors, 2f+1 required for consensus
  // ... >= 2f + 1 means progress towards consensus
  // ... note there must always be f+1 recovery electors, they are always up
  // ... (2f+1 - (f+1 recovery_electors)) = f,
  // Note: So why `>= 2f+1` and not just `2f+1 - (f+1) = f` since the recovery
  // electors are always up? To mitigate case where an attacker is able to
  // prevent a recovery elector from communicating with the others ... recovery
  // mode can still be triggered here and the recovery electors could, once it
  // is triggered, start using a VPC to communicate, thwarting the attacker
  const f = api.maximumFailures(electors.length);
  const threshold = 2 * f + 1;

  // optimize obviously seen enough electors as we saw them all at once
  const {_recovery: recovery} = event;
  if(newElectorsSeen.size >= threshold) {
    recovery.noProgressCounter = 0;
    recovery.electorsSeen.clear();
    return;
  }

  // update recovery info using any newly seen electors
  const originalSize = recovery.electorsSeen.size;
  for(const elector of newElectorsSeen) {
    const beforeSize = recovery.electorsSeen.size;
    recovery.electorsSeen.add(elector);
    // if new elector was seen and threshold crossed...
    if(recovery.electorsSeen.size > beforeSize &&
      recovery.electorsSeen.size === threshold) {
      // progress made, reset counter and return
      recovery.noProgressCounter = 0;
      recovery.electorsSeen.clear();
      return;
    }
  }

  // if no progress towards consensus with this merge event...
  if(originalSize === recovery.electorsSeen.size) {
    // increment no progress counter
    recovery.noProgressCounter++;
  }
}

function _computeMostRecentVotes({event, yElectorSet, yByElector}) {
  const _votes = event._votes = {};
  for(const yElector of yElectorSet) {
    const ancestorEvent = event._index.mostRecentAncestors[yElector];
    if(ancestorEvent === undefined) {
      // no ancestor event for `yElector` to represent a vote
      continue;
    }

    // use `ancestorEvent` if it represents a byzantine node or if it is
    // already known to be a voting event
    if(ancestorEvent === false || ancestorEvent._votes) {
      _votes[yElector] = ancestorEvent;
      continue;
    }

    // if ancestor event comes after its creator's Y use it...
    const creatorYs = yByElector[yElector];

    // common case (no byzantine fork)
    if(creatorYs.length === 1) {
      if(creatorYs[0]._generation <= ancestorEvent._generation) {
        _votes[yElector] = ancestorEvent;
      }
      continue;
    }

    // creator has forked but `event` has not seen it yet; can only use
    // `ancestorEvent` if it occurs after one of the Ys
    let creatorY = ancestorEvent;
    while(creatorY) {
      if(creatorYs.includes(creatorY)) {
        // `ancestorEvent` occurs after a Y event (so it is a voting event)
        _votes[yElector] = ancestorEvent;
        break;
      }
      creatorY = creatorY._treeParent;
    }
  }

  // event is a `y` (by virtue of having an `_x` property);
  // always use itself when voting
  if(event._x) {
    _votes[_getCreator(event)] = event;
  }
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
  {x, descendants, electorSet, supermajority}) {
  // always count `x` as self-endorsed
  const endorsements = new Set([_getCreator(x)]);
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
          if(electorSet.has(creator)) {
            endorsements.add(creator);
            //console.log(
            // 'total', endorsements.size, 'supermajority', supermajority);
            //console.log('electors', electorSet);
            //console.log('endorsements', endorsements);
            if(endorsements.size >= supermajority) {
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
  ledgerNodeId, xByElector, yByElector, blockHeight, electors,
  recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold,
  logger = noopLogger
}) {
  /* Algorithm:

  1. Iterate through each influential elector branch, starting at its tail(s)
     and moving down its tree children. Influential electors include electors
     with Y events and recovery electors.
  2. Compute the most recent influential elector ancestors for the current
     event.
  3. If the event is a voting event, compute a `votes` map of elector => most
     recent event created by that elector, creating the set of events that are
     participating in an experiment to see what Y candidates the various nodes
     are supporting. If a byzantine node is detected, mark the elector's entry
     as `false` and it remains that way until consensus is reached. If some of
     the participants haven't computed their support yet, then defer and move
     to the next iteration of the loop. Eventually all participants will
     support a set and the event's recovery info (if any) and its support can
     then be computed -- or consensus will be reached and the loop will
     return early.
  4. If the event is from a recovery elector, and all of the event's recovery
     ancestors have had their recovery information computed, then compute its
     recovery information; otherwise, defer and move to the next iteration of
     the loop. Eventually all recovery ancestors will have computed their
     recovery information and then the current event's recovery info can be
     computed.
  5. Once a tree child has had any recovery info computed and its support
     computed, move onto the next tree child and continue until no more remain.
  */

  // initialize indexing of elector ancestors that must be tracked in order
  // to determine consensus; these influential electors are either recovery
  // electors or are voting electors (those with a Y event)
  const recoverySet = new Set(recoveryElectors.map(e => e.id));
  const yElectorSet = new Set(Object.keys(yByElector));
  const influentialElectorSet = new Set([...recoverySet, ...yElectorSet]);
  const influentialElectorTails = new Map();
  for(const elector of influentialElectorSet) {
    const firstEvents = xByElector[elector];
    if(firstEvents) {
      influentialElectorTails.set(elector, firstEvents);
    }
  }

  // iterate through events from all influential electors, performing
  // computations and tallying toward consensus
  let next = [].concat(...[...influentialElectorTails.values()]);
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      // compute most recent ancestors for event, if not already computed
      if(!(event._index && event._index.mostRecentAncestors)) {
        _computeMostRecentAncestors({
          ledgerNodeId, event, influentialElectorSet,
          influentialElectorTails});
      }

      // if event is a voting event (i.e. its tree parent was a voting event or
      // it is a `y` by virtue of having a `_x` property), then determine its
      // voting ancestry
      if((event._treeParent && event._treeParent._votes) || event._x) {
        if(!event._votes) {
          _computeMostRecentVotes({event, yElectorSet, yByElector});
        }
        // if some voting ancestors haven't computed support yet, defer
        const votingEvents = Object.values(event._votes);
        if(votingEvents.some(
          e => e && e !== event && e._supporting === undefined)) {
          next.push(event);
          continue;
        }
      }

      // if event is from a recovery elector (by virtue of having a tree parent
      // with `_recovery` info or having it itself), then compute its recovery
      // info
      if((event._treeParent && event._treeParent._recovery) ||
        event._recovery) {
        if(!event._recovery) {
          // shallow copy tree parent's recovery info
          const {electorsSeen} = event._treeParent._recovery;
          event._recovery = {
            ...event._treeParent._recovery,
            electorsSeen: new Set(electorsSeen),
            computed: false
          };
          if(!event._recovery.skip) {
            // update recovery info based on new most recent ancestors
            _updateRecoveryInfo({electors, yByElector, event});
          }
        }

        // if some ancestors haven't computed their recovery info yet, defer
        const recoveryEvents = Object.values(event._index.mostRecentAncestors)
          .filter(e => e && e !== event && recoverySet.has(_getCreator(e)));
        if(recoveryEvents.some(e => !(e._recovery && e._recovery.computed))) {
          next.push(event);
          continue;
        }

        // compute recovery info
        _computeRecovery({
          ledgerNodeId, event, blockHeight,
          recoveryElectors, recoveryGenerationThreshold,
          recoveryDecisionThreshold, logger});
      }

      // if event is voting, compute its support
      if(event._votes) {
        /*const startTime = Date.now();
        logger.debug('Start sync _findConsensusMergeEventProof: ' +
          '_computeSupport');*/
        const result = _computeSupport({
          ledgerNodeId, event, blockHeight, electors, recoveryElectors,
          recoveryDecisionThreshold, logger
        });
        /*logger.debug('End sync _findConsensusMergeEventProof: ' +
          '_computeSupport', {
          duration: Date.now() - startTime
        });*/
        if(result) {
          // consensus reached
          return result;
        }
      }

      // add tree children
      next.push(...(event._treeChildren || []));
    }
  }

  return null;
}

function _computeRecovery({
  ledgerNodeId, event, blockHeight,
  recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold}) {
  if(event._recovery.computed) {
    return;
  }

  if(event._recovery.skip) {
    event._recovery.computed = true;
    return;
  }

  // if no recovery proposal has been made *AND* no decision has been made,
  // check to see if a proposal should be made by testing the no progress
  // counter against the generation threshold
  if(!event._recovery.proposal && !event._recovery.decision &&
    event._recovery.noProgressCounter >= recoveryGenerationThreshold) {
    // recovery mode proposed! ... this elector cannot reject this proposal
    // unless another recovery elector creates a decision; however, recovery
    // mode will not be entered until all recovery electors also create
    // a recovery proposal (making it impossible for any to reject their own
    // recovery proposal)
    event._recovery.proposal = true;
  }

  if(event._recovery.proposal) {
    for(const creator in event._index.mostRecentAncestors) {
      // note: always use a decision from another recovery elector to reject a
      // recovery proposal (note: don't need to bother checking if creator
      // is event's creator because event can't both have a proposal and
      // a decision at the same time)
      const ancestorEvent = event._index.mostRecentAncestors[creator];
      if(ancestorEvent && ancestorEvent._recovery &&
        ancestorEvent._recovery.decision) {
        delete event._recovery.proposal;
        break;
      }
    }
  }

  // Note: Here we must ensure that a concurrent acceptance of recovery mode
  // and a switch from recovery proposal to decision cannot happen. This is
  // done by checking the DAG to ensure that every recovery elector has seen
  // the same forks (if any) and has seen the same number of proposals ...
  // and that the number of proposals is sufficient to enter recovery mode.

  // go through the recovery elector ancestry for this event (including
  // this event) and, from the perspective of each other recovery elector,
  // compute how many proposals it has seen and how many recovery elector
  // it has detected are byzantine (i.e. they have forked) ... then make
  // sure all of the non-byzantine recovery electors are in agreement
  let mustMatch;
  let agreement = true;
  const ancestry = {
    ...event._index.mostRecentAncestors,
    [_getCreator(event)]: event
  };
  for(const creator in ancestry) {
    const ancestorEvent = ancestry[creator];
    if(!ancestorEvent || !ancestorEvent._recovery) {
      // skip, no results to compute for a byzantine creator or a non-recovery
      // elector
      continue;
    }
    const result = {};
    result.byzantines = new Set();
    result.proposals = 0;
    const peerAncestors = {
      ...ancestorEvent._index.mostRecentAncestors,
      [creator]: ancestorEvent
    };
    for(const c in peerAncestors) {
      const ancestorEvent = peerAncestors[c];
      if(!ancestorEvent) {
        result.byzantines.add(c);
      } else if(ancestorEvent._recovery && ancestorEvent._recovery.proposal) {
        result.proposals++;
      }
    }

    // make sure all non-byzantine recovery electors are in agreement (note
    // that any byzantine recovery electors will NOT be in the `results` map
    // per the above algorithm) ... then, if there's agreement, make sure there
    // are sufficient proposals to enter recovery mode below
    if(!mustMatch) {
      mustMatch = result;
    } else if(!(
      mustMatch.proposals === result.proposals &&
      mustMatch.byzantines.size === result.byzantines.size)) {
      // results do not match, break out
      agreement = false;
      break;
    } else {
      // ensure deep set equality
      for(const e of mustMatch.byzantines) {
        if(!result.byzantines.has(e)) {
          agreement = false;
          break;
        }
      }
      // results do not match, break out
      if(!agreement) {
        break;
      }
    }
  }

  if(agreement) {
    // in order to enter recovery mode, 100% of the non-byzantine recovery
    // electors must have recovery proposals AND the non-byzantine recovery
    // electors must form a number that is greater than the decision threshold
    const nonByzantines = recoveryElectors.length - mustMatch.byzantines.size;
    const threshold = recoveryDecisionThreshold + 1;
    if(mustMatch.proposals === nonByzantines && nonByzantines >= threshold) {
      // once all non-byzantine recovery electors have seen that all others
      // have made proposals, they cannot switch to a decision, so we can
      // safely enter recovery mode
      const err = new Error('New electors required.');
      err.name = 'NewElectorsRequiredError';
      throw err;
    }
  }

  // recovery info computed
  event._recovery.computed = true;
}

function _computeSupport({
  ledgerNodeId, event, blockHeight, electors, recoveryElectors,
  recoveryDecisionThreshold, logger = noopLogger
}) {
  // support already chosen for event
  if(event._supporting !== undefined) {
    return;
  }

  /* Choosing support:

  First, if the event's previous self-vote was `false` then the event is
  byzantine and there is nothing to compute, its vote has been revoked.

  If proposal:
    1. If >= f+1 *other* smaller proposals (endorsed or not), support the
       largest of those.
    2. Otherwise, union *endorsed* proposals.
  If no proposal:
    1. If >= f+1 *endorsed* proposals, support the largest.
    2. Otherwise, union support.

  Rejecting proposals/Deciding:
    1. If support is not for the proposal, reject.
    2. Otherwise, if >= f+1 support something other than the proposal and
       each of those sets is a larger set and has been endorsed, reject.
    3. If not rejected, see if >= 2f+1 endorsed proposals for the same thing
       we support, decide. CAVEAT: If `recoveryElectors.length > 0`, then
       a decision can only be made when `recoveryDecisionThreshold` decision
       events, each created by a recovery elector that does not have a pending
       "recovery mode proposal" have been found. If this caveat applies but is
       not met, simply continue on until some future event satisfies the
       requirements or, alternatively, recovery mode is entered.

  Creating a new proposal:

    1. If no existing proposal (or rejected it) and >= 2f+1 nodes support our
       next choice (endorsement not required), proposal it.
  */

  const creator = _getCreator(event);

  // FIXME: can this code path happen? needs tests
  // if event is byzantine, do not bother computing its votes
  if(event._votes[creator] === false) {
    event._supporting = false;
    return null;
  }

  // compute maximum failures `f` and supermajority `2f+1` thresholds
  const f = api.maximumFailures(electors.length);
  const supermajority = api.supermajority(electors.length);

  // TODO: optimize
  /*logger.verbose('Continuity2017 _computeSupport finding votes seen...',
    {ledgerNode: ledgerNodeId, eventHash: event.eventHash});*/
  // tally votes
  /*const startTime = Date.now();
  logger.debug('Start sync _computeSupport vote tally proper');*/
  const tally = [];
  _.values(event._votes).forEach(e => {
    if(e === false || !e._supporting) {
      // do not count byzantine votes or votes without support (initial Ys)
      return;
    }
    const tallyResult = _.find(tally, _findSetInTally(e._supporting));
    const endorsed = _hasEndorsedSwitch(e);
    const proposal = e._proposal;
    const endorsedProposal = _hasEndorsedProposal(e);
    if(tallyResult) {
      // ensure same instance of set is used for faster comparisons
      e._supporting = tallyResult.set;
      tallyResult.count++;
      if(endorsed) {
        tallyResult.endorsedCount++;
      }
      if(proposal) {
        tallyResult.proposalCount++;
      }
      if(endorsedProposal) {
        tallyResult.endorsedProposalCount++;
      }
    } else {
      tally.push({
        set: e._supporting,
        count: 1,
        endorsedCount: endorsed ? 1 : 0,
        proposalCount: proposal ? 1 : 0,
        endorsedProposalCount: endorsedProposal ? 1 : 0
      });
    }
  });
  /*logger.debug('End sync _computeSupport vote tally proper', {
    duration: Date.now() - startTime
  });*/
  /*console.log('tally', tally.map(r => ({
    set: r.set.map(r=>r._generation),
    count: r.count,
    pc: r.proposalCount
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

  // get existing proposal on the event's branch
  let proposal;
  if(event._treeParent) {
    proposal = event._treeParent._proposal;
  }

  // sort tally results by proposal count (if there is an existing proposal)
  // or by *endorsed* proposal count (if no existing proposal), breaking
  // sort ties using set size
  const countType = proposal ? 'proposalCount' : 'endorsedProposalCount';
  tally.sort((a, b) => {
    const diff = b[countType] - a[countType];
    if(diff !== 0) {
      return diff;
    }
    return b.set.length - a.set.length;
  });

  /* Choose support when there's an existing proposal:
    1. If >= f+1 *other* smaller proposals (endorsed or not), support the
       largest of those.
    2. Otherwise, union *endorsed* proposals.
  */
  if(proposal) {
    // determine if there are f+1 smaller proposals *other* than `proposal`
    const total = tally
      .filter(tallyResult =>
        tallyResult.proposalCount > 0 &&
        tallyResult.set.length < proposal._supporting.length)
      .reduce((aggregator, tallyResult) => {
        return aggregator + tallyResult.proposalCount;
      }, 0);
    if(total >= (f + 1)) {
      nextChoice = tally[0];
    }

    if(!nextChoice) {
      // TODO: optimize to find largest endorsed proposal faster
      const union = _findUnionProposalSet(event, proposal);
      nextChoice = _.find(tally, _findSetInTally(union));
    }
  } else {
    /* Choose support when there is no existing proposal:
      1. If >= f+1 *endorsed* proposals, support the largest.
      2. Otherwise, union support.
    */
    // determine if there are f+1 endorsed proposals
    const total = tally.reduce((aggregator, tallyResult) => {
      return aggregator + tallyResult.endorsedProposalCount;
    }, 0);
    if(total >= (f + 1)) {
      nextChoice = tally[0];
    }

    if(!nextChoice) {
      // compute the union of all support
      const union = _.uniq([].concat(
        ..._.values(event._votes)
          .filter(r => r)
          // either is supporting something or is initial Y which supports
          // only itself
          .map(r => r._supporting || r)));

      // set the next choice to the matching tally or create it
      nextChoice = _.find(tally, _findSetInTally(union));
      if(!nextChoice) {
        // create new choice
        nextChoice = {
          set: union,
          count: 0,
          endorsed: 0,
          proposalCount: 0,
          endorsedProposalCount: 0
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
    const endorsement = _findEndorsementMergeEvent(
      {ledgerNodeId, x: event, electors, supermajority,
        descendants: {}, ancestryMap});
    if(endorsement) {
      // `endorsement` will be an array as byzantine nodes may fork, but
      // if that's the case it doesn't matter which one we pick, the endorse
      // point will not be used as the event will be detected as byzantine
      // prior to its use, so just pick the first one
      const endorser = event._endorser = endorsement[0].mergeEvent;
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
    if(proposal && !proposal._endorsed &&
      event._endorses.includes(proposal)) {
      nextChoice.endorsedProposalCount++;
    }
    event._endorses.forEach(e => e._endorsed = true);
  }

  /*console.log('SUPPORT at ', event.eventHash, 'IS FOR',
    nextChoice.set.map(r=>r._generation));*/

  /*
  If you have an existing proposal, check rejection/decision.
  */
  if(proposal) {
    // reject proposal if local support does not match
    let reject = !_compareSupportSet(proposal._supporting, nextChoice.set);
    if(!reject) {
      // if `f+1` nodes support sets larger than existing proposal and their
      // choice has been endorsed, reject
      let total = 0;
      for(const tallyResult of tally) {
        if(tallyResult.set.length > nextChoice.set.length) {
          total += tallyResult.endorsedCount;
        }
      }
      reject = total >= (f + 1);
    }

    if(reject) {
      proposal = null;
    }
  }

  /*
  If existing proposal intact and a supermajority of endorsed proposals
  support the same set, then consensus has been reached. Otherwise, continue
  on and hope the next event will decide...
  */
  if(proposal && nextChoice.endorsedProposalCount >= supermajority) {
    // TODO: can we optimize such that once `_decision` is set ... skip all
    // the tallying and just check `_consensusReached` at the top of the
    // function? ...what else needs to be propagated for this to work?
    const consensusReached = _consensusReached({
      event, recoveryElectors, recoveryDecisionThreshold, nextChoice});
    if(consensusReached) {
      // consensus reached
      return nextChoice.set;
    }
  }

  // compute if the next choice has a supermajority
  const hasSupermajority = nextChoice.count >= supermajority;
  // If the next choice has a supermajority and has no existing (unrejected)
  // proposal, so create a new one.
  if(hasSupermajority && !proposal) {
    // no proposal yet, use current event
    proposal = event;
    if(!event._endorser) {
      // compute endorser
      const ancestryMap = _buildAncestryMap(event);
      const endorsement = _findEndorsementMergeEvent(
        {ledgerNodeId, x: event, electors, supermajority,
          descendants: {}, ancestryMap});
      if(endorsement) {
        // `endorsement` will be an array as byzantine nodes may fork, but
        // since we're searching our own history and we assume we aren't
        // byzantine, we don't have to worry about the array having more than
        // one entry, so just pick the first one
        const endorser = event._endorser = endorsement[0].mergeEvent;
        if(endorser._endorses) {
          endorser._endorses.push(event);
        } else {
          endorser._endorses = [event];
        }
      }
    }
  }

  // set event's proposal
  if(proposal) {
    event._proposal = proposal;
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

function _hasEndorsedProposal(event) {
  const proposal = event._proposal;
  if(proposal && proposal._endorsed) {
    return event._generation >= proposal._endorser._generation;
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

function _findUnionProposalSet(event, proposal) {
  // Note: The algorithm actually guarantees, via containment, that the
  // largest proposal will necessarily be the same as the union of all
  // previous proposals. This is because the earliest proposals are
  // created via support switches that are unions. Any two earliest concurrent
  // proposals have overlap where support must have come from a union.
  //
  // Only union proposals if the current node has made its own proposal,
  // otherwise regular support will be unioned instead (this function will
  // not be called). If the union results in a change of support at the merge
  // event (just locally not supermajority support of the system), then the
  // node's proposal will be rejected and the appropriate new support will
  // be adopted.
  let union = proposal._supporting;
  _.values(event._votes).forEach(r => {
    // Note: other proposals must also be endorsed to prevent byzantine forks
    if(!(r && _hasEndorsedProposal(r))) {
      return;
    }
    const supporting = r._proposal._supporting;
    if(supporting.length > union.length) {
      union = supporting;
    }
  });
  return union;
}

function _consensusReached({
  event, recoveryElectors, recoveryDecisionThreshold, nextChoice}) {
  // can only decide if no recovery electors are specified or if they
  // are and we've detected decisions on enough recovery electors that
  // do not have recovery proposals
  const creator = _getCreator(event);
  if(recoveryElectors.length === 0) {
    // no recovery electors so decision is final; consensus reached
    return true;
  }

  if(!event._recovery) {
    // `event` not from a recovery elector, record decision as seen
    // but not finalized yet
    event._decision = nextChoice.set;
  } else {
    // `event` is from a recovery elector, so mark decision as seen
    // provided that it does not have a recovery proposal (note: per the
    // rules, we can't reject our own recovery proposal and any recovery
    // proposal we have would have been rejected before this code path
    // if the decision was from another recovery elector... so safe to
    // mark decision here)
    if(!event._recovery.proposal) {
      // decision found, but not finalized yet
      event._decision = event._recovery.decision = nextChoice.set;
    }
  }

  // if `event` ancestry shows enough events from recovery electors have
  // decisions to meet the decision threshold, then finalize decision
  // Note: If `event` is from a recovery elector that *just now* saw
  // a decision we need to make sure we count it because it won't be
  // in the ancestry otherwise (rather, only its parent will)
  const ancestry = {
    ...event._index.mostRecentAncestors,
    [creator]: event
  };
  let decisionCount = 0;
  for(const {id: elector} of recoveryElectors) {
    const ancestorEvent = ancestry[elector];
    if(ancestorEvent && ancestorEvent._recovery.decision) {
      decisionCount++;
      if(decisionCount >= recoveryDecisionThreshold) {
        // decision threshold crossed; consensus reached
        return true;
      }
    }
  }

  // consensus not yet reached
  return false;
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
        if(event._parents.length > 0) {
          next.push(...event._parents);
        }
      }
    }
    next = _.uniq(next);
  }
  return map;
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
  return event.meta.continuity2017.creator;
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
