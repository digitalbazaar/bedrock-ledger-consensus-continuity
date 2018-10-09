/*!
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');

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
  logger = {debug: () => {}, verbose: () => {}}
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
  logger = {debug: () => {}, verbose: () => {}}
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

  const {rByElector} = candidates;
  let {yByElector} = candidates;

  // if recovery mode can't be entered, skip tracking it... we will still
  // ensure that decisions can only be made with the appropriate recovery
  // elector thresholds, but we know that there are not enough merge event
  // generations to create sufficient proposals to enter recovery mode
  if(!canEnterRecoveryMode && recoveryElectors.length > 0) {
    for(const elector in rByElector) {
      rByElector[elector].forEach(e => {
        e._recovery.skip = true;
      });
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
    yByElector = {};
  }

  //startTime = Date.now();
  //console.log('Start sync _findConsensusMergeEventProof');
  //logger.verbose('Start sync _findConsensusMergeEventProof');
  const ys = _findConsensusMergeEventProof(
    {ledgerNodeId, rByElector, yByElector, blockHeight,
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
 * @return `null` or a map containing `yByElectors`, `xByElectors`, and
 *           rByElectors; in `yByElectors`, each elector maps to merge event Y
 *           that proves a merge event X has been endorsed by a super majority
 *           of electors, where X and Y are branch-native; in `rByElectors`
 *           each recovery elector maps to the next event for which recovery
 *           information needs to be computed.
 */
function _findMergeEventProofCandidates({
  ledgerNodeId, tails, blockHeight, electors, recoveryElectors = [],
  logger = {debug: () => {}, verbose: () => {}}
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
  const rByElector = {};

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
    // include `result` in initial descendants map, it is used to halt
    // searches for Y and in producing the set of events to include in a
    // block should an X be selected
    for(const mergeEvent of mergeEvents) {
      mergeEvent._initDescendants = {[mergeEvent.eventHash]: []};
    }
    // init recovery info for a recovery elector
    if(recoverySet.has(elector)) {
      rByElector[elector] = mergeEvents;
      for(const event of mergeEvents) {
        event._recovery = {
          noProgressCounter: 0,
          electorsSeen: new Set(),
          tracking: {}
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

  return {yByElector, xByElector, rByElector};
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
            descendants: _copyDescendants(descendants)
          })));
        }
      }
    }
  }

  return results.length === 0 ? null : results;
}

function _findConsensusMergeEventProof({
  ledgerNodeId, rByElector, yByElector, blockHeight, electors,
  recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold,
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

  //let startTime = Date.now();
  //logger.verbose('Start sync _findConsensusMergeEventProof: _tallyBranches');
  //console.log('Start sync _findConsensusMergeEventProof: _tallyBranches');
  // go through each Y's branch looking for consensus
  const consensus = _tallyBranches({
    ledgerNodeId, rByElector, yByElector, blockHeight,
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

// computes descendants from R => event for recovery purposes and returns a
// diff of newly added descendants since event's tree parent
function _computeRDescendants(
  {ledgerNodeId, event, recoverySet, rByElector}) {
  const parentRDescendants = event._treeParent._recovery.rDescendants;
  if(parentRDescendants) {
    if(event._treeParent._treeChildren.length === 1) {
      // can reuse and overwrite parent R descendants
      event._recovery.rDescendants = parentRDescendants;
    } else {
      // must copy due to fork
      event._recovery.rDescendants = _copyDescendants(parentRDescendants);
    }
  } else {
    // initialize R descendants
    event._recovery.rDescendants = {};
    recoverySet.forEach(e => event._recovery.rDescendants[e] = []);
  }

  const diff = [];
  //const startTime = Date.now();
  /*logger.debug(
    'Start sync _findConsensusMergeEventProof: find R descendants');*/
  for(const elector of recoverySet) {
    // Note: if `event` has any recovery tracking event that has a tree
    // ancestor that has two or more tree children then the creator of that
    // recovery tracking event is byzantine; however, we must not indicate
    // that we've detected that it is byzantine unless those two or more
    // tree children are also in the ancestry of `event`, i.e. it
    // cannot be that we simply know about them from some other
    // descendant in the graph -- otherwise we will detect too early
    // calculate `event`'s recovery tracking info differently from other nodes)
    const mergeEvents = rByElector[elector];
    if(!mergeEvents) {
      // no merge events yet for this elector
      continue;
    }
    for(const r of mergeEvents) {
      // compute diff in descendants
      const descendants = event._recovery.rDescendants[elector];
      _findDescendantsInPath({
        ledgerNodeId,
        x: r,
        y: event,
        descendants,
        ancestryMap: r._initDescendants,
        diff
      });
      /*console.log('descendants from r ' + r._generation + ' to event',
        event._generation,
        _flattenDescendants({ledgerNodeId, x: r, descendants}).map(
          r=>r._generation));*/
    }
  }
  /*logger.debug(
    'End sync _findConsensusMergeEventProof: find R descendants', {
      duration: Date.now() - startTime
    });*/

  return _.uniq(diff).filter(e => e !== event);
}

// computes descendants from Y => event and returns a diff of newly added
// descendants since event's tree parent
function _computeYDescendants({ledgerNodeId, event, yElectorSet, yByElector}) {
  const parentYDescendants = event._treeParent._yDescendants;
  if(parentYDescendants) {
    if(event._treeParent._treeChildren.length === 1) {
      // safe to reuse and overwrite parent Y descendants
      event._yDescendants = parentYDescendants;
    } else {
      // must copy due to fork
      event._yDescendants = _copyDescendants(parentYDescendants);
    }
  } else {
    // initialize Y descendants
    event._yDescendants = {};
    yElectorSet.forEach(e => event._yDescendants[e] = []);
  }

  const diff = [];
  //const startTime = Date.now();
  /*logger.debug(
    'Start sync _findConsensusMergeEventProof: find Y descendants');*/
  for(const elector of yElectorSet) {
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
        _flattenDescendants({ledgerNodeId, x: y, descendants}).map(
          r=>r._generation));*/
    }
  }
  /*logger.debug(
    'End sync _findConsensusMergeEventProof: find Y descendants', {
      duration: Date.now() - startTime
    });*/

  return _.uniq(diff).filter(e => e !== event);
}

// `descendants` is a diff representing descendants since last tree parent
function _updateRecoveryInfo({
  ledgerNodeId, electors, descendants, recoverySet, event,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  // update `tracking` recovery events to use the most recent merge events
  // from other recovery electors and keep track of newly seen electors
  const {_recovery: recovery} = event;
  const {tracking} = recovery;
  const newElectorsSeen = new Set();
  const electorSet = new Set(electors.map(e => e.id));
  for(const trackingEvent of descendants) {
    const elector = _getCreator(trackingEvent);
    // only count electors
    if(!electorSet.has(elector)) {
      continue;
    }
    // only count an elector as newly seen if it is not byzantine
    if(!(trackingEvent._votes && trackingEvent._votes[elector] === false)) {
      // NOT detected as a byzantine elector, so count as newly seen
      newElectorsSeen.add(elector);
    }
    if(recoverySet.has(elector)) {
      _useMostRecentRecoveryTrackingEvent(
        {ledgerNodeId, elector, tracking, trackingEvent, logger});
    }
  }

  if(recovery.proposal) {
    // no need to compute further, recovery proposal already generated
    return;
  }

  // calculate threshold (2f) that indicates progress
  // ... there are always 3f+1 electors, 2f+1 required for consensus
  // ... there must always be f+1 recovery electors, they are always up
  // ... (3f+1 - (f+1 recovery_electors)) = 2f,
  // ... >= 2f means progress towards consensus
  // Note: Why `>= 2f` and not `2f+1 - (f+1) = f` since the recovery electors
  // are always up? To mitigate case where an attacker is able to prevent a
  // recovery elector from communicating with the others ... recovery mode can
  // still be triggered here and the recovery electors, could at that point,
  // for example, use a VPC to communicate, thwarting the attacker
  const f = api.maximumFailures(electors.length);
  const threshold = f * 2;

  // optimize obviously seen enough electors as we saw them all at once
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

// `descendants` is a diff representing descendants since last tree parent
function _updateToMostRecentVotes({
  ledgerNodeId, yByElector, descendants, yElectorSet, votes,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  const ySetByElector = {};
  for(const elector in yByElector) {
    ySetByElector[elector] = new Set(yByElector[elector]);
  }
  for(const event of descendants) {
    const creator = _getCreator(event);
    if(yElectorSet.has(creator)) {
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

function _useMostRecentRecoveryTrackingEvent({
  ledgerNodeId, elector, tracking, trackingEvent,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  // TODO: RECOVERY_MODE: could potentially combine code as it performs
  // the same function as `_useMostRecentVotingEvent` just uses different
  // state

  // only count most recent tracking event from a particular elector; if
  // an elector has two tracking events from the same generation or different
  // generations where the younger does not descend from the older, then the
  // elector is byzantine and their tracking info will be invalidated (which
  // is not a condition that is covered by the current algorithm ... and
  // undefined behavior will result)
  if(!(elector in tracking)) {
    /*logger.verbose('Continuity2017 found new recovery tracking event', {
      ledgerNode: ledgerNodeId,
      elector,
      trackingEvent: trackingEvent.eventHash
    });*/
    tracking[elector] = trackingEvent;
    return;
  }

  const existing = tracking[elector];
  if(existing === false || trackingEvent === false) {
    logger.error('Continuity2017 detected byzantine recovery elector node ',
      {ledgerNode: ledgerNodeId, elector});
    return;
  }

  // ensure tracking events of the same generation are the same event
  if(trackingEvent._generation === existing._generation) {
    if(trackingEvent !== existing) {
      // byzantine node!
      logger.error('Continuity2017 detected byzantine recovery elector node', {
        ledgerNode: ledgerNodeId,
        elector,
        trackingEvent: trackingEvent.eventHash
      });
      tracking[elector] = false;
    }
    // tracking events match, nothing to do
    return;
  }

  // ensure new tracking event and existing tracking event do not have forked
  // ancestry by ensuring younger generation descends from older
  let older;
  let younger;
  if(trackingEvent._generation > existing._generation) {
    older = existing;
    younger = trackingEvent;
  } else {
    older = trackingEvent;
    younger = existing;
  }

  if(!_descendsFrom(younger, older)) {
    // byzantine node!
    logger.error('Continuity2017 detected byzantine recovery elector node', {
      ledgerNode: ledgerNodeId,
      elector,
      trackingEvent: trackingEvent.eventHash
    });
    tracking[elector] = false;
    return;
  }

  /*logger.verbose('Continuity2017 replacing tracking event', {
    ledgerNode: ledgerNodeId,
    elector,
    trackingEvent: trackingEvent.eventHash
  });*/
  tracking[elector] = younger;
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
  ledgerNodeId, rByElector, yByElector, blockHeight, electors,
  recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  /* Algorithm:

  1. Iterate through each Y branch, starting at Y and moving down its
     tree children. CAVEAT: If `recoveryElectors` are specified, we must
     also iterate through each of their branches, whether or not they have
     a Y, computing recovery information.
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
  const recoverySet = new Set(recoveryElectors.map(e => e.id));
  const yElectorSet = new Set(Object.keys(yByElector));
  let next = _.uniq(
    [].concat(..._.values(yByElector), ..._.values(rByElector)));
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(event._supporting || event._byzantine ||
        (event._recovery && event._recovery.computed)) {
        // support and/or recovery info already determined
        continue;
      }

      // if event is a voting event (i.e. its tree parent was a voting event or
      // it is a `y` (by virtue of having an initialize `_votes` property),
      // then determine voting ancestry
      if((event._treeParent && event._treeParent._votes) || event._votes) {
        if(!event._votes) {
          // reuse and update tree parent's votes
          // FIXME: do we need to copy or can we reuse?
          // TODO: use Map instead of object for _votes?
          event._votes = Object.assign({}, event._treeParent._votes);
        }

        // determine ancestors that will partipicate in the experiment,
        // looking at descendants of every Y
        if(!event._yDescendants) {
          const diff = _computeYDescendants(
            {ledgerNodeId, event, yElectorSet, yByElector});

          //const startTime = Date.now();
          //logger.debug('Start sync _updateToMostRecentVotes');
          _updateToMostRecentVotes(
            {ledgerNodeId, yByElector, descendants: diff, yElectorSet,
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
      }

      // if event is from a recovery elector (by virtue of having a tree parent
      // with `_recovery` info or having it itself), then compute recovery
      // info
      if((event._treeParent && event._treeParent._recovery) ||
        event._recovery) {
        if(!event._recovery) {
          // shallow copy tree parent's recovery info
          // FIXME: do we need to copy or can we reuse?
          const {noProgressCounter, electorsSeen, decision, proposal, skip} =
            event._treeParent._recovery;
          event._recovery = {
            noProgressCounter,
            electorsSeen: new Set(electorsSeen),
            decision,
            proposal,
            skip
          };
          // TODO: use Map instead of object for _tracking?
          event._recovery.tracking = Object.assign(
            {}, event._treeParent._recovery.tracking);
          if(!event._recovery.skip) {
            // TODO: RECOVERY_MODE: we could potentially optimize in some cases
            // where there is overlap with `_yDescendants`...
            const diff = _computeRDescendants(
              {ledgerNodeId, event, recoverySet, rByElector});
            // update recovery info based on new descendants in path
            _updateRecoveryInfo({
              ledgerNodeId, electors, descendants: diff, recoverySet,
              event, logger});
          }
        }

        const recoveryEvents = _.values(event._recovery.tracking);
        if(recoveryEvents.some(
          e => e && e !== event &&
          !('_recovery' in e && e._recovery.computed))) {
          // some ancestry recovery info still hasn't been computed, cannot
          // tally yet
          next.push(event);
          continue;
        }
      } else if(recoverySet.has(_getCreator(event))) {
        // event must have been pushed onto `next` to compute its votes,
        // but its parent's recovery info is not computed yet, so push it
        // again
        next.push(event);
        continue;
      }

      /*const startTime = Date.now();
      logger.debug('Start sync _findConsensusMergeEventProof: _tally');*/
      const result = _tally({
        ledgerNodeId, event, blockHeight, electors,
        recoveryElectors, recoveryGenerationThreshold,
        recoveryDecisionThreshold, logger
      });
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
  recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  // if event is tracking recovery info, compute it
  if(event._recovery) {
    _computeRecovery({
      ledgerNodeId, event, blockHeight,
      recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold,
      logger
    });
  }

  // if event is not voting, return no consensus found
  if(!event._votes) {
    return null;
  }

  // event is voting, compute its support
  return _computeSupport({
    ledgerNodeId, event, blockHeight, electors, recoveryElectors,
    recoveryDecisionThreshold, logger
  });
}

function _computeRecovery({
  ledgerNodeId, event, blockHeight,
  recoveryElectors, recoveryGenerationThreshold, recoveryDecisionThreshold,
  logger = {debug: () => {}, verbose: () => {}}
}) {
  if(event._recovery.skip) {
    event._recovery.computed = true;
    return;
  }

  // if no recovery proposal has been made, check to see if one should be
  // made by testing the no progress counter against the generation
  // threshold
  if(!event._recovery.proposal &&
    event._recovery.noProgressCounter >= recoveryGenerationThreshold) {
    // recovery mode proposed! ... this elector cannot ever reject this
    // proposal, though, the proposal will not be activated until a sufficient
    // number of recovery electors also propose entering recovery mode -- and
    // detect that condition
    event._recovery.proposal = true;
  }

  // update own tracking information
  event._recovery.tracking[_getCreator(event)] = event;

  // check to see if the recovery tracking ancestry for this event
  // indicates that all other recovery electors have proposed entering
  // recovery mode
  let proposals = 0;
  const trackingEvents = _.values(event._recovery.tracking);
  for(const trackingEvent of trackingEvents) {
    const creator = _getCreator(trackingEvent);
    // only count non-forked recovery electors with proposals
    if(trackingEvent._recovery.proposal &&
      !(trackingEvent._votes && trackingEvent._votes[creator] === false)) {
      proposals++;
    }
  }

  const requiredProposals =
    recoveryElectors.length - recoveryDecisionThreshold + 1;
  if(proposals >= requiredProposals) {
    // since no decision can be made by a particular recovery once that elector
    // has passed the recovery generation threshold and because a certain
    // threshold of recovery electors must have found potential decisions
    // prior to deciding, here we can safely enter recovery mode
    const err = new Error('New electors required.');
    err.name = 'NewElectorsRequiredError';
    throw err;
  }

  // recovery info computed
  event._recovery.computed = true;
}

function _computeSupport({
  ledgerNodeId, event, blockHeight, electors, recoveryElectors,
  recoveryDecisionThreshold, logger = {debug: () => {}, verbose: () => {}}
}) {
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

  // if event is byzantine, do not bother computing its votes
  const creator = _getCreator(event);
  if(event._votes[creator] === false) {
    event._byzantine = true;
    event._supporting = false;
    return null;
  }

  // compute maximum failures `f` and supermajority `2f+1` thresholds
  const f = api.maximumFailures(electors.length);
  const supermajority = api.supermajority(electors.length);

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
  /*logger.debug('End sync _tally vote tally proper', {
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

  if(proposal) {
    /*
    Existing proposal intact, so if a supermajority of endorsed proposals
    support the same set, then consensus has been reached. Otherwise, continue
    on and hope the next event will decide...
    */
    if(nextChoice.endorsedProposalCount >= supermajority) {
      // can only decide if no recovery electors are specified or if they
      // are and we've detected decisions on enough recovery electors that
      // do not have recovery proposals
      let consensusReached = false;
      if(recoveryElectors.length === 0) {
        consensusReached = true;
      } else {
        // must check to see if enough recovery electors have seen a decision
        let count = 0;

        // this event is from a recovery elector, so mark decision as seen
        if(event._recovery && !event._recovery.proposal) {
          // decision found, but not finalized yet
          event._recovery.decision = true;
          count++;
        }

        // if enough recovery tracking events meet the decision threshold,
        // then finalize decision
        const recoverySet = new Set(recoveryElectors.map(e => e.id));
        for(const elector in event._votes) {
          // skip self
          if(elector === creator) {
            continue;
          }
          // count non-byzantine recovery elector with decision and no proposal
          const votingEvent = event._votes[elector];
          if(votingEvent && recoverySet.has(elector)) {
            const {decision, proposal} = votingEvent._recovery;
            if(decision && !proposal) {
              count++;
            }
          }
        }
        consensusReached = (count >= recoveryDecisionThreshold);
      }

      if(consensusReached) {
        // consensus reached
        return nextChoice.set;
      }
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
