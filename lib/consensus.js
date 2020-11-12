/*!
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
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
 * @param [recoveryElectors=[]] an optional set of recovery electors.
 * @param [recoveryGenerationThreshold] the "no progress" merge event
 *          generation threshold required to enter recovery mode; this
 *          must be given if `recoveryElectors` is given.
 * @param [mode='first'] an optional mode for selecting which events
 *          to find consensus on:
 *          first: create support sets from the first (aka "tail" or oldest)
 *            event from each participating elector and return the events that
 *            are in the consensus support set as having reached consensus
 *            (i.e., `eventHashes`) (mode is aka "Ys are Xs").
 *          firstWithConsensusProof: create support sets from the first
 *            endorsement event for the first (aka "tail" or oldest) event from
 *            each participating elector and return the consensus support set
 *            and all ancestors until the associated tail events as
 *            `consensusProofHashes` and the tails associated with the
 *            endorsement events as having reached consensus (i.e.,
 *            `eventHashes`) (mode is aka "Y is the endorsement of X").
 *          batch: the same as `firstWithConsensusProof` except all
 *            `consensusProofHashes` are instead also returned as having
 *            reached consensus in `eventHashes`.
 * @param logger the logger to use.
 *
 * @return a result object with the following properties:
 *           consensus: `true` if consensus has been found, `false` if` not.
 *           eventHashes: the hashes of events that have reached consensus.
 *           consensusProofHashes: the hashes of events endorsing `eventHashes`.
 *           priorityPeers: if consensus is `false`, an array of peer (voter)
 *             IDs identifying the peers that may help achieve consensus most
 *             readily.
 *           creators: the electors that participated in events that reached
 *             consensus.
 */
api.findConsensus = ({
  ledgerNodeId, history, blockHeight, electors,
  recoveryElectors = [], recoveryGenerationThreshold,
  mode = 'first', logger = noopLogger
}) => {
  if(recoveryElectors.length > 0 &&
    !(Number.isInteger(recoveryGenerationThreshold) &&
    recoveryGenerationThreshold >= 5)) {
    throw new Error(
      `"recoveryGenerationThreshold" (${recoveryGenerationThreshold}) ` +
      'must be an integer >= 5 when recovery electors are specified. ');
  }

  // TODO: Note: once computed, merge event Y+X candidates for each
  //   elector can be cached for quick retrieval in the future without
  //   the need to recompute them (they never change) for a given block...
  //   so the next blockHeight, elector, and X pair (its hash) could be
  //   stored in the continuity2017 meta for each candidate merge event Y;
  //   however, this would only apply to certain modes where both X and Y
  //   are needed vs. modes where Ys are Xs
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
    recoveryElectors, recoveryGenerationThreshold, mode, logger
  });
  /*console.log('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  if(proof.consensus.length === 0) {
    const priorityPeers = _getPriorityPeers({
      ledgerNodeId, history, tails, blockHeight, electors,
      recoveryElectors, recoveryGenerationThreshold,
      mode, logger
    });

    //logger.verbose('findConsensus no proof found, exiting');
    return {
      consensus: false,
      priorityPeers
    };
  }
  //logger.verbose('findConsensus proof found, proceeding...');

  // gather events that have achieved consensus
  let events;
  if(mode === 'batch') {
    // consider `y` events to have achieved consensus, they are not only
    // "consensus proof"
    events = proof.consensus.map(p => p.y);
  } else {
    // only `x` events have achieved consensus
    events = proof.consensus.map(p => p.x);
  }

  // gather events used as proof of the consensus support set
  const creators = new Set();
  let consensusProofHashes;
  if(mode === 'firstWithConsensusProof') {
    // compute full consensus proof from `x` to `y`
    consensusProofHashes = [...new Set(
      proof.consensus.reduce((aggregator, current) => {
        creators.add(_getCreator(current.x));
        current.proof.forEach(r => {
          creators.add(_getCreator(r));
          aggregator.push(r.eventHash);
        });
        return aggregator;
      }, []))];
  } else {
    // consensus proof hashes in this case are only the consensus set,
    // everything else (including these hashes) have achieved consensus
    consensusProofHashes = events.map(e => {
      creators.add(_getCreator(e));
      return e.eventHash;
    });
  }

  const eventHashes = _getAncestorHashes({ledgerNodeId, events});
  // return event and consensus proof hashes and creators
  return {
    consensus: true,
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
    let head = null;
    while(next.length > 0) {
      const current = next;
      next = [];
      for(const event of current) {
        event._generation = generation;
        next.push(...(event._treeChildren || []));
      }
      generation++;
      if(next.length === 0) {
        head = current;
      }
    }
    for(const e of tail) {
      e._headGeneration = generation - 1;
      e._head = head;
    }
  }

  return electorTails;
}

// TODO: documentation
function _getAncestorHashes({events, ledgerNodeId}) {
  // get all ancestor hashes from every consensus X
  // TODO: `hashes` seems like it could use event references here rather
  //   than hashes themselves for faster comparisons, etc.
  const hashes = new Set();
  const hashBuffers = [];
  const descendants = [];
  const parentHashes = new Set();
  for(const x of events) {
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
  const candidateAncestryMaps = new Map();
  order.sort((a, b) => {
    let diff = 0;

    if(_getCreator(a) === _getCreator(b)) {
      diff = (a._generation || 0) - (b._generation || 0);
    }

    if(diff !== 0) {
      return diff;
    }

    if(!candidateAncestryMaps.has(a)) {
      candidateAncestryMaps.set(a, _buildAncestryMap(a));
    }
    if(!candidateAncestryMaps.has(b)) {
      candidateAncestryMaps.set(b, _buildAncestryMap(b));
    }

    // determine if `a` descends from `b` or vice versa
    if(_hasAncestors({
      target: a,
      candidates: [b],
      candidateAncestryMaps,
      found: new Set()
    })) {
      // `a` descends from `b`
      diff = 1;
    } else if(_hasAncestors({
      target: b,
      candidates: [a],
      candidateAncestryMaps,
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
 * Find consensus merge event Ys.
 *
 * In `first` mode, Ys are the first (or "tail" or oldest) events from each of
 * the electors.
 *
 * In `firstWithConsensusProof` and `batch` modes, Ys are merge events from a
 * supermajority of electors that are descendants of merge event Xs, where
 * merge event Xs are the first (or "tail" or oldest) events from each of
 * they have been shared with a supermajority of electors because merge event Ys
 * the electors. This means that merge event Xs have been endorsed by a
 * supermajority of electors. For each Y and X combo, it means that
 * "merge event Y (and its ancestors up to X) prove that merge event X has
 * been endorsed by a supermajority of electors".
 *
 * To have reached consensus, there must be at least a supermajority (a number
 * that constitutes 2/3rds + 1 of the current electors) of a set of merge
 * event Y candidates.
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
 * @param [mode='first'] an optional mode for selecting which events
 *          to find consensus on, see `findConsensus`.
 * @param logger the logger to use.
 *
 * @return a map with `consensus` and `yCandidates`; the `consensus` key's
 *         value is an array of merge event X and Y pairs where either X and Y
 *         are the same (mode = 'first') or where each merge event Y and its
 *         history proves its paired merge event X has been endorsed by a super
 *         majority of electors (other modes) -- another key, `proof` is
 *         also included with each pair that includes `y` and its direct
 *         ancestors until `x`, these, in total, constitute endorsers of `x`.
 *
 * @throws `NewElectorsRequiredError` when recovery mode has been triggered
 *         because consensus was not making sufficient progress.
 */
function _findMergeEventProof({
  ledgerNodeId, history, tails, blockHeight, electors, recoveryElectors = [],
  recoveryGenerationThreshold, mode = 'first',
  logger = noopLogger
}) {
  // FIXME: remove this conditional once confirmed it is enforced elsewhere
  // by throwing an error
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
    const recoverySupermajority = api.supermajority(recoveryElectors.length);
    canEnterRecoveryMode = (count >= recoverySupermajority);
  }

  //let startTime = Date.now();
  //logger.verbose('Start sync _findMergeEventProofCandidates');
  //console.log('Start sync _findMergeEventProofCandidates');
  const candidates = _findMergeEventProofCandidates({
    ledgerNodeId, tails, blockHeight, electors, recoveryElectors, mode, logger
  });
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

  const {xByElector, yByElector} = candidates;

  // if recovery mode can't be entered, skip tracking it... we will still
  // ensure that decisions can only be made with the appropriate recovery
  // elector thresholds and that no vital computations would be any different
  // if we *were* tracking recovery information
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

  //startTime = Date.now();
  //console.log('Start sync _findConsensusMergeEventProof');
  //logger.verbose('Start sync _findConsensusMergeEventProof');
  const ys = _findConsensusMergeEventProof(
    {ledgerNodeId, xByElector, yByElector, blockHeight,
      electors, recoveryElectors, recoveryGenerationThreshold, logger});
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

  const supermajority = api.supermajority(electors.length);
  return {
    // pair Ys with Xs
    consensus: ys.map(y => {
      const x = y._x;
      let proof = _flattenDescendants(
        {ledgerNodeId, x, descendants: y._xDescendants});
      if(proof.length === 0 && supermajority === 1) {
        // always include single elector as proof; enables continuity of that
        // single elector when computing electors in the next block via
        // quick inspection of `block.consensusProofHash`
        proof = [x];
      }
      return {y, x, proof};
    }),
    // return all yCandidates for debugging purposes
    yCandidates: [].concat(..._.values(yByElector))
  };
}

/**
 * Find the next merge events X and Y candidates according to `mode`.
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
 * @param [mode='first'] an optional mode for selecting which events
 *          to find consensus on, see `findConsensus`.
 * @param logger the logger to use.
 *
 * @return `null` or a map containing `yByElectors` and `xByElectors`; in
 *   `xByElectors`, each elector maps to the first event(s) on that elector's
 *   branch. In `yByElectors`, each elector maps to merge event(s) Y on its
 *   branch according to `mode`.
 */
function _findMergeEventProofCandidates({
  ledgerNodeId, tails, blockHeight, electors, recoveryElectors = [],
  mode = 'first', logger = noopLogger
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
        mergeEvent._recovery = {noProgressCounter: 0};
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
  let electorsForEndorsement = electors;
  let supermajorityForEndorsement = supermajority;
  let endorsementType = 'elector';
  if(mode !== 'first' && recoveryElectors.length > 0) {
    // use recovery electors for endorsement in batch mode
    /* Note: Any other modes that might be created in the future must be
    carefully vetted to ensure that optimizations made elsewhere would not
    be making false assumptions. For example, detecting endorsed recovery
    proposals in the ancestry of a voting event assume that an RE with
    an endorsed recovery proposal must have made a Y event and would therefore
    be a voting ancestor. */
    electorsForEndorsement = recoveryElectors;
    supermajorityForEndorsement = api.supermajority(recoveryElectors.length);
    endorsementType = 'recovery';
  }
  for(const elector in xByElector) {
    for(const x of xByElector[elector]) {
      //console.log('FINDING Y FOR X', x, elector);
      let results;
      if(mode === 'first') {
        results = [{mergeEvent: x, descendants: new Map()}];
      } else {
        // pass `x._index.ancestryMap` as the ancestry map to short-circuit
        // searches as it includes all ancestors of X -- which should not be
        // searched when finding a Y because they cannot lead to X
        results = _findEndorsementMergeEvent({
          ledgerNodeId, x,
          electors: electorsForEndorsement,
          supermajority: supermajorityForEndorsement,
          ancestryMap: x._index.ancestryMap,
          type: endorsementType
        });
      }
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
 * @param type the type of endorsement: 'elector' or 'recovery'.
 *
 * @return `null` or an array of the earliest endorsement merge event(s)
 *           coupled with descendants maps: {mergeEvent, descendants}.
 */
function _findEndorsementMergeEvent({
  ledgerNodeId, x, electors, supermajority,
  descendants = new Map(), ancestryMap = _buildAncestryMap(x),
  type
}) {
  //console.log('EVENT', x.eventHash);

  if(supermajority === 1) {
    // trivial case, return `x`
    if(type === 'elector') {
      x._endorsers = new Set([x]);
    } else if(x._recovery) {
      x._recoveryEndorsers = new Set([x]);
    }
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

      // see if there are a supermajority of endorsers of `x` now
      if(_hasSufficientEndorsers({
        x, descendants, electorSet, supermajority, type
      })) {
        //console.log('supermajority of endorsers found at generation',
        //  treeDescendant._generation);
        //console.log();
        results.push({
          mergeEvent: treeDescendant,
          descendants
        });
        continue;
      }
      //console.log('not enough endorsers yet at generation',
      //  treeDescendant._generation);
      // FIXME: remove me
      //const ancestors = _flattenDescendants({ledgerNodeId, x, descendants});
      //console.log('total descendants so far', ancestors.map(r=>({
      //  creator: _getCreator(r),
      //  generation: r._generation,
      //  hash: r.eventHash,
      //  children: r._treeChildren && r._treeChildren.length
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
  recoveryElectors, recoveryGenerationThreshold, logger = noopLogger
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
    electors, recoveryElectors, recoveryGenerationThreshold, logger});
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
 * same-tree descendant that has a supermajority of endorsers of `x`.
 *
 * @param ledgerNodeId the ID of the current ledgerNode, for logging.
 * @param x the starting event to find descendants of.
 * @param y the stopping event to find ancestors of.
 * @param descendants the descendants map to use.
 * @param ancestryMap a map of the ancestry of `x` to optimize searching.
 * @param diff an array to populate with new descendants found.
 */
function _findDescendantsInPath({
  ledgerNodeId, x, y, descendants = new Map(),
  ancestryMap = _buildAncestryMap(x), diff = null
}) {
  // find all descendants of `x` that are ancestors of `y`
  let next = [y];
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(ancestryMap.has(event)) {
        //console.log('SKIPPING', event.eventHash);
        continue;
      }
      for(const parent of event._parents) {
        //console.log('event.parent', {
        //  creator: _getCreator(parent),
        //  generation: parent._generation,
        //  hash: parent.eventHash
        //});
        const d = descendants.get(parent);
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
        descendants.set(parent, [event]);
        if(diff) {
          diff.push(event);
        }
        next.push(parent);
      }
    }
  }
  // console.log(
  //   'entries in descendants', [...descendants.keys()].map(e => e.eventHash));
}

// shallow copy a map of arrays (arrays will be shallow copied)
function _copyMapOfArrays(mapOfArrays) {
  const copy = new Map();
  for(const [k, v] of mapOfArrays) {
    copy[k] = v.slice();
  }
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
      const d = descendants.get(event);
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
      ledgerNodeId, elector, index: mostRecentAncestors, ancestorEvent
    });
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
      event._index.electorPedigree = {};
      for(const key in parentElectorPedigree) {
        event._index.electorPedigree[key] =
          _copyMapOfArrays(parentElectorPedigree[key]);
      }
    }
  } else {
    // initialize elector pedigree
    event._index.electorPedigree = {};
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
    // elector pedigree is all `descendants` of tails from `elector` until
    // `event._treeParent` ... and will be updated by this call to include
    // additional descendants until `event`
    let descendants = event._index.electorPedigree[elector];
    if(!descendants) {
      // no descendants map for this elector yet, add it
      event._index.electorPedigree[elector] = descendants = new Map();
    }

    // should just be one loop through here in the common case, but have to
    // account for byzantine electors that have forked
    for(const ancestorEvent of mergeEvents) {
      // compute diff in descendants (`descendants` will be updated and all
      // the new entries will appear in `diff`)
      const {ancestryMap} = ancestorEvent._index;
      const addedToDiffPreviously = descendants.has(ancestorEvent);
      _findDescendantsInPath({
        ledgerNodeId,
        x: ancestorEvent,
        y: event,
        descendants,
        ancestryMap,
        diff
      });
      if(!addedToDiffPreviously && descendants.has(ancestorEvent)) {
        // ensure that the `ancestorEvent` itself is added to the diff if it
        // was not previously
        diff.push(ancestorEvent);
      }
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

function _hasSufficientEndorsers({
  x, descendants, electorSet, supermajority, type
}) {
  // always count `x` as self-endorsed
  const xCreator = _getCreator(x);
  const endorsers = new Set([xCreator]);
  if(type === 'elector') {
    x._endorsers = endorsers;
  } else {
    x._recoveryEndorsers = endorsers;
  }
  let next = [x];
  //console.log('checking for sufficient endorsers...');
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      const d = descendants.get(event);
      if(d) {
        // `event` is in the computed path of descendants
        for(const e of d) {
          const creator = _getCreator(e);
          if(electorSet.has(creator)) {
            endorsers.add(creator);
            //console.log(
            // 'total', endorsers.size, 'supermajority', supermajority);
            //console.log('electors', electorSet);
            //console.log('endorsers', endorsers);
            if(endorsers.size >= supermajority) {
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
  recoveryElectors, recoveryGenerationThreshold, logger = noopLogger
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
          influentialElectorTails
        });
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
        // if some ancestors haven't computed their recovery info yet, defer
        const recoveryEvents = Object.values(event._index.mostRecentAncestors)
          .filter(e => e && e !== event && recoverySet.has(_getCreator(e)));
        if(recoveryEvents.some(e => !(e._recovery && e._recovery.computed))) {
          next.push(event);
          continue;
        }

        if(!event._recovery) {
          // shallow copy tree parent's recovery info
          event._recovery = {
            ...event._treeParent._recovery,
            computed: false
          };
        }
      }

      // if event is voting, compute its support
      if(event._votes) {
        /*const startTime = Date.now();
        logger.debug('Start sync _findConsensusMergeEventProof: ' +
          '_computeSupport');*/
        const result = _computeSupport({
          ledgerNodeId, event, blockHeight,
          electors, recoveryElectors, logger
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

      // if event is from a recovery elector, compute its recovery info
      if(event._recovery) {
        _computeRecovery({
          ledgerNodeId, event, blockHeight, electors,
          recoveryElectors, recoveryGenerationThreshold, logger});
      }

      // add tree children
      next.push(...(event._treeChildren || []));
    }
  }

  return null;
}

// TODO: consider merging this function's implementation with `_computeSupport`
// to reduce number of loops when recovery mode is supported (this would be
// an optimization, it should not change the computed results, will have to
// be careful to make sure counting decision proposals (only count tree
// parent's), etc. to avoid changing anything erroneously when writing this
// optimization -- unit tests MUST cover it to check for correctness
function _computeRecovery({
  ledgerNodeId, event, blockHeight, electors,
  recoveryElectors, recoveryGenerationThreshold, logger = noopLogger
}) {
  if(event._recovery.computed) {
    return;
  }

  if(event._recovery.skip) {
    event._recovery.computed = true;
    return;
  }

  /* Rules:

  1. Update event's `noProgressCounter`. If `event` is the first event on the
     branch, calculate the next events for each of the decision endorsement and
     recovery endorsement chains. Otherwise, determine if the event is on
     either (or both) of the chains. If the event is on the recovery endorsement
     chain, then increment the `noProgressCounter` and calculate the next
     event on the recovery endorsement chain. If the event is on the decision
     endorsement chain, then reset the `noProgressCounter` and calculate the
     next event on the decision endorsement chain. Note that if event is on
     both chains, the counter will be reset.
  2. Iterate over most recent ancestor events from all electors and
     tally decision proposals, endorsed recovery proposals, and "met no
     progress threshold"s from most recent ancestors, using `event` when
     processing its own branch (instead of its tree parent). The count for
     "met no progress threshold" is incremented when an event's
     `noProgressCounter` is greater than or equal to the
     `recoveryGenerationThreshold`.
  3. If event's branch has an existing recovery proposal decide if it should
     be rejected. Reject it if either the "met no progress threshold" is
     < `2r+1` or if the tallied decision proposals >= `f+1`. Adjust the
     endorsed recovery proposal count if necessary.
  4. If a recovery proposal endorse point has been reached; mark any matching
     recovery proposal as endorsed. Adjust the endorsed recovery proposal
     count if necessary.
  5. If >= `r+1` endorsed recovery proposals, enter recovery mode.
  6. If the event's branch has no recovery proposal, no decision proposal,
     the "met no progress threshold" tally is >= `2r+1`, and there
     are < `f+1` decision proposals, then create a recovery proposal on the
     event and compute its future endorse point.
  */

  /*
  1. Update event's `noProgressCounter`. If `event` is the first event on the
     branch, calculate the next events for each of the decision endorsement and
     recovery endorsement chains. Otherwise, determine if the event is on
     either (or both) of the chains. If the event is on the recovery endorsement
     chain, then increment the `noProgressCounter` and calculate the next
     event on the recovery endorsement chain. If the event is on the decision
     endorsement chain, then reset the `noProgressCounter` and calculate the
     next event on the decision endorsement chain. Note that if event is on
     both chains, the counter will be reset.
  */
  const {_recovery: recovery} = event;
  const supermajority = api.supermajority(electors.length);
  const recoverySupermajority = api.supermajority(recoveryElectors.length);
  if(!event._treeParent) {
    // first event, compute next times to reset/increment no progress counter
    recovery.progressCheck = event;
    recovery.noProgressCheck = event;
    _computeProgressEndorsement({ledgerNodeId, event, electors, supermajority});
    _computeNoProgressEndorsement(
      {ledgerNodeId, event, recoveryElectors, recoverySupermajority});
  } else {
    // decide if progress check reached
    const isProgressCheck = (event._endorsesProgress &&
      recovery.progressCheck &&
      event._endorsesProgress.includes(recovery.progressCheck));
    // decide if no progress check reached
    const isNoProgressCheck = (event._endorsesNoProgress &&
      recovery.noProgressCheck &&
      event._endorsesNoProgress.includes(recovery.noProgressCheck));

    if(isNoProgressCheck) {
      // mark endorsed, increment counter and calculate next no progress check
      recovery.noProgressCounter++;
      recovery.noProgressCheck._noProgressEndorsed = true;
      recovery.noProgressCheck = event;
      _computeNoProgressEndorsement(
        {ledgerNodeId, event, recoveryElectors, recoverySupermajority});
    }

    if(isProgressCheck) {
      // mark endorsed, reset counter (takes precedence over an incremented
      // counter that may have just happened above because no progress and
      // progress checks happened to coincide), and calculate next progress
      // check
      recovery.noProgressCounter = 0;
      recovery.progressCheck._progressEndorsed = true;
      recovery.progressCheck = event;
      _computeProgressEndorsement({
        ledgerNodeId, event, electors, supermajority
      });
    }
  }

  /*
  2. Iterate over most recent ancestor events from all electors and
     tally decision proposals, endorsed recovery proposals, and "met no
     progress threshold"s from most recent ancestors, using `event` when
     processing its own branch (instead of its tree parent). The count for
     "met no progress threshold" is incremented when an event's
     `noProgressCounter` is greater than or equal to the
     `recoveryGenerationThreshold`.
  */
  let decisionProposals = 0;
  let endorsedRecoveryProposals = 0;
  let meetThreshold = 0;
  const recoverySet = new Set(recoveryElectors.map(e => e.id));
  const creator = _getCreator(event);
  const mostRecentAncestors = {
    ...event._index.mostRecentAncestors,
    [creator]: event
  };
  for(const elector in mostRecentAncestors) {
    const ancestorEvent = mostRecentAncestors[elector];
    if(!ancestorEvent) {
      // byzantine node detected, skip
      continue;
    }
    // always count decision proposals, regardless of recovery elector status
    if(ancestorEvent._proposal) {
      decisionProposals++;
    }
    if(!recoverySet.has(elector)) {
      // ancestor is not a recovery elector
      continue;
    }
    if(_hasEndorsedRecoveryProposal(ancestorEvent)) {
      endorsedRecoveryProposals++;
    }
    if(ancestorEvent._recovery.noProgressCounter >=
      recoveryGenerationThreshold) {
      meetThreshold++;
    }
  }

  /*
  3. If event's branch has an existing recovery proposal decide if it should
     be rejected. Reject it if either the "met no progress threshold" is
     < `2r+1` or if the tallied decision proposals >= `f+1`. Adjust the
     endorsed recovery proposal count if necessary.
  */
  const f = api.maximumFailures(electors.length);
  if(recovery.proposal &&
    ((meetThreshold < recoverySupermajority) ||
    (decisionProposals >= (f + 1)))) {
    // reject recovery proposal, adjusting count as needed
    if(_hasEndorsedRecoveryProposal(event)) {
      endorsedRecoveryProposals--;
    }
    delete recovery.proposal;
  }

  /*
  4. If a recovery proposal endorse point has been reached; mark any matching
     recovery proposal as endorsed.
  */
  if(event._endorsesRecovery && recovery.proposal) {
    // endorse point reached... only mark endorsed if it matches
    if(event._endorsesRecovery.includes(recovery.proposal)) {
      recovery.proposal._recoveryEndorsed = true;
      endorsedRecoveryProposals++;
    }
  }

  /*
  5. If >= `r+1` endorsed recovery proposals, enter recovery mode.
  */
  const r = api.maximumFailures(recoveryElectors.length);
  if(endorsedRecoveryProposals >= (r + 1)) {
    // enter recovery mode
    const err = new Error('New electors required.');
    err.name = 'NewElectorsRequiredError';
    throw err;
  }

  /*
  6. If the event's branch has no recovery proposal, no decision proposal,
     the "met no progress threshold" tally is >= `2r+1`, and there
     are < `f+1` decision proposals, then create a recovery proposal on the
     event and compute its future endorse point.
  */
  if(!recovery.proposal && !event._proposal &&
    (meetThreshold >= recoverySupermajority) &&
    (decisionProposals < (f + 1))) {
    recovery.proposal = event;
    _computeRecoveryEndorsement({
      ledgerNodeId, event, electors, recoveryElectors, recoverySupermajority
    });
  }

  // recovery info computed
  event._recovery.computed = true;
}

function _computeSupport({
  ledgerNodeId, event, blockHeight, electors,
  recoveryElectors, logger = noopLogger
}) {
  // support already chosen for event
  if(event._supporting !== undefined) {
    return;
  }

  /* Choosing support:

  First, if the event's previous self-vote was `false` then the event is
  byzantine and there is nothing to compute, its vote has been revoked.

  Compute support:
    1. If >= `f+1` support for any same set, union all sets with >= `f+1`
       support.
    2. Otherwise, union support.

  Rejecting proposals/Deciding:
    1. If support is not for the proposal, reject.
    2. If >= `f+1` endorsed proposals for the same set, decide.

  Creating a new proposal:

    1. If no existing proposal (or rejected it) and >= `2f+1` nodes support our
       next choice (endorsement not required), propose it.
       CAVEAT: If `recoveryElectors.length > 0` then >= `2r+1` recovery
       electors must also support the next choice (where `r` is the
       acceptable number of failed recovery electors). If a recovery proposal
       has been made on the event's branch, do not create the proposal.
       Note: A decision proposal will be subsequently rejected when recovery
       rules are applied if there are `r+1` recovery proposals enter the
       ancestry of its branch.
  */

  /* Endorsement rules:

  1. An event is an "endorsement event" if the events are on the same branch
     and if the endorsement event is the first event since the earlier event
     that includes >= `2f+1` electors in its ancestry that have the earlier
     event in their ancestry (these electors have "endorsed" the earlier event).
  2. A proposal is endorsed at its endorsement event only if support at the
     endorsement event matches support at the proposal (i.e., the proposal was
     never rejected).

  CAVEAT: If `recoveryElectors.length > 0` then:

  1. A proposal endorsement requires >= `2r+1` recovery electors instead of
     >= `2f+1` decision electors.
  2. A recovery proposal is endorsed at its endorsement event if the
     endorsement event's ancestry includes >= `2r+1` recovery electors
     (instead of >= `2f+1` decision electors) that have the recovery proposal
     in their ancestry.
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
  const r = api.maximumFailures(recoveryElectors.length);
  const recoverySupermajority = api.supermajority(recoveryElectors.length);
  const recoverySet = new Set(recoveryElectors.map(e => e.id));

  // TODO: optimize
  /*logger.verbose('Continuity2017 _computeSupport finding votes seen...',
    {ledgerNode: ledgerNodeId, eventHash: event.eventHash});*/
  // tally votes
  /*const startTime = Date.now();
  logger.debug('Start sync _computeSupport vote tally proper');*/
  const tally = [];
  const fPlusOne = f + 1;
  const fPlusOneSupport = new Set();
  for(const elector in event._votes) {
    const e = event._votes[elector];
    if(e === false || e === event) {
      // do not count byzantine votes or "most recent vote" that is
      // from `event` itself; this latter self-referential case only happens
      // when `event` is an initial Y ... but its support hasn't been set yet,
      // so it must not be tallied
      continue;
    }

    // find existing result to update that matches support set
    let tallyResult = _.find(tally, _findSetInTally(e._supporting));
    if(!tallyResult) {
      // no matching result found, initialize new result
      tallyResult = {
        set: e._supporting,
        count: 0,
        endorsedProposalCount: 0,
        recoveryElectorCount: 0
      };
      tally.push(tallyResult);
    } else {
      // ensure same instance of set is used for faster comparisons
      e._supporting = tallyResult.set;
    }

    // update counts
    tallyResult.count++;
    if(tallyResult.count >= fPlusOne) {
      fPlusOneSupport.add(tallyResult);
    }
    if(_hasEndorsedProposal(e)) {
      tallyResult.endorsedProposalCount++;
    }
    if(recoverySet.has(elector)) {
      tallyResult.recoveryElectorCount++;
    }
  }
  /*logger.debug('End sync _computeSupport vote tally proper', {
    duration: Date.now() - startTime
  });*/
  /*console.log('tally', tally.map(t => ({
    set: t.set.map(e => e._generation),
    count: t.count,
    epc: t.endorsedProposalCount
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

  /* Choose support:
    1. If >= `f+1` support for any same set, union all sets with >= `f+1`
       support.
    2. Otherwise, union support.
  */
  let nextChoice;
  if(fPlusOneSupport.size > 0) {
    // compute union of all sets with >= `f+1` support
    if(fPlusOneSupport.size === 1) {
      // optimize for single set
      nextChoice = fPlusOneSupport.values().next().value;
    } else {
      // union multiple sets
      let union = new Set();
      for(const {set} of fPlusOneSupport) {
        for(const e of set) {
          union.add(e);
        }
      }
      union = [...union];

      // set the next choice to the matching tally or create it
      nextChoice = _.find(tally, _findSetInTally(union));
      if(!nextChoice) {
        // create new choice
        nextChoice = {
          set: union,
          count: 0,
          endorsedProposalCount: 0,
          recoveryElectorCount: 0
        };
      }
    }
  } else {
    // compute the union of all support
    let union = new Set();
    for(const elector in event._votes) {
      const votingEvent = event._votes[elector];
      if(votingEvent) {
        // if `votingEvent` is itself, include it in the union as an initial Y
        if(votingEvent === event) {
          union.add(votingEvent);
          continue;
        }
        // add computed support from `votingEvent`
        for(const e of votingEvent._supporting) {
          union.add(e);
        }
      }
    }
    union = [...union];

    // set the next choice to the matching tally or create it
    nextChoice = _.find(tally, _findSetInTally(union));
    if(!nextChoice) {
      // create new choice
      nextChoice = {
        set: union,
        count: 0,
        endorsedProposalCount: 0,
        recoveryElectorCount: 0
      };
    }
  }

  /*console.log('SUPPORT at ', event.eventHash, 'IS FOR',
    nextChoice.set.map(e => e._generation));*/

  /*
    If the previous supporting is different from the new support, update the
    next choice count.
  */
  const previousVoteEvent = event._votes[creator];
  const switched = !(previousVoteEvent && previousVoteEvent._supporting &&
    _compareSupportSet(nextChoice.set, previousVoteEvent._supporting));
  if(switched) {
    // increment next choice
    nextChoice.count++;
    if(recoverySet.has(creator)) {
      nextChoice.recoveryElectorCount++;
    }
  }

  /*
  If you have an existing proposal, check rejection.
  */
  // get existing proposal on the event's branch
  let proposal = event._treeParent ? event._treeParent._proposal : null;
  if(proposal) {
    // reject proposal if local support does not match
    let reject = !_compareSupportSet(proposal._supporting, nextChoice.set);

    // if recovery mode is enabled, reject proposal if most recent ancestors
    // have >= `r+1` recovery proposals
    if(!reject && recoverySet.size > 0) {
      // TODO: Could be optimized away if entering recovery mode not possible

      // count recovery proposals from REs
      let recoveryProposals = 0;
      const {mostRecentAncestors} = event._index;
      const rPlusOne = r + 1;
      for(const elector of recoverySet) {
        const ancestorEvent = mostRecentAncestors[elector];
        if(ancestorEvent && ancestorEvent._recovery.proposal) {
          recoveryProposals++;
          if(recoveryProposals >= rPlusOne) {
            reject = true;
            break;
          }
        }
      }
    }

    if(reject) {
      // Note: No need to find tally result for the set the proposal supports
      // to adjust it... because it is not used (only `nextChoice` is used)
      proposal = null;
    }
  }

  if(event._endorsesProposal && proposal && !proposal._proposalEndorsed) {
    // proposal endorse point reached...

    /* Note: Only mark proposal endorsed if it matches. This prevents
    having to remove the proposal if it was rejected and allows for debugging
    to see that a proposal was not marked endorsed at its endorse point
    because it was rejected. */
    if(event._endorsesProposal.includes(proposal)) {
      proposal._proposalEndorsed = true;
      // ensure count is updated (note that a decision proposal precludes a
      // recovery proposal so no need to check for one here before updating
      nextChoice.endorsedProposalCount++;
    }
  }

  /*
  If `f+1` endorsed proposals, decide; consensus has been reached. Otherwise,
  continue on and hope the next event will decide...
  */
  if(nextChoice.endorsedProposalCount >= fPlusOne) {
    // consensus reached, all done!
    event._decision = true;
    return nextChoice.set;
  }

  /*
  If the next choice has a supermajority and has no existing (unrejected)
  proposal, create a new one. CAVEAT: If `recoveryElectors > 0` and
  event has a recovery proposal in its ancestry, do not create proposal;
  also require a supermajority of recovery electors to support as well.
  */
  const hasSupermajority = (nextChoice.count >= supermajority &&
    (recoveryElectors.length === 0 ||
      nextChoice.recoveryElectorCount >= recoverySupermajority));
  if(hasSupermajority && !proposal &&
    !(event._recovery && event._recovery.proposal)) {
    // no proposal yet, use current event
    proposal = event;
    _computeProposalEndorsement({
      ledgerNodeId, event, electors, supermajority,
      recoveryElectors, recoverySupermajority
    });
  }

  // set event's proposal
  if(proposal) {
    event._proposal = proposal;
  }

  // support next choice
  event._supporting = nextChoice.set;
  event._votes[creator] = event;

  return null;
}

function _hasEndorsedProposal(event) {
  const proposal = event._proposal;
  if(proposal && proposal._proposalEndorsed) {
    for(const endorsement of proposal._proposalEndorsement) {
      if(event._generation >= endorsement._generation) {
        return true;
      }
    }
  }
  return false;
}

function _hasEndorsedRecoveryProposal(event) {
  const proposal = event._recovery && event._recovery.proposal;
  if(proposal && proposal._recoveryEndorsed) {
    for(const endorsement of proposal._recoveryEndorsement) {
      if(event._generation >= endorsement._generation) {
        return true;
      }
    }
  }
  return false;
}

function _findSetInTally(set) {
  if(!set) {
    return () => false;
  }
  return tallyResult => _compareSupportSet(tallyResult.set, set);
}

function _compareSupportSet(set1, set2) {
  return (set1 === set2) ||
    (set1.length === set2.length &&
    _.differenceWith(set1, set2, _compareEventHashes).length === 0);
}

function _compareEventHashes(r1, r2) {
  return r1.eventHash === r2.eventHash;
}

function _computeProposalEndorsement({
  ledgerNodeId, event, electors, supermajority,
  recoveryElectors, recoverySupermajority
}) {
  // if recovery mode disabled, use electors, otherwise use recovery electors
  const type = (recoveryElectors.length === 0) ? 'elector' : 'recovery';
  return _computeEndorsement({
    ledgerNodeId, event, electors, supermajority,
    recoveryElectors, recoverySupermajority,
    type,
    endorsementKey: '_proposalEndorsement', endorsesKey: '_endorsesProposal'
  });
}

function _computeProgressEndorsement({
  ledgerNodeId, event, electors, supermajority
}) {
  return _computeEndorsement({
    ledgerNodeId, event, electors, supermajority,
    type: 'elector',
    endorsementKey: '_progressEndorsement', endorsesKey: '_endorsesProgress'
  });
}

function _computeNoProgressEndorsement({
  ledgerNodeId, event, recoveryElectors, recoverySupermajority
}) {
  return _computeEndorsement({
    ledgerNodeId, event, recoveryElectors, recoverySupermajority,
    type: 'recovery',
    endorsementKey: '_noProgressEndorsement', endorsesKey: '_endorsesNoProgress'
  });
}

function _computeRecoveryEndorsement({
  ledgerNodeId, event, recoveryElectors, recoverySupermajority
}) {
  return _computeEndorsement({
    ledgerNodeId, event, recoveryElectors, recoverySupermajority,
    type: 'recovery',
    endorsementKey: '_recoveryEndorsement', endorsesKey: '_endorsesRecovery'
  });
}

function _computeEndorsement({
  ledgerNodeId, event, electors, supermajority,
  recoveryElectors = [], recoverySupermajority = 0,
  type, endorsementKey, endorsesKey
}) {
  if(event[endorsementKey]) {
    // already computed
    return;
  }

  let sharedEndorsementKey;
  if(type === 'recovery') {
    // recovery endorsement is *only* on recovery electors, so treat them
    // like regular electors when looking for endorsement
    electors = recoveryElectors;
    supermajority = recoverySupermajority;
    sharedEndorsementKey = '_recoveryElectorEndorsement';
  } else {
    sharedEndorsementKey = '_electorEndorsement';
  }

  if(event[sharedEndorsementKey] !== undefined) {
    // reuse previously computed endorsements of the same type (with
    // the same `sharedEndorsementKey`)
    const endorsement = event[sharedEndorsementKey];
    if(endorsement === false) {
      event[endorsementKey] = false;
      return;
    }
    // ensure `endorsesKey` is updated
    for(const e of endorsement) {
      if(e[endorsesKey]) {
        e[endorsesKey].push(event);
      } else {
        e[endorsesKey] = [event];
      }
    }
    event[endorsementKey] = endorsement;
    return;
  }

  // `endorsement` must be computed
  const ancestryMap = event._index.ancestryMap || _buildAncestryMap(event);
  const endorsement = _findEndorsementMergeEvent({
    ledgerNodeId, x: event, electors, supermajority,
    descendants: new Map(), ancestryMap, type
  });
  if(endorsement) {
    // `endorsement` will be an array as byzantine nodes may fork, but forks
    // will get detected via endorse point(s) due to overlap of at least
    // one functioning node
    const allEndorsements = [];
    for(const {mergeEvent: e} of endorsement) {
      if(e[endorsesKey]) {
        e[endorsesKey].push(event);
      } else {
        e[endorsesKey] = [event];
      }
      /*console.log(
        `created ${endorsementKey} endorse point for`, event._generation,
        'at', e._generation);*/
      allEndorsements.push(e);
    }
    event[sharedEndorsementKey] = event[endorsementKey] = allEndorsements;
  } else {
    // no endorsement of given type in DAG yet for `event`
    event[sharedEndorsementKey] = event[endorsementKey] = false;
  }
}

function _getPriorityPeers({
  ledgerNodeId, history, tails, blockHeight, electors,
  recoveryElectors, recoveryGenerationThreshold,
  mode, logger
}) {
  /* Algorithm:

  Determine which electors could help progress consensus by creating a
  new merge event.

  For each elector, determine if it needs an endorsement for a decision
  proposal or a recovery proposal. If so, track whether any other elector can
  endorse and see if there are enough to reach a supermajority.

  For each elector, go through each of the other elector's heads and see if
  they are new (not in the elector's ancestry) and, if so, if creating a
  new merge event would either provide an endorsement they need or if their
  new support has changed in a way that would influence a new merge event.

  If creating a new merge event would be helpful, prioritize the elector.
  */
  const recoverySet = new Set(recoveryElectors.map(e => e.id));
  const supermajority = api.supermajority(electors.length);
  const recoverySupermajority = api.supermajority(recoveryElectors.length);
  const f = api.maximumFailures(electors.length);
  const r = api.maximumFailures(recoveryElectors.length);

  // collect non-byzantine peers and their heads
  const nonByzantinePeers = [];
  const heads = new Map();
  for(const {id: elector} of electors) {
    const electorTails = tails[elector];
    if(!electorTails) {
      // no events from the elector yet
      nonByzantinePeers.push(elector);
      continue;
    }

    if(electorTails.length > 1) {
      // byzantine node, do not tally
      continue;
    }
    const [tail] = electorTails;
    if(tail._head.length > 1) {
      // byzantine node, do not tally
      continue;
    }

    // non-byzantine peer with head
    nonByzantinePeers.push(elector);
    heads.set(elector, tail._head[0]);
  }

  // go through every peer and determine if they should be prioritized (if
  // a new merge event from them could progress consensus); if any peer has no
  // head yet, prioritize it
  const priorityPeers = new Set(nonByzantinePeers.filter(p => !heads.has(p)));
  const endorsersKey = recoverySet.size > 0 ?
    '_recoveryEndorsers' : '_endorsers';
  for(const [elector, head] of heads) {
    // skip any peer that is already prioritized
    if(priorityPeers.has(elector)) {
      continue;
    }

    // determine if `elector` needs an endorsement for a decision/recovery
    // proposal... use this to test whether or not merging each other head
    // could progress consensus
    let proposalEndorsers = null;
    if(head._proposal && !_hasEndorsedProposal(head)) {
      proposalEndorsers = head._proposal[endorsersKey];
    } else if(head._recovery && head._recovery.proposal &&
      !_hasEndorsedRecoveryProposal(head)) {
      proposalEndorsers = new Set(head._recovery.proposal[endorsersKey]);
    }

    // check other heads to determine if merging them in would be beneficial
    for(const [otherElector, otherHead] of heads) {
      // can't help by merging self
      if(otherElector === elector) {
        continue;
      }

      // if `elector` has a previous MRA from `otherElector`, see if there is
      // an important change to merge in
      const mra = head._index && head._index.mostRecentAncestors &&
        head._index.mostRecentAncestors[otherElector];
      if(!mra) {
        // never seen any event from `otherHead`, prioritize `elector` to
        // merge it in
        priorityPeers.add(elector);
        break;
      }

      if(mra === otherHead) {
        // `otherHead` is already in elector's ancestry, won't help to merge it
        continue;
      }

      // if `otherHead` has new endorsed decision/recovery proposal, then
      // prioritize to merge it in
      if((!_hasEndorsedProposal(mra) && _hasEndorsedProposal(otherHead)) ||
        (!_hasEndorsedRecoveryProposal(mra) &&
          _hasEndorsedRecoveryProposal(otherHead))) {
        priorityPeers.add(elector);
        break;
      }

      // if `otherHead` has a new decision/proposal vs mra, then prioritize
      // `elector` to merge it in
      if(mra._proposal !== otherHead._proposal ||
        (mra._recovery && otherHead._recovery &&
          mra._recovery.proposal !== otherHead._recovery.proposal)) {
        priorityPeers.add(elector);
        break;
      }

      // if `otherHead`'s support has switched, then prioritize `elector` to
      // merge it in
      if((!mra._supporting && otherHead._supporting) ||
        (mra._supporting && otherHead._supporting &&
        !_compareSupportSet(mra._supporting, otherHead._supporting))) {
        priorityPeers.add(elector);
        break;
      }

      // if recovery mode is enabled only recovery electors endorse...
      if(recoverySet.size === 0 || recoverySet.has(elector)) {
        // if `otherHead` has a proposal that `elector` can endorse, then
        // prioritize `elector`
        if(otherHead._proposal && otherHead._proposal[endorsersKey] &&
          !_hasEndorsedProposal(otherHead) &&
          !otherHead._proposal[endorsersKey].has(elector)) {
          priorityPeers.add(elector);
          break;
        }

        // if `otherHead` has a recovery proposal that `elector` can endorse,
        // then prioritize `elector`
        if(otherHead._recovery && otherHead._recovery.proposal &&
          otherHead._recovery.proposal[endorsersKey] &&
          !_hasEndorsedRecoveryProposal(otherHead) &&
          !otherHead._recovery.proposal[endorsersKey].has(elector)) {
          priorityPeers.add(elector);
          break;
        }
      }

      // if `elector` needs a proposal endorsement and `otherElector` hasn't
      // provided it, track it to check for threshold later
      if(proposalEndorsers &&
        (recoverySet.size === 0 || recoverySet.has(otherElector)) &&
        !proposalEndorsers.has(otherElector) &&
        _hasElectorAncestor({
          event: otherHead,
          elector,
          ancestor: head._proposal || head._recovery.proposal
        })) {
        proposalEndorsers.add(otherElector);
      }
    }

    // if proposal endorsers would reach threshold, then prioritize `elector`
    const threshold = (recoverySet.size > 0 ?
      recoverySupermajority : supermajority);
    if(proposalEndorsers && proposalEndorsers.size >= threshold) {
      priorityPeers.add(elector);
    }
  }

  if(recoverySet.size > 0) {
    // there must always be > `r` recovery electors that are prioritized
    // to ensure recovery mode can be entered; if there are <= r, add all
    // recovery electors
    // TODO: determine how to optimize
    const count = [...priorityPeers].reduce(
      (total, elector) => recoverySet.has(elector) ? 1 : 0, 0);
    if(count <= r) {
      for(const recoveryElector of nonByzantinePeers) {
        if(recoverySet.has(recoveryElector)) {
          priorityPeers.add(recoveryElector);
        }
      }
    }
  } else if(priorityPeers.size <= f) {
    // in non-recovery mode, we must guarantee there will always be > `f`
    // electors in `priorityPeers` or else it is possible that all prioritized
    // peers will be failed nodes
    for(const elector of nonByzantinePeers) {
      priorityPeers.add(elector);
    }
  }

  return [...priorityPeers];
}

function _buildAncestryMap(event) {
  // ancestry map can actually represented as a set of ancestors
  const map = new Set();
  let next = [event];
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(!map.has(event)) {
        map.add(event);
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
function _hasAncestors({
  target, candidates, min = candidates.length,
  candidateAncestryMaps, found
}) {
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
        c => !(candidateAncestryMaps.get(c).has(parent.eventHash)));
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

function _hasElectorAncestor({event, elector, ancestor}) {
  const mra = event._parents.length > 0 &&
    event._index.mostRecentAncestors[elector];
  return mra && mra._generation >= ancestor._generation;
}
