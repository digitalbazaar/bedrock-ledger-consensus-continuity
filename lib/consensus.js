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
 * @param [mode='first'] an optional mode for selecting which events
 *          to find consensus on:
 *          first: create support sets from the first (aka "tail" or oldest)
 *            event from each participating elector and return the events that
 *            are in the consensus support set as having reached consensus
 *            (i.e., `eventHashes`) (mode is aka "Ys are Xs").
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
  mode = 'first', logger = noopLogger
}) => {
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
    ledgerNodeId, history, tails, blockHeight, electors, mode, logger
  });
  /*console.log('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findMergeEventProof', {
    duration: Date.now() - startTime
  });*/
  if(proof.consensus.length === 0) {
    const priorityPeers = _getPriorityPeers({
      ledgerNodeId, history, tails, blockHeight, electors, mode, logger
    });

    //logger.verbose('findConsensus no proof found, exiting');
    return {
      consensus: false,
      priorityPeers
    };
  }
  //logger.verbose('findConsensus proof found, proceeding...');

  // gather events that have achieved consensus
  // consider `x` events to have achieved consensus (in mode='first' these
  // are the same as `y` events anyway
  const events = proof.consensus.map(p => p.x);

  // gather events used as proof of the consensus support set
  const creators = new Set();
  // consensus proof hashes for `mode`='first' are only the consensus set,
  // everything else (including these hashes) have achieved consensus
  const consensusProofHashes = events.map(e => {
    creators.add(_getCreator(e));
    return e.eventHash;
  });

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
 * Find consensus merge event `y`s.
 *
 * In `first` mode, Ys are the first (or "tail" or oldest) events from each of
 * the electors. No other modes are presently implemented.
 *
 * @param ledgerNodeId the ID of the local ledger node.
 * @param history recent history rooted at the ledger node's local branch
 *          including ONLY merge events, it must NOT include local regular
 *          events.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *          by elector ID.
 * @param electors the current array of electors.
 * @param [mode='first'] an optional mode for selecting which events
 *          to find consensus on, see `findConsensus`.
 * @param logger the logger to use.
 *
 * @return a map with `consensus` and `yCandidates`; the `consensus` key's
 *   value is an array of objects with merge event `x` and `y` pairs where
 *   they refer to the same merge event when `mode` = 'first'; this can be
*    different for another mode.
 */
function _findMergeEventProof({
  ledgerNodeId, history, tails, blockHeight, electors, mode = 'first',
  logger = noopLogger
}) {
  //let startTime = Date.now();
  //logger.verbose('Start sync _findMergeEventProofCandidates');
  //console.log('Start sync _findMergeEventProofCandidates');
  const candidates = _findMergeEventProofCandidates({
    ledgerNodeId, tails, blockHeight, electors, mode, logger
  });
  /*console.log('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findMergeEventProofCandidates', {
    duration: Date.now() - startTime
  });*/

  if(!candidates) {
    // no `y` candidates yet, so safe to return early with no consensus
    return {consensus: []};
  }

  const {yByElector} = candidates;

  //startTime = Date.now();
  //console.log('Start sync _findConsensusMergeEventProof');
  //logger.verbose('Start sync _findConsensusMergeEventProof');
  const ys = _findConsensusMergeEventProof(
    {ledgerNodeId, yByElector, blockHeight, electors, logger});
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
    // pair `y`s with `x`s
    consensus: ys.map(y => {
      const x = y;
      let proof = [];
      if(proof.length === 0 && supermajority === 1) {
        // always include single elector as proof; enables continuity of that
        // single elector when computing electors in the next block via
        // quick inspection of `block.consensusProofHash`
        proof = [y];
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
 * @param [mode='first'] an optional mode for selecting which events
 *          to find consensus on, see `findConsensus`.
 * @param logger the logger to use.
 *
 * @return `null` or a map containing `yByElectors`, whereby each elector maps
 *   to merge event(s) `y` on its branch according to `mode`.
 */
function _findMergeEventProofCandidates({
  ledgerNodeId, tails, blockHeight, electors,
  mode = 'first', logger = noopLogger
}) {
  if(mode !== 'first') {
    throw new Error(`Unsupported mode "${mode}".`);
  }

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
  if(electorsWithTails.length < supermajority) {
    // non-consensus events from a supermajority of electors have not yet
    // been collected, so return early
    return null;
  }

  /* Algorithm:

  For each elector, find the earliest branch-native merge event `y`. There
  may be more than one `y` if the node is byzantine and has forked. */
  const yByElector = {};

  // Note: we need to allow multiple events for each elector, in the event that
  // they are byzantine -- we must allow this because we must calculate
  // support on remote merge events from their node's perspective, not our
  // own (i.e. we know a node is byzantine and has forked, but they may not
  // so they will calculate support accordingly)

  // set `y` merge events for each elector to tails
  for(const elector of electorsWithTails) {
    //console.log('FINDING y for', elector);
    // Note: must not skip electors with multiple tails detected (byzantine),
    // as not every valid node will see the fork and we must calculate their
    // support properly
    const electorTails = tails[elector];

    // `y` is the always the tail (oldest event) for a given elector
    //console.log('***y found for', elector, ' at generation ',
    //  result._generation, result);
    const copy = electorTails.slice();
    yByElector[elector] = copy;
    // mark each with `_y` property to indicate it is a little `y` event
    for(const e of copy) {
      e._y = true;
    }
  }

  // TODO: ensure logging `yByElector` is not slow
  /*logger.verbose('Continuity2017 y merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, yByElector});*/
  /*console.log('Continuity2017 y merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, yByElector});*/

  if(Object.keys(yByElector).length < supermajority) {
    // non-consensus events y from a supermajority of electors have not yet
    // been collected, so return early
    return null;
  }

  // compute most recent witness ancestors index
  _computeMostRecentWitnessAncestors({ledgerNodeId, yByElector});

  return {yByElector};
}

/**
 * This helper function determines the most recent events from each witness
 * and stores them in `event._index.mostRecentWitnessAncestor`.
 */
function _computeMostRecentWitnessAncestors({ledgerNodeId, yByElector}) {
  // TODO: change `mostRecentWitnessAncestors` to use a `Map`

  // walk witness tails to heads, computing most recent witness ancestors
  // for all events connected to a witness tail; this index enables finding
  // the events to use to calculate support and enables finding endorsements
  const yElectorSet = new Set(Object.keys(yByElector));
  let next = [].concat(...[...Object.values(yByElector)]);
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(event._index && event._index.mostRecentWitnessAncestors) {
        // already computed index
        continue;
      }

      // defer if any parent has not yet computed the index
      let defer = false;
      for(const parent of event._parents) {
        if(!(parent._index && parent._index.mostRecentWitnessAncestors)) {
          next.push(parent);
          defer = true;
        }
      }
      if(defer) {
        next.push(event);
        continue;
      }

      // determine if event is created by a witness
      const creator = _getCreator(event);
      const isWitness = yElectorSet.has(creator);

      // initialize recent ancestors index
      let mostRecentWitnessAncestors;
      if(!event._treeParent) {
        // no ancestors yet computed, first event on branch
        mostRecentWitnessAncestors = {};
      } else {
        // copy parent ancestors
        mostRecentWitnessAncestors =
          {...event._treeParent._index.mostRecentWitnessAncestors};
        // add parent as most recent ancestor if creator is a witness
        if(isWitness) {
          mostRecentWitnessAncestors[creator] = event._treeParent;
        }
      }
      if(!event._index) {
        event._index = {};
      }
      event._index.mostRecentWitnessAncestors = mostRecentWitnessAncestors;

      // process MRAs from each non-tree parent
      for(const parent of event._parents) {
        const parentCreator = _getCreator(parent);
        if(yElectorSet.has(parentCreator)) {
          // make sure to set ancestor using this function to catch forks
          // and handle unusual merges, do not try to optimize by setting
          // parent directly by assuming it is most recent (it may not be)
          _useMostRecentAncestorEvent({
            ledgerNodeId, elector: parentCreator,
            index: mostRecentWitnessAncestors, ancestorEvent: parent
          });
        }
        // no need to merge tree parent's MRWAs, they formed the basis
        // for our index above
        if(parent === event._treeParent) {
          continue;
        }
        const parentMRWAs = parent._index.mostRecentWitnessAncestors;
        for(const elector in parentMRWAs) {
          const ancestorEvent = parentMRWAs[elector];
          _useMostRecentAncestorEvent({
            ledgerNodeId, elector,
            index: mostRecentWitnessAncestors, ancestorEvent
          });
        }
      }

      // recurse through tree children if event was created by a witness
      if(event._treeChildren && isWitness) {
        next.push(...event._treeChildren);
      }
    }
  }
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
 *
 * @return `null` or an array of the earliest endorsement merge event(s)
 *           coupled with descendants maps: {mergeEvent, descendants}.
 */
function _findEndorsementMergeEvent({
  ledgerNodeId, x, electors, supermajority
}) {
  //console.log('EVENT', x.eventHash);

  const xCreator = _getCreator(x);
  const _endorsers = new Set([xCreator]);

  if(supermajority === 1) {
    // trivial case, return `x`
    x._endorsers = _endorsers;
    return [{mergeEvent: x, descendants: new Map()}];
  }

  if(!x._treeChildren) {
    // no tree children, so return nothing
    return null;
  }

  // walk from `x` through its tree children until an endorsement is found;
  // an endorsement event occurs on the branch of `x` when events from `2f+1`
  // (a supermajority) of the witnesses have `x` in their ancestry
  const results = [];
  const electorSet = new Set(electors.map(e => e.id));
  const endorsers = new Set([xCreator]);

  // must iterate through all possible forks on byzantine nodes as some nodes
  // may only see one side of the fork, this requires making copies for each
  // possible path through tree children
  let next = [];
  if(x._treeChildren.length === 1) {
    // no fork, no need to copy
    next.push({treeDescendant: x._treeChildren[0], endorsers});
  } else {
    // must copy endorsers and mrwa due to fork
    next.push(...x._treeChildren.map(treeDescendant => ({
      treeDescendant,
      endorsers: new Set(endorsers)
    })));
  }
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const {treeDescendant, endorsers} of current) {
      let found = false;
      for(const witness in treeDescendant._index.mostRecentWitnessAncestors) {
        if(endorsers.has(witness)) {
          // already counted this witness as an endorser, continue
          continue;
        }

        // get `ancestorEvent` and its most recent `x` ancestor
        const ancestorEvent =
          treeDescendant._index.mostRecentWitnessAncestors[witness];
        if(!ancestorEvent) {
          // byzantine fork detected, skip, node cannot endorse after forking
          continue;
        }
        const xAncestor =
          ancestorEvent._index.mostRecentWitnessAncestors[xCreator];

        // in order for `ancestorEvent` to endorse `x`, it must have an `x`
        // ancestor that is either `x` or that descends from `x`
        if(!(xAncestor && (xAncestor === x || _descendsFrom(xAncestor, x)))) {
          // `ancestorEvent` does not endorse `x`
          continue;
        }

        // new endorser found
        endorsers.add(witness);
        _endorsers.add(witness);
        // console.log('endorser', witness, ancestorEvent.eventHash);
        // console.log('endorsers', endorsers);
        // console.log('supermajority', supermajority);
        // console.log('electors', electorSet);
        if(endorsers.size >= supermajority) {
          found = true;
          //console.log('supermajority of endorsers found at generation',
          //  event._generation);
          //console.log();
          results.push({
            mergeEvent: treeDescendant,
            // FIXME: remove
            descendants: new Map()
          });
          break;
        }
      }

      // if endorsement merge event found or no more children, continue
      // to other tree descendant fork, if any
      if(found || !treeDescendant._treeChildren) {
        continue;
      }

      // endorsement merge event NOT found yet, add tree children
      if(treeDescendant._treeChildren.length === 1) {
        // no fork, no need to copy
        next.push(
          {treeDescendant: treeDescendant._treeChildren[0], endorsers});
      } else {
        // must copy endorsers due to fork
        next.push(...treeDescendant._treeChildren.map(treeDescendant => ({
          treeDescendant,
          endorsers: new Set(endorsers)
        })));
      }
    }
  }

  // save total endorsers of `x`
  x._endorsers = _endorsers;

  return results.length === 0 ? null : results;
}

function _findConsensusMergeEventProof({
  ledgerNodeId, yByElector, blockHeight, electors,
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
    ledgerNodeId, yByElector, blockHeight, electors, logger
  });
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
    index[elector] = false;
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
    const ancestorEvent = event._index.mostRecentWitnessAncestors[yElector];
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

  // event is a `y`, always use itself when voting
  if(event._y) {
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

function _tallyBranches({
  ledgerNodeId, yByElector, blockHeight, electors, logger = noopLogger
}) {
  /* Algorithm:

  1. Iterate through each influential elector branch, starting at its tail(s)
     and moving down its tree children. Influential electors include electors
     with Y events.
  2. Compute the most recent influential elector ancestors for the current
     event.
  3. If the event is a voting event, compute a `votes` map of elector => most
     recent event created by that elector, creating the set of events that are
     participating in an experiment to see what Y candidates the various nodes
     are supporting. If a byzantine node is detected, mark the elector's entry
     as `false` and it remains that way until consensus is reached. If some of
     the participants haven't computed their support yet, then defer and move
     to the next iteration of the loop. Eventually all ancestors will support
     a set and its support can then be computed -- or consensus will be reached
     and the loop will return early.
  4. Once a tree child has had its support computed, move onto the next tree
     child and continue until no more remain.
  */

  // initialize indexing of elector ancestors that must be tracked in order
  // to determine consensus; these influential electors are voting witnesses
  // (those with a `y` event)
  const yElectorSet = new Set(Object.keys(yByElector));
  const influentialElectorTails = new Map();
  for(const elector of yElectorSet) {
    influentialElectorTails.set(elector, yByElector[elector]);
  }
  const tailsArray = [].concat(...[...influentialElectorTails.values()]);

  // iterate through events from all influential electors, performing
  // computations and tallying toward consensus
  let next = tailsArray;
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      // if event is a voting event (i.e. its tree parent was a voting event or
      // it is a `y` by virtue of having a `_y` property), then determine its
      // voting ancestry
      if((event._treeParent && event._treeParent._votes) || event._y) {
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

      // if event is voting, compute its support
      if(event._votes) {
        /*const startTime = Date.now();
        logger.debug('Start sync _findConsensusMergeEventProof: ' +
          '_computeSupport');*/
        const result = _computeSupport(
          {ledgerNodeId, event, blockHeight, electors, logger});
        /*logger.debug('End sync _findConsensusMergeEventProof: ' +
          '_computeSupport', {
          duration: Date.now() - startTime
        });*/
        if(result) {
          // consensus reached
          return result;
        }
      }

      // add tree children, if any
      if(event._treeChildren) {
        next.push(...event._treeChildren);
      }
    }
  }

  return null;
}

function _computeSupport({
  ledgerNodeId, event, blockHeight, electors, logger = noopLogger
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
  */

  /* Endorsement rules:

  1. An event is an "endorsement event" if the events are on the same branch
     and if the endorsement event is the first event since the earlier event
     that includes >= `2f+1` electors in its ancestry that have the earlier
     event in their ancestry (these electors have "endorsed" the earlier event).
  2. A proposal is endorsed at its endorsement event only if support at the
     endorsement event matches support at the proposal (i.e., the proposal was
     never rejected).
  */

  const creator = _getCreator(event);

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
        endorsedProposalCount: 0
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
          endorsedProposalCount: 0
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
        endorsedProposalCount: 0
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
  }

  /*
  If you have an existing proposal, check rejection.
  */
  // get existing proposal on the event's branch
  let proposal = event._treeParent ? event._treeParent._proposal : null;
  if(proposal) {
    // reject proposal if local support does not match
    const reject = !_compareSupportSet(proposal._supporting, nextChoice.set);
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
      // ensure count is updated
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
  proposal, create a new one.
  */
  const hasSupermajority = nextChoice.count >= supermajority;
  if(hasSupermajority && !proposal) {
    // no proposal yet, use current event
    proposal = event;
    _computeProposalEndorsement({
      ledgerNodeId, event, electors, supermajority
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
  ledgerNodeId, event, electors, supermajority
}) {
  if(event._proposalEndorsement) {
    // already computed
    return;
  }

  if(event._electorEndorsement !== undefined) {
    // reuse previously computed endorsements
    const endorsement = event._electorEndorsement;
    if(endorsement === false) {
      event._proposalEndorsement = false;
      return;
    }
    // ensure `endorsesKey` is updated
    for(const e of endorsement) {
      if(e._endorsesProposal) {
        e._endorsesProposal.push(event);
      } else {
        e._endorsesProposal = [event];
      }
    }
    event._proposalEndorsement = endorsement;
    return;
  }

  // `endorsement` must be computed
  const endorsement = _findEndorsementMergeEvent({
    ledgerNodeId, x: event, electors, supermajority
  });
  if(endorsement) {
    // `endorsement` will be an array as byzantine nodes may fork, but forks
    // will get detected via endorse point(s) due to overlap of at least
    // one functioning node
    const allEndorsements = [];
    for(const {mergeEvent: e} of endorsement) {
      if(e._endorsesProposal) {
        e._endorsesProposal.push(event);
      } else {
        e._endorsesProposal = [event];
      }
      /*console.log(
        `created proposal endorse point for`, event._generation,
        'at', e._generation);*/
      allEndorsements.push(e);
    }
    event._electorEndorsement = event._proposalEndorsement = allEndorsements;
  } else {
    // no endorsement of given type in DAG yet for `event`
    event._electorEndorsement = event._proposalEndorsement = false;
  }
}

function _getPriorityPeers({
  ledgerNodeId, history, tails, blockHeight, electors, mode, logger
}) {
  /* Algorithm:

  Determine which electors could help progress consensus by creating a
  new merge event.

  For each elector, determine if it needs an endorsement for a decision
  proposal. If so, track whether any other elector can endorse and see if
  there are enough to reach a supermajority.

  For each elector, go through each of the other elector's heads and see if
  they are new (not in the elector's ancestry) and, if so, if creating a
  new merge event would either provide an endorsement they need or if their
  new support has changed in a way that would influence a new merge event.

  If creating a new merge event would be helpful, prioritize the elector.
  */
  const supermajority = api.supermajority(electors.length);
  const f = api.maximumFailures(electors.length);

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
  for(const [elector, head] of heads) {
    // skip any peer that is already prioritized
    if(priorityPeers.has(elector)) {
      continue;
    }

    // determine if `elector` needs an endorsement for a proposal...
    // this to test whether or not merging each other head could
    // progress consensus
    let proposalEndorsers = null;
    if(head._proposal && !_hasEndorsedProposal(head)) {
      proposalEndorsers = head._proposal._endorsers;
    }

    // check other heads to determine if merging them in would be beneficial
    for(const [otherElector, otherHead] of heads) {
      // can't help by merging self
      if(otherElector === elector) {
        continue;
      }

      // if `elector` has a previous MRA from `otherElector`, see if there is
      // an important change to merge in
      const mra = head._index && head._index.mostRecentWitnessAncestors &&
        head._index.mostRecentWitnessAncestors[otherElector];
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

      // if `otherHead` has new endorsed proposal, then prioritize to merge
      // it in
      if(!_hasEndorsedProposal(mra) && _hasEndorsedProposal(otherHead)) {
        priorityPeers.add(elector);
        break;
      }

      // if `otherHead` has a new proposal vs mra, then prioritize
      // `elector` to merge it in
      if(mra._proposal !== otherHead._proposal) {
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

      // if `otherHead` has a proposal that `elector` can endorse, then
      // prioritize `elector`
      if(otherHead._proposal && otherHead._proposal._endorsers &&
        !_hasEndorsedProposal(otherHead) &&
        !otherHead._proposal._endorsers.has(elector)) {
        priorityPeers.add(elector);
        break;
      }

      // if `elector` needs a proposal endorsement and `otherElector` hasn't
      // provided it, track it to check for threshold later
      if(proposalEndorsers &&
        !proposalEndorsers.has(otherElector) &&
        _hasElectorAncestor({
          event: otherHead,
          elector,
          ancestor: head._proposal
        })) {
        proposalEndorsers.add(otherElector);
      }
    }

    // if proposal endorsers would reach threshold, then prioritize `elector`
    if(proposalEndorsers && proposalEndorsers.size >= supermajority) {
      priorityPeers.add(elector);
    }
  }

  if(priorityPeers.size <= f) {
    // we must guarantee there will always be > `f` witnesses in
    // `priorityPeers` or else it is possible that all prioritized
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
    event._index.mostRecentWitnessAncestors[elector];
  return mra && mra._generation >= ancestor._generation;
}
