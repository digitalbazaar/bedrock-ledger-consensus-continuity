/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
/* eslint-disable no-unused-vars */
'use strict';

const _ = require('lodash');
const noopLogger = require('./noopLogger');

// module API
const api = {};
module.exports = api;

// exposed for testing
api._getTails = _getTails;
api._findConsensusSet = _findConsensusSet;

/**
 * Determine if any new merge events have reached consensus in the given
 * history summary of merge events w/o consensus.
 *
 * @param ledgerNodeId the ID of the local ledger node.
 * @param history recent history rooted at the ledger node's local branch
 *   including ONLY merge events, it must NOT include local regular
 *   events.
 * @param witnesses the current witnesses.
 * @param [state=null] an optional object for storing state information that
 *   can be reused for the same `blockHeight`.
 * @param logger the logger to use.
 *
 * @return a result object with the following properties:
 *   consensus: `true` if consensus has been found, `false` if` not.
 *   eventHashes: the hashes of events that have reached consensus.
 *   consensusProofHashes: the hashes of events endorsing `eventHashes`.
 *   priorityPeers: if consensus is `false`, an array of peer
 *     IDs identifying the peers that may help achieve consensus most
 *     readily.
 *   creators: the witnesses that participated in events that reached
 *     consensus.
 */
api.findConsensus = ({
  ledgerNodeId, history, blockHeight, witnesses, state = null,
  logger = noopLogger
}) => {
  // initialize/validate state as needed
  if(state) {
    if(!state.init) {
      state.init = true;
      state.eventMap = null;
      // guard against misusing state for the wrong block height
      state.blockHeight = blockHeight;
      // maps that enable lookups/reverse lookups of pairs of `eventHash` and
      // memo containing the hash and a unique identifier (Symbol) to enable
      // caches to share memory
      state.hashToMemo = new Map();
      state.symbolToMemo = new Map();
      // cached support sets for events, key'd by memo Symbol associated with
      // the event's hash
      state.supportCache = new Map();
    } else if(state.blockHeight !== blockHeight) {
      throw new Error(
        `Given "state" is blockHeight "${state.blockHeight}", ` +
        `not expected blockHeight "${blockHeight}".`);
    }

    // prevent V8 wrongMap deoptimizations triggered by hidden class transitions
    Object.seal(state);
  }

  //logger.verbose('Start sync _getTails', {witnesses});
  //let startTime = Date.now();
  const {tails, witnessTails} = _getTails({history, witnesses, state});
  /*logger.verbose('End sync _getTails', {
    duration: Date.now() - startTime
  });*/
  //logger.verbose('Start sync _findConsensusSet');
  //console.log('Start sync _findConsensusSet');
  //startTime = Date.now();
  const result = _findConsensusSet({
    ledgerNodeId, history, tails, witnessTails, blockHeight, witnesses, state,
    logger
  });
  /*console.log('End sync _findConsensusSet', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findConsensusSet', {
    duration: Date.now() - startTime
  });*/

  if(state) {
    // clear event map as it cannot be reused and we want to allow the memory
    // to be reclaimed; do not preserve after `_findConsensusSet`
    state.eventMap = null;
  }

  // consensus not found
  if(result.consensus.length === 0) {
    if(state) {
      // cache support for future reuse
      for(const event of history.events) {
        _cacheSupport({state, event});
      }
    }

    // calculate priority peer information to report
    const priorityPeers = _getPriorityPeers({
      ledgerNodeId, history, witnessTails, blockHeight,
      witnesses, logger
    });

    //logger.verbose('findConsensus no result found, exiting');
    return {consensus: false, priorityPeers};
  }
  //logger.verbose('findConsensus result found, proceeding...');

  // gather events that have achieved consensus; consider `x` events to have
  // achieved consensus (these are the same as `y` events anyway)
  const events = [];
  for(const p of result.consensus) {
    events.push(p.x);
  }
  // consensus proof hashes represent the consensus set
  const creators = new Set();
  const consensusProofHashes = [];
  for(const e of events) {
    creators.add(_getCreator(e));
    consensusProofHashes.push(e.eventHash);
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
 * Calculate a supermajority of witnesses (`2f+1`). When witnesses <= 3,
 * every witness must agree.
 *
 * @param witnessCount the total number of witnesses.
 *
 * @return the number of witnesses that constitute a supermajority.
 */
api.supermajority = witnessCount => (witnessCount <= 3) ?
  witnessCount : Math.floor(witnessCount / 3) * 2 + 1;

/**
 * Determines the maximum number of failed witnesses. There are always either
 * `3f+1` witnesses or 1 witness. When there are `3f+1` witnesses, there can
 * be `f` failures. When there is 1 witness, the maximum number of failures
 * is zero.
 *
 * @param witnessCount the total number of witnesses.
 *
 * @return the maximum number of witnesses that can fail (`f`).
 */
api.maximumFailures = witnessCount => (witnessCount === 1) ?
  0 : ((witnessCount - 1) / 3);

/**
 * Converts the given view of history from one particular ledger node's
 * perspective into the views for each of the nodes (including both
 * witnesses and non-witnesses).
 *
 * @param history recent history.
 * @param witnesses the current witnesses.
 * @param [state=null] an optional object for storing state information that
 *   can be reused for the same `blockHeight`.
 *
 * @return a map containing {tails, witnessTails} where each is a map with
 *   creator => an array containing the node's branch of history starting at
 *     its earliest merge event (tails has all nodes, witnessTails contains
 *     only witnesses), i.e. the array contains the tail event created by the
 *     witness (but an array is used because there may be more than tail, to
 *     account for byzantine behavior).
 */
function _getTails({history, witnesses, state = null}) {
  if(history === undefined || witnesses === undefined) {
    throw new TypeError('`history` and `witnesses` are required.');
  }

  const witnessTails = new Map();
  const witnessSet = new Set(witnesses);

  // build history links
  const eventMap = new Map();
  if(state) {
    state.eventMap = eventMap;
  }
  for(const e of history.events) {
    // initialize consensus information for event
    e._c = {
      // basic navigation
      parents: [],
      generation: 0,
      head: null,
      headGeneration: 0,
      treeParent: null,
      treeChildren: [],

      // support
      decision: false,
      endorsers: null,
      endorsesProposal: null,
      proposal: null,
      proposalEndorsed: false,
      proposalEndorsement: null,
      mostRecentWitnessAncestors: null,
      support: null,
      witness: witnessSet.has(_getCreator(e)),
      y: false,

      // consensus order
      hashBuffer: null,
      consensusSortHash: null,
      externalParents: null
    };

    // prevent V8 wrongMap deoptimizations triggered by hidden class transitions
    Object.seal(e._c);

    eventMap.set(e.eventHash, e);
  }
  for(const e of history.events) {
    for(const parentHash of e.event.parentHash) {
      const parent = eventMap.get(parentHash);
      if(!parent) {
        continue;
      }
      e._c.parents.push(parent);
    }
  }

  // find all tails and build `treeParent` index
  const tails = new Map();
  for(const e of history.events) {
    // find parent from the same branch
    const treeHash = e.event.treeHash;
    for(const parent of e._c.parents) {
      if(parent.eventHash === treeHash) {
        e._c.treeParent = parent;
        break;
      }
    }
    if(e._c.treeParent) {
      e._c.treeParent._c.treeChildren.push(e);
    } else {
      // event has no tree parent, so it is a tail (the earliest event in
      // recent history created by the witness)
      const creator = _getCreator(e);
      const tail = tails.get(creator);
      if(tail) {
        // note that there is only one tail for correct nodes but we must
        // account here for byzantine nodes reporting more than one
        tail.push(e);
      } else {
        tails.set(creator, [e]);
      }
    }
  }

  // set generations for each branch and set witness tails
  for(const [creator, tail] of tails) {
    let generation = 1;
    let next = tail;
    let head = null;
    while(next.length > 0) {
      const current = next;
      next = [];
      for(const event of current) {
        event._c.generation = generation;
        next.push(...event._c.treeChildren);
      }
      generation++;
      if(next.length === 0) {
        head = current;
      }
    }
    if(witnessSet.has(creator)) {
      witnessTails.set(creator, tail);
    }
    for(const e of tail) {
      e._c.headGeneration = generation - 1;
      e._c.head = head;
    }
  }

  return {tails, witnessTails};
}

// helper that gets all hashes for the given events and their ancestors
function _getAncestorHashes({events, ledgerNodeId}) {
  // get all ancestor hashes from every consensus X merge event; track
  // known merge event hashes separately from parent hashes that could
  // either be regular events that are also achieving consensus now or merge
  // events that previously achieved consensus (these merge events merely
  // represent where this latest set of events connects with the consensus DAG)
  // ... or regular events that already achieved consensus from a forked peer
  const mergeEventHashes = new Set();
  const parentHashes = new Set();
  const hashBuffers = [];
  const mergeEvents = [];
  let next = events;
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const event of current) {
      if(mergeEventHashes.has(event.eventHash)) {
        // already added `event`
        continue;
      }
      mergeEventHashes.add(event.eventHash);
      mergeEvents.push(event);
      // save merge event buffer
      event._c.hashBuffer = _parseHash(event.eventHash);
      hashBuffers.push(event._c.hashBuffer);
      // push merge event parents for additional processing
      next.push(...event._c.parents);
      // add potential regular event parents
      // Note: We may want to move this process outside of this module
      // in the future; if so, we should return the `baseHashBuffer` so
      // an external process can properly sort the regular events.
      for(const parentHash of event.event.parentHash) {
        // skip tree parent, it cannot be a regular event and skip any known
        // merge events
        if(event.event.treeHash === parentHash ||
          mergeEventHashes.has(parentHash) ||
          parentHashes.has(parentHash) ||
          event._c.parents.some(p => p.eventHash === parentHash)) {
          continue;
        }
        // this event is either a regular event that is just now achieving
        // consensus or an event that has previously reached consensus,
        // we can't tell here but will check this later
        parentHashes.add(parentHash);
        // track all possible regular events so they can be added to the
        // total consensus set in the proper order; the event will be removed
        // from the set of consensus events later if it is actually an event
        // that has previously achieved consensus
        const externalParent = {
          eventHash: parentHash,
          _c: {hashBuffer: null, consensusSortHash: null}
        };
        if(!event._c.externalParents) {
          event._c.externalParents = [externalParent];
        } else {
          event._c.externalParents.push(externalParent);
        }
      }
    }
  }

  // Note: Sorting is performed here in `continuity.js` instead of in
  // `consensus.js` to reduce the amount of data that must be copied between
  // processes if a workerpool is used. If the workerpool implementation is
  // removed, then this sorting could be moved outside of this module and
  // some complexity could be potentially reduced with respect to processing
  // `externalParents`.

  // compute base hash for mixin for sorting and then sort merge events
  // into a total deterministic ordering for the block
  const baseHashBuffer = _createBaseHashBuffer(hashBuffers);
  const blockOrdering = _sortIntoBlockOrder({mergeEvents, baseHashBuffer});

  return {
    mergeEventHashes: [...mergeEventHashes],
    parentHashes: [...parentHashes],
    blockOrdering
  };
}

/**
 * Find consensus `y` merge events. The `y` merge events are the first (or
 * "tail" or "oldest") events from each of the witnesses.
 *
 * @param ledgerNodeId the ID of the local ledger node.
 * @param history recent history rooted at the ledger node's local branch
 *   including ONLY merge events, it must NOT include local regular events.
 * @param tails the tails (earliest ancestry) of linked recent history, indexed
 *   by node ID (event creator).
 * @param witnessTails the tails (earliest ancestry) of linked recent history,
 *   indexed by witness ID (event creator that is a witness).
 * @param witnesses the current array of witnesses.
 * @param [state=null] an optional object for storing state information that
 *   can be reused for the same `blockHeight`.
 * @param logger the logger to use.
 *
 * @return a map with `consensus` and `yCandidates`; the `consensus` key's
 *   value is an array of objects with merge event `x` and `y` pairs where
 *   they refer to the same merge event.
 */
function _findConsensusSet({
  ledgerNodeId, history, tails, witnessTails,
  blockHeight, witnesses, state = null, logger = noopLogger
}) {
  //let startTime = Date.now();
  //logger.verbose('Start sync _findConsensusCandidates');
  //console.log('Start sync _findConsensusCandidates');
  const candidates = _findConsensusCandidates({
    ledgerNodeId, witnessTails, blockHeight, witnesses, logger
  });
  /*console.log('End sync _findConsensusCandidates', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findConsensusCandidates', {
    duration: Date.now() - startTime
  });*/

  if(!candidates) {
    // no `y` candidates yet, so safe to return early with no consensus
    return {consensus: [], yCandidates: []};
  }

  const {yByWitness} = candidates;

  //startTime = Date.now();
  //console.log('Start sync _findConsensusMergeEvents');
  //logger.verbose('Start sync _findConsensusMergeEvents');
  const ys = _findConsensusMergeEvents({
    ledgerNodeId, yByWitness, tails, blockHeight, witnesses, state,
    logger
  });
  /*console.log('End sync _findConsensusMergeEvents', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _findConsensusMergeEvents', {
    duration: Date.now() - startTime
  });*/
  if(ys.length === 0) {
    // no consensus yet
    return {consensus: [], yCandidates: []};
  }

  const supermajority = api.supermajority(witnesses.length);
  return {
    // pair `y`s with `x`s
    consensus: _findConsensusMergeEventPairs({ys, supermajority}),
    // return all yCandidates for debugging purposes
    yCandidates: [].concat(...[...yByWitness.values()])
  };
}

/**
 * Find the next `y` merge events that are candidates for consensus.
 *
 * @param ledgerNodeId the ID of the local ledger node.
 * @param witnessTails the tails (earliest ancestry) of linked recent history,
 *    indexed by witness ID.
 * @param witnesses the current set of witnesses.
 * @param logger the logger to use.
 *
 * @return `null` or a map containing `yByWitness`, whereby each witness maps
 *   to merge event(s) `y` on its branch.
 */
function _findConsensusCandidates({
  ledgerNodeId, witnessTails, blockHeight, witnesses, logger = noopLogger
}) {
  const supermajority = api.supermajority(witnesses.length);

  //console.log('TAILS', util.inspect(tails, {depth:10}));

  // TODO: ensure logging `witnessesWithTails` is not slow
  /*logger.verbose('Continuity2017 witnesses with tails for ledger node ' +
    ledgerNodeId + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNodeId, witnessesWithTails});*/
  /*console.log('Continuity2017 witnesses with tails for ledger node ' +
    ledgerNodeId + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNodeId, witnessesWithTails});*/
  if(witnessTails.size < supermajority) {
    // non-consensus events from a supermajority of witnesses have not yet
    // been collected, so return early
    return null;
  }

  /* Algorithm:

  For each witness, find the earliest branch-native merge event `y`. There
  may be more than one `y` if the node is byzantine and has forked. */
  const yByWitness = new Map();

  // Note: we need to allow multiple events for each witness, in the event that
  // they are byzantine -- we must allow this because we must calculate
  // support on remote merge events from their node's perspective, not our
  // own (i.e. we know a node is byzantine and has forked, but they may not
  // so they will calculate support accordingly)

  // set `y` merge events for each witness to tails
  // Note: This conversion from a map to an object and then getting its keys
  // is *intentional* -- tests have shown this to iterate significantly faster
  // than iterating over the map directly
  const witnessesWithTails = Object.keys(Object.fromEntries(witnessTails));
  for(const witness of witnessesWithTails) {
    //console.log('FINDING y for', witness);
    // Note: must not skip witnesses with multiple tails detected (byzantine),
    // as not every valid node will see the fork and we must calculate their
    // support properly

    // `y` is the always the tail (oldest event) for a given witness
    //console.log('***y found for', witness, ' at generation ',
    //  result._c.generation, result);
    const tails = witnessTails.get(witness);
    const copy = tails.slice();
    yByWitness.set(witness, copy);
    // mark each with `_c.y` property to indicate it is a little `y` event
    for(const e of copy) {
      e._c.y = true;
    }
  }

  // TODO: ensure logging `yByWitness` is not slow
  /*logger.verbose('Continuity2017 y merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, yByWitness});*/
  /*console.log('Continuity2017 y merge events found for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId, yByWitness});*/

  if(yByWitness.size < supermajority) {
    // non-consensus events y from a supermajority of witnesses have not yet
    // been collected, so return early
    return null;
  }

  return {yByWitness};
}

/**
 * This helper function determines the most recent events from each witness
 * and stores them in `event._c.mostRecentWitnessAncestor`.
 */
function _computeMostRecentWitnessAncestors({ledgerNodeId, tails}) {
  // walk tails to heads, computing most recent witness ancestors for all
  // events; this index enables finding the events to use to calculate support
  // and enables finding endorsements; non-witnesses are walked as well as
  // it is the quickest way to build the index
  // Note: Flattening using this approach has been tested here and is faster
  // than doing `[].concat(...[...tails.values()])`
  let next = [].concat(...Object.values(Object.fromEntries(tails)));
  while(next.length > 0) {
    const current = new Set(next);
    next = [];
    for(const event of current) {
      if(event._c.mostRecentWitnessAncestors) {
        // already computed index
        continue;
      }

      // defer if any parent has not yet computed the index
      let defer = false;
      for(const parent of event._c.parents) {
        if(!parent._c.mostRecentWitnessAncestors) {
          next.push(parent);
          defer = true;
        }
      }
      if(defer) {
        next.push(event);
        continue;
      }

      // initialize recent ancestors index
      let mostRecentWitnessAncestors;
      if(!event._c.treeParent) {
        // no ancestors yet computed, first event on branch
        mostRecentWitnessAncestors = new Map();
      } else {
        // shallow copy parent ancestors
        mostRecentWitnessAncestors = new Map(
          event._c.treeParent._c.mostRecentWitnessAncestors);
        // add parent as most recent ancestor if creator is a witness
        if(event._c.witness) {
          mostRecentWitnessAncestors.set(
            _getCreator(event), event._c.treeParent);
        }
      }
      event._c.mostRecentWitnessAncestors = mostRecentWitnessAncestors;

      // process MRAs from each non-tree parent
      for(const parent of event._c.parents) {
        if(parent._c.witness) {
          // make sure to set ancestor using this function to catch forks
          // and handle unusual merges, do not try to optimize by setting
          // parent directly by assuming it is most recent (it may not be)
          _useMostRecentAncestorEvent({
            ledgerNodeId, witness: _getCreator(parent),
            index: mostRecentWitnessAncestors, ancestorEvent: parent
          });
        }
        // no need to merge tree parent's MRWAs, they formed the basis
        // for our index above
        if(parent === event._c.treeParent) {
          continue;
        }
        const parentMRWAs = parent._c.mostRecentWitnessAncestors;
        for(const [witness, ancestorEvent] of parentMRWAs) {
          _useMostRecentAncestorEvent({
            ledgerNodeId, witness,
            index: mostRecentWitnessAncestors, ancestorEvent
          });
        }
      }

      // recurse through tree children if present
      if(event._c.treeChildren.length > 0) {
        next.push(...event._c.treeChildren);
      }
    }
  }
}

function _findConsensusMergeEvents({
  ledgerNodeId, yByWitness, tails, blockHeight, witnesses, state = null,
  logger = noopLogger
}) {
  /*logger.verbose(
    'Continuity2017 looking for consensus merge events for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId});*/
  /*console.log(
    'Continuity2017 looking for consensus merge events for ledger node ' +
    ledgerNodeId, {ledgerNode: ledgerNodeId});*/

  // if witnesses is 1, consensus is trivial
  if(witnesses.length === 1) {
    return [].concat(...[...yByWitness.values()]);
  }

  // compute most recent witness ancestors index
  _computeMostRecentWitnessAncestors({ledgerNodeId, tails});

  //let startTime = Date.now();
  //logger.verbose('Start sync _runExperiments');
  //console.log('Start sync _runExperiments');
  // go through each y's branch looking for consensus
  const consensus = _runExperiments({
    ledgerNodeId, yByWitness, blockHeight, witnesses, state, logger
  });
  /*console.log('End sync _runExperiments', {
    duration: Date.now() - startTime
  });*/
  /*logger.verbose('End sync _runExperiments', {
    duration: Date.now() - startTime
  });*/

  if(consensus) {
    // TODO: ensure logging `consensus` is not slow
    /*logger.verbose(
      'Continuity2017 merge events found for ledger node ' +
      ledgerNodeId, {ledgerNode: ledgerNodeId, consensus});*/
    /*console.log(
      'Continuity2017 merge events found for ledger node ' +
      ledgerNodeId, {ledgerNode: ledgerNodeId, consensus});*/
  }

  return consensus || [];
}

function _useMostRecentAncestorEvent({
  ledgerNodeId, witness, index, ancestorEvent,
  logger = noopLogger
}) {
  // only count most recent ancestor event from a particular witness; if
  // an witness has two events from the same generation or different
  // generations where the younger does not descend from the older, then the
  // witness is byzantine and their tracking info will be invalidated
  const existing = index.get(witness);
  if(existing === undefined) {
    /*logger.verbose('Continuity2017 found new most recent ancestor event', {
      ledgerNode: ledgerNodeId,
      witness,
      ancestorEvent: ancestorEvent.eventHash
    });*/
    index.set(witness, ancestorEvent);
    return;
  }

  if(existing === false || ancestorEvent === false) {
    logger.warning('Continuity2017 detected byzantine node fork',
      {ledgerNode: ledgerNodeId, witness});
    index.set(witness, false);
    return;
  }

  // ensure ancestor events of the same generation are the same event
  if(ancestorEvent._c.generation === existing._c.generation) {
    if(ancestorEvent !== existing) {
      // byzantine node!
      logger.warning('Continuity2017 detected byzantine node fork', {
        ledgerNode: ledgerNodeId,
        witness,
        ancestorEvent: ancestorEvent.eventHash
      });
      index.set(witness, false);
    }
    // ancestor events match, nothing to do
    return;
  }

  // ensure new ancestor event and existing ancestor event do not have forked
  // ancestry by ensuring younger generation descends from older
  let older;
  let younger;
  if(ancestorEvent._c.generation > existing._c.generation) {
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
      witness,
      ancestorEvent: ancestorEvent.eventHash
    });
    index.set(witness, false);
    return;
  }

  /*logger.verbose('Continuity2017 replacing most recent ancestor event', {
    ledgerNode: ledgerNodeId,
    witness,
    ancestorEvent: ancestorEvent.eventHash
  });*/
  index.set(witness, younger);
}

function _descendsFrom(younger, older) {
  let difference = younger._c.generation - older._c.generation;
  let parent = younger;
  while(parent && difference > 0) {
    parent = parent._c.treeParent;
    difference--;
  }
  return parent === older;
}

function _runExperiments({
  ledgerNodeId, yByWitness, blockHeight, witnesses, state = null,
  logger = noopLogger
}) {
  /* Algorithm:

  1. Iterate through each influential witness branch, starting at its tail(s)
     and moving down its tree children. Influential witnesses include witnesses
     with `y` events.
  2. If the event is a witness event, use the `mostRecentWitnessAncestors`
     index to find the set of events that are participating in an experiment to
     see what each is supporting. If a byzantine node is detected, the event
     at which it is detected will be marked as `false` and its support will
     be marked as `false`. This (lack of) support will remain that way until
     consensus is reached. If some of the participants (the event's witness
     ancestor events) haven't computed their support yet, then defer the
     current event and move to the next iteration of the loop. Eventually all
     ancestors will support a set and the current event's support can then be
     computed -- or consensus will be reached and the loop will return early.
  3. Once a tree child has had its support computed, move onto the next tree
     child and continue until no more remain.
  */

  // initialize indexing of witness ancestors that must be tracked in order
  // to determine consensus; these influential witnesses have a `y` event
  const tailsArray = [].concat(...[...yByWitness.values()]);

  // keep a set of support set instances
  const supportSets = new Set();

  // iterate through events from all influential witnesses, performing
  // computations and tallying toward consensus
  let next = tailsArray;
  while(next.length > 0) {
    const current = new Set(next);
    next = [];
    for(const event of current) {
      // if some witness ancestors haven't computed support yet, defer
      let defer = false;
      for(const e of event._c.mostRecentWitnessAncestors.values()) {
        if(e && e._c.support === null) {
          next.push(event);
          defer = true;
          break;
        }
      }
      if(defer) {
        continue;
      }

      /*const startTime = Date.now();
      logger.debug('Start sync _experiment');*/
      const result = _experiment({
        ledgerNodeId, event, blockHeight, witnesses, supportSets, state,
        logger
      });
      /*logger.debug('End sync _experiment', {
        duration: Date.now() - startTime
      });*/
      if(result) {
        // consensus reached
        return result;
      }

      // add tree children, if any
      if(event._c.treeChildren.length > 0) {
        next.push(...event._c.treeChildren);
      }
    }
  }

  return null;
}

function _experiment({
  ledgerNodeId, event, blockHeight, witnesses, supportSets,
  state = null, logger = noopLogger
}) {
  // support already chosen for event
  if(event._c.support !== null) {
    return null;
  }

  /* Choosing support:

  First, if the event's previous self-support was `false` then the event is
  byzantine and there is nothing to compute, its support has been revoked.

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
     that includes >= `2f+1` witnesses in its ancestry that have the earlier
     event in their ancestry (these witnesses have "endorsed" the earlier
     event).
  2. A proposal is endorsed at its endorsement event only if support at the
     endorsement event matches support at the proposal (i.e., the proposal was
     never rejected).
  */

  const creator = _getCreator(event);

  // if event is from a node that has been detected as byzantine, do not
  // bother computing its support
  const previousSupportEvent = event._c.mostRecentWitnessAncestors.get(creator);
  if(previousSupportEvent === false) {
    event._c.support = false;
    return null;
  }

  // compute maximum failures `f` and supermajority `2f+1` thresholds
  const f = api.maximumFailures(witnesses.length);
  const fPlusOne = f + 1;
  const supermajority = api.supermajority(witnesses.length);

  // tally support seen
  const {tally, fPlusOneSupport} = _tallySupport({event, f, fPlusOne});

  // choose support
  const nextChoice = _chooseSupport(
    {event, tally, supportSets, fPlusOneSupport, state});

  /*console.log('SUPPORT at ', event.eventHash, 'IS FOR',
    nextChoice.set.map(e => e._c.generation));*/

  /*
    If the previous support is different from the new support, update the
    next choice count. For initial `y` events, there is no previous support
    so this is always true.
  */
  const switched = !(previousSupportEvent && previousSupportEvent._c.support &&
    nextChoice.set === previousSupportEvent._c.support);
  if(switched) {
    // increment next choice
    nextChoice.count++;
  }

  /*
  If you have an existing proposal, check rejection.
  */
  // get existing proposal on the event's branch
  let proposal = event._c.treeParent ? event._c.treeParent._c.proposal : null;
  if(proposal) {
    // reject proposal if local support does not match
    const reject = proposal._c.support !== nextChoice.set;
    if(reject) {
      // Note: No need to find tally result for the set the proposal supports
      // to adjust it... because it is not used (only `nextChoice` is used)
      proposal = null;
    }
  }

  if(event._c.endorsesProposal && proposal && !proposal._c.proposalEndorsed) {
    // proposal endorse point reached...

    /* Note: Only mark proposal endorsed if it matches. This prevents
    having to remove the proposal if it was rejected and allows for debugging
    to see that a proposal was not marked endorsed at its endorse point
    because it was rejected. */
    if(event._c.endorsesProposal.includes(proposal)) {
      proposal._c.proposalEndorsed = true;
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
    event._c.decision = true;
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
    _computeProposalEndorsement(
      {ledgerNodeId, event, witnesses, supermajority});
  }

  // set event's proposal
  if(proposal) {
    event._c.proposal = proposal;
  }

  // support next choice
  event._c.support = nextChoice.set;
  return null;
}

function _tallySupport({event, f, fPlusOne}) {
  /*logger.verbose('Continuity2017 _tallySupport finding support seen...',
    {ledgerNode: ledgerNodeId, eventHash: event.eventHash});*/
  // tally support seen
  /*const startTime = Date.now();
  logger.debug('Start sync _tallySupport');*/
  const tally = new Map();
  const fPlusOneSupport = new Set();
  for(const e of event._c.mostRecentWitnessAncestors.values()) {
    if(e === false) {
      // do not count byzantine nodes
      continue;
    }

    // find existing result to update that matches support set
    let tallyResult = tally.get(e._c.support);
    if(!tallyResult) {
      // no matching result found, initialize new result
      tallyResult = {
        set: e._c.support,
        count: 0,
        endorsedProposalCount: 0
      };
      tally.set(tallyResult.set, tallyResult);
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
  /*logger.debug('End sync _tallySupport', {
    duration: Date.now() - startTime
  });*/
  /*console.log('tally', [...tally.values()].map(t => ({
    set: t.set.map(e => e._c.generation),
    count: t.count,
    epc: t.endorsedProposalCount
  })));*/

  /*console.log('==================');
  console.log('BLOCK HEIGHT', blockHeight);
  console.log('support received at generation', event._c.generation);
  console.log('by experimenter', creator.substr(-5));
  console.log('------------------');
  for(const [k, v] of event._c.mostRecentWitnessAncestors) {
    if(v._c.support) {
      console.log('|');
      console.log('|-witness:', k.substr(-5));
      console.log('  generation:', v._c.generation);
      v._c.support.forEach(r => {
        console.log(
          '    Y generation:', r._c.generation,
          ', creator:', _getCreator(r).substr(-5));
      });
    }
  }
  console.log('------------------');*/

  return {tally, fPlusOneSupport: [...fPlusOneSupport]};
}

function _chooseSupport({event, tally, supportSets, fPlusOneSupport, state}) {
  /* Choose support:
    1. If >= `f+1` support for any same set, union all sets with >= `f+1`
       support.
    2. Otherwise, union support.
  */
  if(fPlusOneSupport.length === 1) {
    // Note: Part of support branch 1. (union `f+1` support sets)
    // compute union of all sets with >= `f+1` support
    // optimize for single set
    return fPlusOneSupport[0];
  }

  // use cached support if available
  let set = _getCachedSupportSet({state, event, supportSets});
  if(!set) {
    // either union `f+1` sets or all sets...
    let union;
    if(fPlusOneSupport.length > 1) {
      // Note: Part of support branch 1. (union `f+1` support sets)
      // compute union of all sets with >= `f+1` support
      // union multiple sets (at most 2 sets, since `3f+1 - (f+1)*2 = f-1`)
      union = new Set(fPlusOneSupport[0].set);
      for(const e of fPlusOneSupport[1].set) {
        union.add(e);
      }
    } else {
      // Note: Part of support branch 2. (union all support sets)
      // compute the union of all support
      union = new Set();
      for(const supportEvent of event._c.mostRecentWitnessAncestors.values()) {
        if(!supportEvent) {
          // do not count support from byzantine nodes
          continue;
        }
        // add computed support from `supportEvent`
        for(const e of supportEvent._c.support) {
          union.add(e);
        }
      }
      // if event is an initial `y`, include it as supporting itself
      if(event._c.y) {
        union.add(event);
      }
    }

    // get support set instance that represents union
    set = _getSupportSet(supportSets, union);
  }

  // set the next choice to an existing choice or create a new one
  const nextChoice = tally.get(set);
  if(nextChoice) {
    return nextChoice;
  }

  // create new choice
  return {
    set,
    count: 0,
    endorsedProposalCount: 0
  };
}

function _hasEndorsedProposal(event) {
  const proposal = event._c.proposal;
  if(proposal && proposal._c.proposalEndorsed) {
    for(const endorsement of proposal._c.proposalEndorsement) {
      if(event._c.generation >= endorsement._c.generation) {
        return true;
      }
    }
  }
  return false;
}

// get a support set instance for the given `union`, creating one if necessary;
// using the same instances is an optimization for speeding up comparisons
function _getSupportSet(supportSets, union) {
  if(union instanceof Set) {
    // convert union to array and sort it by event hash
    union = [...union].sort(_compareEventHashes);
  }
  for(const set of supportSets) {
    if(_compareSupportSet(set, union)) {
      return set;
    }
  }
  // support set not found, add it
  supportSets.add(union);
  return union;
}

function _compareSupportSet(set1, set2) {
  if(set1.length !== set2.length) {
    return false;
  }
  // can compare events directly as hashes are assumed unique and sets
  // are ordered by hash; compare in reverse to more quickly find differences
  for(let i = set1.length - 1; i >= 0; --i) {
    if(set1[i] !== set2[i]) {
      return false;
    }
  }
  return true;
}

function _compareEventHashes(a, b) {
  return (a.eventHash < b.eventHash ? -1 : (a.eventHash > b.eventHash ? 1 : 0));
}

function _computeProposalEndorsement({
  ledgerNodeId, event, witnesses, supermajority
}) {
  if(event._c.proposalEndorsement !== null) {
    // already computed
    return;
  }

  // `endorsement` must be computed
  const endorsement = _findEndorsement({
    ledgerNodeId, x: event, witnesses, supermajority
  });
  if(endorsement) {
    // `endorsement` will be an array as byzantine nodes may fork, but forks
    // will get detected via endorse point(s) due to overlap of at least
    // one functioning node
    const allEndorsements = [];
    for(const e of endorsement) {
      if(e._c.endorsesProposal) {
        e._c.endorsesProposal.push(event);
      } else {
        e._c.endorsesProposal = [event];
      }
      /*console.log(
        `created proposal endorse point for`, event._c.generation,
        'at', e._c.generation);*/
      allEndorsements.push(e);
    }
    event._c.proposalEndorsement = allEndorsements;
  } else {
    // no proposal endorsement in DAG yet for `event`
    event._c.proposalEndorsement = false;
  }
}

/**
 * Find the earliest merge event for a witness that includes an ancestry of
 * merge events from at least a supermajority of witnesses. This merge event is
 * said to be an endorsement event for `x` because its ancestry includes merge
 * events from a supermajority of other nodes that descend from `x`. The search
 * starts at `x` and proceeds forward through history. It is possible to find
 * more than one merge event endorsement event if the node that created `x` is
 * byzantine.
 *
 * @param ledgerNodeId the ID of the current ledger node, used for logging.
 * @param x the event in history to begin searching at.
 * @param witnesses all current witnesses.
 * @param supermajority the number that constitutes a supermajority of
 *   witnesses.
 *
 * @return `null` or an array with the earliest endorsement merge event(s).
 */
function _findEndorsement({ledgerNodeId, x, witnesses, supermajority}) {
  //console.log('EVENT', x.eventHash);

  const xCreator = _getCreator(x);
  const _endorsers = new Set([xCreator]);

  if(supermajority === 1) {
    // trivial case, return `x`
    x._c.endorsers = _endorsers;
    return [x];
  }

  if(x._c.treeChildren.length === 0) {
    // no tree children, so return nothing
    return null;
  }

  // walk from `x` through its tree children until an endorsement is found;
  // an endorsement event occurs on the branch of `x` when events from `2f+1`
  // (a supermajority) of the witnesses have `x` in their ancestry
  const results = [];
  const endorsers = new Set([xCreator]);

  // must iterate through all possible forks on byzantine nodes as some nodes
  // may only see one side of the fork, this requires making copies for each
  // possible path through tree children
  let next = [];
  if(x._c.treeChildren.length === 1) {
    // no fork, no need to copy
    next.push({treeDescendant: x._c.treeChildren[0], endorsers});
  } else {
    // must copy endorsers due to fork
    next.push(...x._c.treeChildren.map(treeDescendant => ({
      treeDescendant,
      endorsers: new Set(endorsers)
    })));
  }
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const {treeDescendant, endorsers} of current) {
      let found = false;
      for(const [witness, ancestorEvent] of
        treeDescendant._c.mostRecentWitnessAncestors) {
        if(endorsers.has(witness)) {
          // already counted this witness as an endorser, continue
          continue;
        }

        // check `ancestorEvent` and its most recent `x` ancestor
        if(!ancestorEvent) {
          // byzantine fork detected, skip, node cannot endorse after forking
          continue;
        }
        const xAncestor =
          ancestorEvent._c.mostRecentWitnessAncestors.get(xCreator);

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
        if(endorsers.size >= supermajority) {
          found = true;
          //console.log('supermajority of endorsers found at generation',
          //  event._c.generation);
          //console.log();
          results.push(treeDescendant);
          break;
        }
      }

      // if endorsement merge event found or no more children, continue
      // to other tree descendant fork, if any
      if(found || treeDescendant._c.treeChildren.length === 0) {
        continue;
      }

      // endorsement merge event NOT found yet, add tree children
      if(treeDescendant._c.treeChildren.length === 1) {
        // no fork, no need to copy
        next.push(
          {treeDescendant: treeDescendant._c.treeChildren[0], endorsers});
      } else {
        // must copy endorsers due to fork
        next.push(...treeDescendant._c.treeChildren.map(treeDescendant => ({
          treeDescendant,
          endorsers: new Set(endorsers)
        })));
      }
    }
  }

  // save total endorsers of `x`
  x._c.endorsers = _endorsers;

  return results.length === 0 ? null : results;
}

function _getPriorityPeers({
  ledgerNodeId, history, witnessTails, blockHeight, witnesses, logger
}) {
  /* Algorithm:

  Determine which witnesses could help progress consensus by creating a
  new merge event.

  For each witness, determine if it needs an endorsement for a decision
  proposal. If so, track whether any other witness can endorse and see if
  there are enough to reach a supermajority.

  For each witness, go through each of the other witness's heads and see if
  they are new (not in the witness's ancestry) and, if so, if creating a
  new merge event would either provide an endorsement they need or if their
  new support has changed in a way that would influence a new merge event.

  If creating a new merge event would be helpful, prioritize the witness.
  */
  const supermajority = api.supermajority(witnesses.length);
  const f = api.maximumFailures(witnesses.length);

  // collect non-byzantine peers and their heads
  const nonByzantinePeers = [];
  const heads = new Map();
  for(const witness of witnesses) {
    const tails = witnessTails.get(witness);
    if(!tails) {
      // no events from the witness yet
      nonByzantinePeers.push(witness);
      continue;
    }

    if(tails.length > 1) {
      // byzantine node, do not tally
      continue;
    }
    const [tail] = tails;
    if(tail._c.head.length > 1) {
      // byzantine node, do not tally
      continue;
    }

    // non-byzantine peer with head
    nonByzantinePeers.push(witness);
    heads.set(witness, tail._c.head[0]);
  }

  // go through every peer and determine if they should be prioritized (if
  // a new merge event from them could progress consensus); if any peer has no
  // head yet, prioritize it
  const priorityPeers = new Set(nonByzantinePeers.filter(p => !heads.has(p)));
  for(const [witness, head] of heads) {
    // skip any peer that is already prioritized
    if(priorityPeers.has(witness)) {
      continue;
    }

    // determine if `witness` needs an endorsement for a proposal...
    // this to test whether or not merging each other head could
    // progress consensus
    let proposalEndorsers = null;
    if(head._c.proposal && !_hasEndorsedProposal(head)) {
      proposalEndorsers = head._c.proposal._c.endorsers;
    }

    // check other heads to determine if merging them in would be beneficial
    for(const [otherWitness, otherHead] of heads) {
      // can't help by merging self
      if(otherWitness === witness) {
        continue;
      }

      // if `witness` has a previous MRA from `otherWitness`, see if there is
      // an important change to merge in
      const mra = head._c.mostRecentWitnessAncestors &&
        head._c.mostRecentWitnessAncestors.get(otherWitness);
      if(!mra) {
        // never seen any event from `otherHead`, prioritize `witness` to
        // merge it in
        priorityPeers.add(witness);
        break;
      }

      if(mra === otherHead) {
        // `otherHead` is already in witness's ancestry, won't help to merge it
        continue;
      }

      // if `otherHead` has new endorsed proposal, then prioritize to merge
      // it in
      if(!_hasEndorsedProposal(mra) && _hasEndorsedProposal(otherHead)) {
        priorityPeers.add(witness);
        break;
      }

      // if `otherHead` has a new proposal vs mra, then prioritize
      // `witness` to merge it in
      if(mra._c.proposal !== otherHead._c.proposal) {
        priorityPeers.add(witness);
        break;
      }

      // if `otherHead`'s support has switched, then prioritize `witness` to
      // merge it in
      if((!mra._c.support && otherHead._c.support) ||
        (mra._c.support && otherHead._c.support &&
        mra._c.support !== otherHead._c.support)) {
        priorityPeers.add(witness);
        break;
      }

      // if `otherHead` has a proposal that `witness` can endorse, then
      // prioritize `witness`
      if(otherHead._c.proposal && otherHead._c.proposal._c.endorsers &&
        !_hasEndorsedProposal(otherHead) &&
        !otherHead._c.proposal._c.endorsers.has(witness)) {
        priorityPeers.add(witness);
        break;
      }

      // if `witness` needs a proposal endorsement and `otherWitness` hasn't
      // provided it, track it to check for threshold later
      if(proposalEndorsers &&
        !proposalEndorsers.has(otherWitness) &&
        _hasWitnessAncestor({
          event: otherHead,
          witness,
          ancestor: head._c.proposal
        })) {
        proposalEndorsers.add(otherWitness);
      }
    }

    // if proposal endorsers would reach threshold, then prioritize `witness`
    if(proposalEndorsers && proposalEndorsers.size >= supermajority) {
      priorityPeers.add(witness);
    }
  }

  if(priorityPeers.size <= f) {
    // we must guarantee there will always be > `f` witnesses in
    // `priorityPeers` or else it is possible that all prioritized
    // peers will be failed nodes
    for(const witness of nonByzantinePeers) {
      priorityPeers.add(witness);
    }
  }

  return [...priorityPeers];
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

function _hasWitnessAncestor({event, witness, ancestor}) {
  const mra = event._c.parents.length > 0 &&
    event._c.mostRecentWitnessAncestors.get(witness);
  return mra && mra._c.generation >= ancestor._c.generation;
}

function _findConsensusMergeEventPairs({ys, supermajority}) {
  const pairs = [];
  for(const y of ys) {
    const x = y;
    let proof = [];
    if(proof.length === 0 && supermajority === 1) {
      // always include single witness as proof; enables continuity of that
      // single witness when computing witnesses in the next block via
      // quick inspection of `block.consensusProofHash`
      proof = [y];
    }
    pairs.push({y, x, proof});
  }
  return pairs;
}

function _cacheSupport({state, event}) {
  // do not cache byzantine or uncomputed support
  if(!event._c.support) {
    return;
  }

  const memo = _memoizeEventHash({state, eventHash: event.eventHash});
  if(state.supportCache.has(memo.s)) {
    // already cached
    return;
  }

  const support = [];
  for(const {eventHash} of event._c.support) {
    support.push(_memoizeEventHash({state, eventHash}));
  }
  state.supportCache.set(memo.s, support);
}

function _getCachedSupportSet({state, event, supportSets}) {
  if(!state) {
    return;
  }
  const memo = _memoizeEventHash({state, eventHash: event.eventHash});
  const support = state.supportCache.get(memo.s);
  if(!Array.isArray(support)) {
    return support;
  }
  const set = [];
  for(const {h, s} of support) {
    set.push(state.eventMap.get(h));
  }
  return _getSupportSet(supportSets, set);
}

function _memoizeEventHash({state, eventHash}) {
  let memo = state.hashToMemo.get(eventHash);
  if(!memo) {
    memo = {h: eventHash, s: Symbol()};
    state.hashToMemo.set(eventHash, memo);
    state.symbolToMemo.set(memo.s, memo);
  }
  return memo;
}

function _sortIntoBlockOrder({mergeEvents, baseHashBuffer}) {
  function compareByMixedHash(a, b) {
    // sort by hash augmented via base hash (base hash is not under the
    // control of the creator of `a` or `b` so they cannot use it to influence
    // ordering)
    if(!a._c.consensusSortHash) {
      if(!a._c.hashBuffer) {
        a._c.hashBuffer = _parseHash(a.eventHash);
      }
      // mixin baseHashBuffer (modifies _hashBuffer in place to optimize)
      _xor(a._c.hashBuffer, baseHashBuffer);
      a._c.consensusSortHash = a._c.hashBuffer;
    }
    if(!b._c.consensusSortHash) {
      if(!b._c.hashBuffer) {
        b._c.hashBuffer = _parseHash(b.eventHash);
      }
      // mixin baseHashBuffer (modifies _hashBuffer in place to optimize)
      _xor(b._c.hashBuffer, baseHashBuffer);
      b._c.consensusSortHash = b._c.hashBuffer;
    }
    return Buffer.compare(a._c.hashBuffer, b._c.hashBuffer);
  }

  /* Algorithm for a total deterministic sort order for the events as they
  are to be processed in a block:
  1. Sort merge events by mixed hash (event hash + baseHash mixin). Sorting
    via mixed hash ensures that creators of events cannot gain any advantage
    in topological tie breaks. It also means, however, that a different order
    must be produced for gossipping blocks to enable more efficient
    partitioning of blocks for data transfer.
  2. Sort again, topologically, but preserve hash-sorted order; for each
    merge event, add its possible regular events in mixed-hash-sorted order
    before adding the merge event.
  */

  // 1. Sort by mixed hash.
  mergeEvents.sort(compareByMixedHash);

  // 2. Sort topologically to produce deterministic total ordering. Note: It
  //   is important that events are visited in sorted hash order so they are
  //   added in that order -- thereby deterministically settling any
  //   topological ties. Possible regular events are added just before the
  //   merge event that merged them, in mixed-hash-sorted order.
  let next = mergeEvents;
  const sortedSet = new Set();
  while(next.length > 0) {
    const current = new Set(next);
    next = [];
    const round = new Set();
    for(const event of current) {
      let defer = false;
      for(const parent of event._c.parents) {
        // if parent hasn't been sorted in a previous round, defer; deferring
        // if the parent was added in this round ensures that the mixed-hash
        // sort will break ties using the lesser hash
        if(!(sortedSet.has(parent) && !round.has(parent))) {
          defer = true;
          break;
        }
      }
      if(defer) {
        next.push(event);
        continue;
      }
      // if the event has external parents that could possibly be regular
      // events, sort by mixed hash and add those first; note: if any are not
      // actually regular events they will be removed in a subsequent process
      if(event._c.externalParents) {
        event._c.externalParents.sort(compareByMixedHash);
        for(const parent of event._c.externalParents) {
          sortedSet.add(parent);
        }
      }
      sortedSet.add(event);
      round.add(event);
    }
  }
  // convert sorted set to just unique hashes
  /* Note: If a fork occurs that reuses a regular event, the regular event's
  hash will only be included once and just prior to the merge event that
  is sorted first. If fork causes the regular event is referenced in a
  later block, it will not be included in that block at all, as it reached
  consensus in a previous one; it will treated just a referenced merge event
  parent that has previously reached consensus and will be removed from
  the ordered set of hashes for the current block by a subsequent process. */
  const sortedHashes = new Set();
  for(const {eventHash} of sortedSet) {
    sortedHashes.add(eventHash);
  }
  return [...sortedHashes];
}

function _compareBasisBlockHeight(a, b) {
  return a.event.basisBlockHeight - b.event.basisBlockHeight;
}

function _compareEventHash(a, b) {
  if(!a._c.hashBuffer) {
    a._c.hashBuffer = _parseHash(a.eventHash);
  }
  if(!b._c.hashBuffer) {
    b._c.hashBuffer = _parseHash(b.eventHash);
  }
  return Buffer.compare(a._c.hashBuffer, b._c.hashBuffer);
}
