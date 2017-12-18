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
api.findConsensus = ({ledgerNode, history, electors}, callback) => {
  // TODO: Note: once computed, merge event Y+X candidates for each
  //   elector can be cached for quick retrieval in the future without
  //   the need to recompute them (they never change) for a given block...
  //   so the next blockHeight, elector, and X pair (its hash) could be
  //   stored in the continuity2017 meta for each candidate merge event Y
  console.log('FINDCONSENSUS' /*, util.inspect(history, {depth: 8}) */);
  console.log('ELECTORS', electors);
  const tails = _getElectorBranches({history, electors});
  const proof = _findMergeEventProof({ledgerNode, history, tails, electors});
  if(proof.length === 0) {
    return callback();
  }
  const allXs = proof.consensus.map(p => p.x.eventHash);
  let consensusProofHash = [];
  for(let i = 0; i < proof.consensus.length; ++i) {
    const p = proof.consensus[i].proof;
    for(let n = 0; n < p.length; ++n) {
      consensusProofHash.push(p[n].eventHash);
    }
  }
  // remove duplicates
  consensusProofHash = _.uniq(consensusProofHash);
  async.auto({
    xAncestors: callback => _getAncestors(
      {ledgerNode, eventHash: allXs}, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.length === 0) {
          // FIXME: return a StateError?
          return callback(null, []);
        }
        // FIXME: eventHash is no longer needed since it's included in event
        const rVal = {event: [], eventHash: []};
        for(let i = 0; i < result.length; ++i) {
          rVal.event.push({
            eventHash: result[i]._id,
            event: result[i].event[0]
          });
          // FIXME: eventHash is no longer needed since it's included in event
          rVal.eventHash.push(result[i]._id);
        }
        callback(null, rVal);
      }),
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
      event: results.xAncestors.event,
      // FIXME: eventHash is no longer needed since it's included in event
      eventHash: results.xAncestors.eventHash,
    });
  });

  // TODO: fetch full history of proof; non-optimized way may be to do
  //   a $graphLookup on each Y stopping at its X and then doing a $graphLookup
  //   on its X, stopping at non-consensus (does that work?)

  // TODO: put Xs and their entire history into `result.event`

  // TODO: put remaining events into `result.consensusProof` (i.e. Ys and
  //   their entire history up to but not including X)
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

  // FIXME: remove below
  const byElector = {};
  // FIXME: remove above

  // find elector tails and build _treeParent index
  for(const e of history.events) {
    const creator = _getCreator(e);
    // TODO: use a more efficient search
    if(electors.map(e => e.id).includes(creator)) {
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
        if(!e._treeParent._treeChildren) {
          e._treeParent._treeChildren = [e];
        } else {
          e._treeParent._treeChildren.push(e);
        }
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
  for(const elector in electorTails) {
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
        // FIXME: handle byzantine case
        if(next._treeChildren) {
          next = next._treeChildren[0];
        } else {
          next = null;
        }
      }
    }
  }

  // FIXME: remove below
  for(const elector in byElector) {
    console.log('TOTAL ELECTOR EVENTS', elector, byElector[elector].length, byElector[elector]);
  }
  // FIXME: remove above

  return electorTails;
}

// FIXME: documentation
// accepts one or more eventHash
function _getAncestors({eventHash, ledgerNode}, callback) {
  const hashes = [].concat(eventHash);
  const collection = ledgerNode.storage.events.collection;
  collection.aggregate([
    {$match: {eventHash: {$in: hashes}}},
    {
      $graphLookup: {
        from: collection.s.name,
        startWith: '$eventHash',
        connectFromField: "event.parentHash",
        connectToField: "eventHash",
        as: "_parents",
        restrictSearchWithMatch: {
          'meta.consensus': {$exists: false}
        }
      },
    },
    {$unwind: '$_parents'},
    {$replaceRoot: {newRoot: '$_parents'}},
    // FIXME: remove if there is no need to sort here
    // the order of events is unpredictable without this sort, and we
    // must ensure that events are added in chronological order
    // {$sort: {'meta.created': 1}}
    {$group: {_id: '$eventHash', event: {$addToSet: '$event'}}},
  ], callback);
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
 * @param history recent history.
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
function _findMergeEventProof({ledgerNode, history, tails, electors}) {
  const candidates = _findMergeEventProofCandidates(
    {ledgerNode, tails, electors});
  if(!candidates) {
    // no Y candidates yet
    return [];
  }

  const yCandidatesByElector = candidates.yByElector;
  const supermajority = api.twoThirdsMajority(electors.length);
  if(Object.keys(yCandidatesByElector).length < supermajority) {
    // insufficient Y candidates so far, supermajority not reached
    return [];
  }

  const ys = _findConsensusMergeEventProof(
    {ledgerNode, history, yByElector: yCandidatesByElector, electors});
  if(ys.length === 0) {
    // no consensus yet
    return ys;
  }

  return {
    // pair Ys with Xs
    consensus: ys.map(y => (
      {y, x: candidates.xByElector[_getCreator(y)], proof: y._xPaths})),
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
      do {
        electorEvents.push(foo);
        foo = (foo._treeChildren || [])[0];
      } while(foo);
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
      console.log(
        'TOTAL ENDORSEMENTS', cache[earliest.eventHash].endorsements.length);
      // FIXME: remove below
      let min = 0;
      let foobar;
      for(const z in cache) {
        const len = (cache[z].endorsements || []).length;
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
    const cache = {};
    const x = xByElector[elector];
    console.log('FINDING Y FOR X', x, elector);
    const result = _findDiversePedigreeMergeEvent(
      {x, branch: elector, electors, cache, supermajority});
    if(result) {
      yByElector[elector] = result;
      result._xPaths = cache[result.eventHash].ancestors.filter(
        ancestor => ancestor !== x);
    }
  }

  logger.verbose(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, yByElector});
  console.log(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, yByElector});

  return {yByElector, xByElector};
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
 * @param ancestors an array of every endorsing ancestor, including
 *          non-electors, necessary for establishing unbroken proof for audit.
 * @param endorsements an array of unique endorsing electors (elector IDs).
 * @param candidate the earliest candidate found so far.
 *
 * @return the earliest merge event with a diverse pedigree.
 */
function _findDiversePedigreeMergeEvent({
  x, branch, electors, supermajority,
  endorsements = [_getCreator(x)],
  ancestors = [],
  cache = {},
  candidate = null
}) {
  console.log('EVENT', x.eventHash);
  // TODO: consider optimizing tracking unique endorsements using maps and
  // counters instead of arrays

  // if event creator is an elector, add its endorsement
  const creator = _getCreator(x);
  ancestors.push(x);
  console.log('CREATOR OF EVENT', creator);

  if(creator !== branch) {
    // FIXME: use a more efficient search
    if(electors.map(e => e.id).includes(creator) &&
      !endorsements.includes(creator)) {
      endorsements.push(creator);
      console.log('NEW ENDORSEMENT ON BRANCH', branch, endorsements);
    }
  } else {
    if(candidate && x._generation > candidate._generation) {
      // generation threshold passed, no earlier candidate can be found
      return candidate;
    }

    // TODO: don't always need to compute ancestors, only need it for Y
    //   not for X; optimize by only running code when flag is set or
    //   do ancestor computation elsewhere just once for Ys that are confirmed
    //   to be proof

    // update totals for the current event
    const totals = cache[x.eventHash] || {};
    // total endorsements
    let totalEndorsements = totals.endorsements || [];
    totalEndorsements.push(...endorsements);
    totalEndorsements = _.uniq(totalEndorsements);
    // total ancestors
    let totalAncestors = totals.ancestors || [];
    totalAncestors.push(...ancestors);
    totalAncestors = _.uniq(totalAncestors);
    cache[x.eventHash] = {
      endorsements: totalEndorsements,
      ancestors: totalAncestors
    };
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
    // FIXME: if regular events are removed from the history, this conditional
    // should be removed as well
    if(_isHistoryEntryRegularEvent(child)) {
      console.log('regular event detected at', child);
      // could be a regular event that has not been merged yet
      if(child._children.length === 0) {
        continue;
      }
      child = child._children[0];
      console.log('using new event', child);
    }

    // recurse into child
    console.log('RECURSING into child', child.eventHash, 'created by',
      _getCreator(child), 'from parent', x.eventHash);
    candidate = _findDiversePedigreeMergeEvent({
      x: child, branch, electors, supermajority,
      endorsements: endorsements.slice(),
      ancestors: ancestors.slice(), cache, candidate});
    console.log('RECURSION DONE for child', child.eventHash, 'created by',
      _getCreator(child), 'of parent', x.eventHash);
  }

  return candidate;
}

function _findConsensusMergeEventProof(
  {ledgerNode, history, yByElector, electors}) {
  logger.verbose(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id});
  console.log(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id});

  // build map of each Y's ancestry for quick checks of halting conditions
  const allYs = _.values(yByElector);
  const yAncestryMaps = _buildAncestryMaps(allYs);

  // TODO: consider initializing support to earlier Ys, if they exist,
  // for quicker consensus

  // initialize all Ys to support all self-seen Ys and mark all Y descendants
  allYs.forEach(y => {
    const found = {};
    _allSeenYs
    _hasAncestors({
      target: y,
      candidates: allYs,
      eventMap: history.eventMap,
      candidateAncestryMaps: yAncestryMaps,
      found
    });
    const supporting = _allSeenYs({event: y, history, allYs, yAncestryMaps});
    // always include self
    supporting.push(y);
    y._supporting = supporting;
    _markYDescendants(y);
  });

  // go through each Y's branch looking for consensus
  const result = {consensus: null};
  for(const elector in yByElector) {
    _tallyBranch(
      {ledgerNode, history, event: yByElector[elector], yByElector, allYs,
      electors, yAncestryMaps, result});
    if(result.consensus) {
      break;
    }
  }

  if(result.consensus) {
    logger.verbose(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNode.id, {ledgerNode: ledgerNode.id, proof: result.consensus});
    console.log(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNode.id, {ledgerNode: ledgerNode.id, proof: result.consensus});
  }

  return result.consensus || [];
}

function _tallyBranch(
  {ledgerNode, history, event, yByElector, allYs, electors,
  yAncestryMaps, result}) {
  _tally(
    {ledgerNode, history, event, yByElector, allYs,
      electors, yAncestryMaps, result});
  // handle byzantine fork case
  const forks = event._treeChildren || [];
  for(const next of forks) {
    if(result.consensus) {
      break;
    }
    _tallyBranch(
      {ledgerNode, history, event: next, yByElector, allYs,
        electors, yAncestryMaps, result});
  }
}

function _tally(
  {ledgerNode, history, event, yByElector, allYs, electors,
  yAncestryMaps, result}) {
  if(event._votes) {
    return;
  }

  logger.verbose('Continuity2017 _tally finding votes seen...',
    {ledgerNode: ledgerNode.id, event});
  event._votes = {};

  // consensus reached, no further tallying required
  if(result.consensus) {
    return;
  }

  for(const parent of event._parents) {
    if(_isHistoryEntryRegularEvent(parent)) {
      console.log('regular event detected at', parent);
      if(!parent._treeParent) {
        continue;
      }
      parent = parent._treeParent;
    }

    if(!parent._descendsFromY) {
      // parent sees no votes
      continue;
    }

    if(!parent._votes) {
      // use `tally` to recursively find votes that `parent` sees
      logger.verbose('Continuity2017 _tally recursing',
        {ledgerNode: ledgerNode.id, event, parent});
      _tally(
        {ledgerNode, history, event: parent, yByElector, allYs, electors,
          yAncestryMaps, result});
      if(result.consensus) {
        return;
      }
    }

    // update votes received from parent
    for(const elector in parent._votes) {
      // only count vote from a particular elector once, i.e. from the most
      // recent one from that elector
      const votingEvent = parent._votes[elector];
      if(elector in event._votes) {
        // FIXME: ensure byzantine fork case is adequately covered
        const latest = _latestInBranch(event._votes[elector], votingEvent);
        if(latest === votingEvent) {
          logger.verbose('Continuity2017 _tally replacing vote',
            {ledgerNode: ledgerNode.id, elector, votingEvent});
          event._votes[elector] = votingEvent;
        }
      } else {
        logger.verbose('Continuity2017 _tally found new vote',
          {ledgerNode: ledgerNode.id, elector, votingEvent});
        event._votes[elector] = votingEvent;
      }
    }
  }

  if(result.consensus) {
    return;
  }

  const creator = _getCreator(event);

  if(event._supporting) {
    // event is already supporting a candidate
    event._votes[creator] = event;
  } else if(!(creator in yByElector)) {
    // event not voting
    return;
  }

  // tally votes
  const tally = [];
  _.values(event._votes).forEach(e => {
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

  // sort tally by count
  tally.sort((a, b) => {
    // 1. sort descending by count
    if(a.count !== b.count) {
      return b.count - a.count;
    }
    // generate and cache hashes
    // the hash of the sorted Ys is combined with the current event's hash
    // to introduce pseudorandomness to break ties
    a.hash = a.hash || _sha256(_hashSet(a.set) + event.eventHash);
    b.hash = b.hash || _sha256(_hashSet(b.set) + event.eventHash);

    // 2. sort by hash
    return a.hash.localeCompare(b.hash);
  });

  logger.verbose('Continuity2017 _tally tally',
    {ledgerNode: ledgerNode.id, tally});
  // TODO: remove me
  console.log('VOTE TALLY', tally.map(t => ({
    count: t.count,
    set: JSON.stringify(t.set.map(r => r.eventHash))
  })));

  // see if first tally has a supermajority
  const supermajority = api.twoThirdsMajority(electors.length);
  if(tally[0].count >= supermajority) {
    // FIXME: must ensure a supermajority can see the election result in order
    // for the result to be safe
    // TODO: to accomplish this, perhaps run find Y algorithm on `event` as if
    //   it was an `X` and see if it has a supermajority of endorsements?
    /*const endorsed = _findDiversePedigreeMergeEvent(
      {x: event, branch: creator, electors, supermajority});
    if(endorsed) {
      // consensus reached
      result.consensus = tally[0].set;
    }*/

    // consensus reached
    result.consensus = tally[0].set;
    return;
  }

  // event already supporting a value
  if(event._supporting) {
    return;
  }

  // get previously supported value
  const previousChoice = event._treeParent._supporting;

  // total all votes and compute other votes
  const total = tally.reduce(
    (accumulator, currentValue) => accumulator + currentValue.count, 0);

  // determine if the remaining votes could cause previous choice to win
  const remaining = electors.length - total;
  const max = previousChoice.count + remaining;
  if(max >= supermajority) {
    // continue to support previous choice because it could still win
    event._supporting = previousChoice.set;
    logger.verbose('Continuity2017 _tally continuing support', {
      ledgerNode: ledgerNode.id,
      notVoted: remaining,
      currentVotes: previousChoice.count,
      requiredVotes: supermajority,
      event
    });
    // TODO: remove me
    console.log('continue supporting, remaining votes',
      remaining, 'count', previousChoice.count,
      'supporting', event._supporting);
    return;
  }

  // find top valid choice to support, based on its ancestry
  let topChoice;
  for(const tallyResult of tally) {
    // TODO: optimize
    // TODO: determine if creating this subset is necessary or if
    //   we can just pass `yAncestryMaps`
    const ancestryMaps = {};
    for(const y of tallyResult.set) {
      ancestryMaps[y.eventHash] = yAncestryMaps[y.eventHash];
    }
    if(_hasAncestors({
      target: event, candidates: tallyResult.set, eventMap: history.eventMap,
      candidateAncestryMaps: ancestryMaps, found: {}})) {
      topChoice = tallyResult;
      break;
    }
  }

  // if top choice only has a single vote for it, try to make a better
  // choice that includes all Ys seen at this point
  if(topChoice.count === 1) {
    topChoice = {
      set: _allSeenYs({event, history, allYs, yAncestryMaps})
    };
    logger.verbose('Continuity2017 _tally event supporting all seen Ys', {
      ledgerNode: ledgerNode.id,
      event,
      allSeenYs: topChoice.set.map(r => r.eventHash),
      seenYsCount: topChoice.set.length
    });
  }

  event._supporting = topChoice.set;
  event._votes[creator] = event;
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
    return _.difference(a, b).length === 0;
  };
}

function _hashSet(set) {
  if(set.hash) {
    return set.hash;
  }
  return set.hash = _sha256(
    set.map(r => r.eventHash)
      .sort((a, b) => a.localeCompare(b))
      .join(''));
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
  let parent = event;
  while(parent) {
    map[parent.eventHash] = true;
    parent = parent._treeParent;
  }
  return map;
}

function _markYDescendants(event) {
  event._descendsFromY = true;
  event._children.forEach(_markYDescendants);
}

function _latestInBranch(e1, e2) {
  if(e1._generation > e2._generation) {
    return e1;
  }
  return e2;
}

function _allSeenYs({event, history, allYs, yAncestryMaps}) {
  const found = {};
  _hasAncestors({
    target: event,
    candidates: allYs,
    eventMap: history.eventMap,
    candidateAncestryMaps: yAncestryMaps,
    found
  });
  return Object.keys(found).map(hash => history.eventMap[hash]);
}

/**
 * Return only events from `events` that are not descendants of other events
 * from `events`.
 *
 * @param events the events to prune.
 * @param ancestryMaps maps of `<eventHash> => {<ancestor eventHash>: true}`.
 * @param eventMap a map of eventHash to event for all events in recent history.
 *
 * @return an array of events where none is an ancestor of any other in the set.
 */
function _pruneDescendants({events, ancestryMaps, eventMap}) {
  const pruned = [];
  for(const event of events) {
    // track other events that are ancestors found so far
    const found = {};
    // exclude self from the search
    const candidates = events.filter(e => e !== event);
    candidates.forEach(e => found[e.eventHash] = false);

    if(!_hasAncestors({
      target: event,
      candidates,
      min: 1,
      eventMap,
      candidateAncestryMaps: ancestryMaps,
      found
    })) {
      pruned.push(event);
    }
  }
  return pruned;
}

/**
 * Returns `true` if the `target` has `min` ancestors from `candidates`.
 *
 * @param target the event to check the ancestry of for `candidates`.
 * @param candidates the possible ancestors of `target`.
 * @param min the minimum number of candidates that must be ancestors.
 * @param eventMap a map of all recent history events.
 * @param candidateAncestryMaps for the search halting positions.
 * @param found a map for tracking which candidates have been found so far.
 * @param haltAncestryMap a map of tree hashes that stop the search when found.
 *
 * @return `true` if `candidate` is an ancestor of `target`, `false` if not.
 */
function _hasAncestors(
  {target, candidates, min = candidates.length,
    eventMap, candidateAncestryMaps, found, haltAncestryMap = {}}) {
  for(const parent of target._parents) {
    if(parent.eventHash in haltAncestryMap) {
      // do not recurse into parent, it is in the halt map
      continue;
    }

    if(candidates.includes(parent)) {
      found[parent.eventHash] = true;
      if(_.values(found).filter(r => r).length >= min) {
        return true;
      }
    }

    if(_hasAncestors({
      target: parent, candidates, min, eventMap,
      candidateAncestryMaps, found, haltAncestryMap})) {
      return true;
    }
  }

  return _.values(found).filter(r => r).length >= min;
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
        //   a single signature and that it's by the voter?
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
