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
api._recommendElectors = _recommendElectors;
api._getElectorBranches = _getElectorBranches;
api._getAncestors = _getAncestors;
api._findMergeEventProof = _findMergeEventProof;
api._compareTallies = _compareTallies;

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
  logger.debug('Start sync _getElectorBranches branches', {electors});
  const tails = _getElectorBranches({history, electors});
  logger.debug('End sync _getElectorBranches');
  const startProof = Date.now();
  logger.debug('Start sync _findMergeEventProof');
  const proof = _findMergeEventProof({ledgerNode, history, tails, electors});
  logger.debug('End sync _findMergeEventProof', {
    proofDuration: Date.now() - startProof
  });
  if(proof.consensus.length === 0) {
    logger.debug('findConsensus no proof found, exiting');
    return callback(null, null);
  }
  logger.debug('findConsensus proof found, proceeding...');
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
  const electorSet = new Set(electors.map(e => e.id));

  // find elector tails and build _treeParent index
  for(const e of history.events) {
    const creator = _getCreator(e);
    if(electorSet.has(creator)) {
      // find parent from the same branch
      const treeHash = e.event.treeHash;
      e._treeParent = e._parents.filter(
        p => p.eventHash === treeHash)[0] || null;
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
  let startTime = Date.now();
  logger.debug('Start sync _findMergeEventProofCandidates');
  const candidates = _findMergeEventProofCandidates(
    {ledgerNode, tails, electors});
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
  logger.debug('Start sync _findConsensusMergeEventProof');
  const ys = _findConsensusMergeEventProof(
    {ledgerNode, history, xByElector: candidates.xByElector,
      yByElector: yCandidatesByElector, electors});
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
      return {
        y,
        x,
        proof: _flattenDescendants(
          {ledgerNode, x, y, descendants: y._xDescendants})
      };
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
function _findMergeEventProofCandidates({ledgerNode, tails, electors}) {
  const supermajority = api.twoThirdsMajority(electors.length);

  //console.log('TAILS', util.inspect(tails, {depth:10}));

  const electorsWithTails = Object.keys(tails);
  logger.verbose('Continuity2017 electors with tails for ledger node ' +
    ledgerNode.id + ' with required supermajority ' + supermajority,
    {ledgerNode: ledgerNode.id, electorsWithTails});
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

    // TODO: optimization: use X._initDescendants instead of a $graphLookup
    //   to find the events that must be retrieved from the database to build
    //   a block

    // find earliest `x` for the elector's tail
    const descendants = {};
    const result = _findDiversePedigreeMergeEvent(
      {ledgerNode, x: electorTails[0], electors, supermajority, descendants});
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

  logger.debug('Continuity2017 X merge events found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, xByElector});
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

  logger.verbose(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, yByElector});
  /*console.log(
    'Continuity2017 Y merge event candidates found for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id, yByElector});*/

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
  // find all descendants of `x` that are ancestors of with `y`
  let next = [];
  for(let parent of y._parents) {
    // FIXME: can we remove regular events from the recent history view
    // entirely? do they serve a useful purpose? there is a lot of repetitious
    // code that skips over them
    if(_isHistoryEntryRegularEvent(parent)) {
      if(!parent._treeParent) {
        continue;
      }
      parent = parent._treeParent;
    }
    const d = descendants[parent.eventHash];
    if(d) {
      if(!d.includes(y)) {
        d.push(y);
      }
      continue;
    }
    descendants[parent.eventHash] = [y];
    next.push(parent);
  }
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
  // remove regular events from `x` children
  let next = [];
  for(let child of x._children) {
    if(_isHistoryEntryRegularEvent(child)) {
      if(!child._children || child._children.length !== 1) {
        continue;
      }
      child = child._children[0];
    }
    // TODO: try to combine with below without losing optimization
    if(child.eventHash in descendants) {
      result.push(child);
    }
    next.push(child);
  }
  //console.log('X children', next.map(r=>({
  //  creator: _getCreator(r),
  //  generation: r._generation,
  //  hash: r.eventHash
  //})));
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
    next = _.uniq(next);
    result.push(...next);
  }
  return _.uniq(result);
}

function _hasSufficientEndorsements(
  {ledgerNode, x, descendants, electorSet, supermajority}) {
  // always count `x` as self-endorsed
  const endorsements = new Set([_getCreator(x)]);
  let total = 1;
  // remove regular events from `x` children
  let next = [];
  for(let child of x._children) {
    if(_isHistoryEntryRegularEvent(child)) {
      if(!child._children || child._children.length !== 1) {
        continue;
      }
      child = child._children[0];
    }
    // TODO: try to combine with below without losing optimization
    if(child.eventHash in descendants) {
      const creator = _getCreator(child);
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
    next.push(child);
  }

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

function _findConsensusMergeEventProof(
  {ledgerNode, history, xByElector, yByElector, electors}) {
  console.log('aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');
  logger.verbose(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id});
  /*console.log(
    'Continuity2017 looking for consensus merge proof for ledger node ' +
    ledgerNode.id, {ledgerNode: ledgerNode.id});*/

  // build map of each Y's ancestry for quick checks of halting conditions
  const allYs = _.values(yByElector);
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

  // TODO: consider initializing support to earlier Ys, if they exist,
  // for quicker consensus

  // initialize all Ys to support all self-endorsed Ys and mark all Y
  // descendants
  allYs.forEach(y => {
    console.log('Start sync _allEndorsedYs');
    const supporting = _allEndorsedYs({event: y, allYs, yAncestryMaps});
    console.log('End sync _allEndorsedYs');
    // always include self
    supporting.add(y);
    y._supporting = [...supporting];
    _markYDescendants(y);
  });

  let startTime = Date.now();
  logger.debug('Start sync _findConsensusMergeEventProof: _tallyBranch');
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
  logger.debug('End sync _findConsensusMergeEventProof: _tallyBranch', {
    duration: Date.now() - startTime
  });

  if(result.consensus) {
    logger.verbose(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNode.id, {ledgerNode: ledgerNode.id, proof: result.consensus});
    /*console.log(
      'Continuity2017 merge event proof found for ledger node ' +
      ledgerNode.id, {ledgerNode: ledgerNode.id, proof: result.consensus});*/
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

  for(let parent of event._parents) {
    if(_isHistoryEntryRegularEvent(parent)) {
      //console.log('regular event detected at', parent);
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
      // TODO: rewrite to be non-recursive
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

    // include votes received from parent
    for(const elector in parent._votes) {
      const votingEvent = parent._votes[elector];
      if(elector in event._votes) {
        // only count vote from a particular elector once, using the most
        // recent from that elector; if an elector has two voting events
        // from the same generation, it is byzantine, invalidate its vote
        const existing = event._votes[elector];
        if(existing === false || votingEvent === false) {
          logger.verbose('Continuity2017 _tally detected byzantine node ',
            {ledgerNode: ledgerNode.id, elector});
        } else if(votingEvent._generation > existing._generation) {
          logger.verbose('Continuity2017 _tally replacing voting event',
            {ledgerNode: ledgerNode.id, elector, votingEvent});
          event._votes[elector] = votingEvent;
        } else if(
          votingEvent._generation === existing._generation &&
          votingEvent !== existing) {
          // byzantine node!
          logger.verbose('Continuity2017 _tally detected byzantine node',
            {ledgerNode: ledgerNode.id, elector, votingEvent});
          event._votes[elector] = false;
        }
      } else {
        logger.verbose('Continuity2017 _tally found new voting event',
          {ledgerNode: ledgerNode.id, elector, votingEvent});
        event._votes[elector] = votingEvent;
      }
    }
  }

  if(result.consensus) {
    return;
  }

  const creator = _getCreator(event);

  if(event._supporting && event._votes[creator] !== false) {
    // event is already supporting a candidate
    event._votes[creator] = event;
  } else if(!(creator in yByElector)) {
    // event not voting
    return;
  }

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

  // sort tally by count
  tally.sort((a, b) => {
    // 1. sort descending by count
    if(a.count !== b.count) {
      return b.count - a.count;
    }
    return api._compareTallies({event, a, b});
  });

  logger.verbose('Continuity2017 _tally tally',
    {ledgerNode: ledgerNode.id, tally});
  // TODO: remove me
  /*console.log('VOTE TALLY', ledgerNode.id, tally.map(t => ({
    count: t.count,
    set: JSON.stringify(t.set.map(r => r.eventHash))
  })));*/

  // see if first tally has a supermajority
  const supermajority = api.twoThirdsMajority(electors.length);
  if(tally.length > 0 && tally[0].count >= supermajority) {
    // consensus reached
    result.consensus = tally[0].set;
    return;
  }

  // event already supporting a value
  if(event._supporting) {
    return;
  }

  // byzantine node, do not update votes
  if(event._votes[creator] === false) {
    return;
  }

  // get previously supported value (there won't be one if the tree parent
  // is not a descendant of any Y, in which case we are making our first
  // choice)
  const previousChoice = event._votes[creator] ? _.find(
    tally, _findSetInTally(event._votes[creator]._supporting)) : null;
  if(previousChoice) {
    // total all votes and compute other votes
    const total = tally.reduce(
      (accumulator, currentValue) => accumulator + currentValue.count, 0);

    // determine if the remaining votes could cause previous choice to win
    const remaining = electors.length - total;
    const max = previousChoice.count + remaining;
    if(max >= supermajority) {
      // continue to support previous choice because it could still win
      event._supporting = previousChoice.set;
      event._votes[creator] = event;
      logger.verbose('Continuity2017 _tally continuing support', {
        ledgerNode: ledgerNode.id,
        notVoted: remaining,
        currentVotes: previousChoice.count,
        requiredVotes: supermajority,
        event
      });
      // TODO: remove me
      /*console.log('continue supporting, remaining votes',
        remaining, 'count', previousChoice.count,
        'supporting', event._supporting);*/
      return;
    }
  }

  // choose first choice of sorted tallies (it is a valid choice because it
  // is entirely based on ancestry)
  let topChoice = tally[0];

  // if top choice has only a single vote for it, try to make a better
  // choice that includes all Ys endorsed at this point
  if(!topChoice || topChoice.count === 1) {
    let union;
    if(!topChoice) {
      // handle corner case with byzantine nodes where there is no top choice
      // because all tallies were cleared due to invalid votes; support
      // all endorsed Ys
      union = [..._allEndorsedYs({event, allYs, yAncestryMaps})];
    } else {
      // union all Ys in previous choices
      // TODO: make more functional
      union = [];
      tally.forEach(tallyResult => union.push(...tallyResult.set));
      union = _.uniq(union);
    }

    topChoice = _.find(tally, _findSetInTally(union));
    if(!topChoice) {
      // create new top choice
      topChoice = {set: union, count: 0};
    }

    logger.verbose('Continuity2017 _tally event supporting all endorsed Ys', {
      ledgerNode: ledgerNode.id,
      event,
      allEndorsedYs: topChoice.set.map(r => r.eventHash),
      endorsedYsCount: topChoice.set.length
    });
  }

  event._supporting = topChoice.set;
  event._votes[creator] = event;

  // if vote has changed
  if(previousChoice !== topChoice) {
    // increment top choice count
    topChoice.count++;

    // if top choice now has a supermajority...
    if(topChoice.count >= supermajority) {
      // consensus reached
      result.consensus = topChoice.set;
      return;
    }
  }
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

/**
 * Compares two vote tallies, each with `{count, set}` in the context of a
 * particular voting event. Exposed for testing purposes.
 *
 * @param event the event that provides context.
 * @param a the first vote tally.
 * @param b the second vote tally.
 *
 * @return true if a < b, false otherwise.
 */
function _compareTallies({event, a, b}) {
  // generate and cache hashes
  // the hash of the sorted Ys is combined with the current event's hash
  // to introduce pseudorandomness to break ties
  a.hash = a.hash || _sha256(_hashSet(a.set) + event.eventHash);
  b.hash = b.hash || _sha256(_hashSet(b.set) + event.eventHash);

  // 2. sort by hash
  return a.hash.localeCompare(b.hash);
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
  while(next.length > 0) {
    const current = next;
    next = [];
    for(const parent of current) {
      if(candidateSet.has(parent)) {
        found.add(parent);
        if(found.size >= min) {
          return true;
        }
      }
      // determine if parent can be now be ruled out as leading to any further
      // discoveries by testing if it is not in at least one of the remaining
      // candidate ancestry maps
      const difference = [...candidateSet].filter(x => !found.has(x));
      const viable = difference.some(
        c => !(parent.eventHash in candidateAncestryMaps[c.eventHash]));
      if(viable) {
        next.push(...(parent._parents || []));
      } else {
        console.log('ruled out!');
      }
    }
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

      // reduce electors to highest multiple of `3f + 1`, i.e.
      // `electors.length % 3 === 1` or electors < 4 ... electors MUST be a
      // multiple of `3f + 1` for BFT or less than 4 for 100% valid nodes
      while(electors.length > 3 && (electors.length % 3 !== 1)) {
        electors.pop();
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
