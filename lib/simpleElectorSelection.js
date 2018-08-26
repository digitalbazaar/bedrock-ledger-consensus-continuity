/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _blocks = require('./blocks');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const {callbackify} = bedrock.util;
const crypto = require('crypto');
const logger = require('./logger');

// maximum number of electors if not specified in the ledger configuration
const MAX_ELECTOR_COUNT = 10;

// module API
const api = {};
module.exports = api;

api.type = 'SimpleElectorSelection';

// register this ledger plugin
bedrock.events.on('bedrock.start', () => {
  brLedgerNode.use('SimpleElectorSelection', {
    type: 'electorSelection',
    api: api
  });
});

api.getBlockElectors = callbackify(async (
  {ledgerNode, ledgerConfiguration, latestBlockSummary, blockHeight}) => {
  // get partipicants for the last block
  const {consensusProofPeers, mergeEventPeers} = await _blocks.getParticipants(
    {blockHeight: blockHeight - 1, ledgerNode});

  // TODO: we should be able to easily remove previously detected
  // byzantine nodes (e.g. those that forked at least) from the electors

  // TODO: simply count consensus event signers once and proof signers
  //   twice for now -- add comprehensive elector selection and
  //   recommended elector vote aggregating algorithm in v2
  const aggregate = {};
  let electors = mergeEventPeers.map(id => {
    return aggregate[id] = {id, weight: 1};
  });
  // TODO: weight previous electors more heavily to encourage continuity
  consensusProofPeers.map(id => {
    if(id in aggregate) {
      aggregate[id].weight = 3;
    } else {
      aggregate[id] = {id, weight: 2};
    }
  });
  electors = Object.values(aggregate);

  // get elector count, defaulting to MAX_ELECTOR_COUNT if not set
  // (hardcoded, all nodes must do the same thing -- but ideally this would
  // *always* be set)
  const electorCount = ledgerConfiguration.electorCount || MAX_ELECTOR_COUNT;

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

  const {blockHash} = latestBlockSummary.eventBlock.meta;

  // break ties via sorting
  electors.sort((a, b) => {
    // 1. sort descending by weight
    if(a.weight !== b.weight) {
      // FIXME: with current weights, this prevents elector cycling
      //   if commented out, will force elector cycling, needs adjustment
      return b.weight - a.weight;
    }

    // FIXME: when mixing in data, why not `xor` instead of sha-256?

    // generate and cache hashes
    // the hash of the previous block is combined with the elector id to
    // prevent any elector from *always* being sorted to the top
    a.hash = a.hash || _sha256(blockHash + _sha256(a.id));
    b.hash = b.hash || _sha256(blockHash + _sha256(b.id));

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

  logger.verbose(
    'Continuity2017 electors for ledger node ' + ledgerNode.id +
    ' at block height ' + blockHeight,
    {ledgerNode: ledgerNode.id, blockHeight, electors});

  return electors;
});

function _sha256(x) {
  return crypto.createHash('sha256').update(x).digest('hex');
}
