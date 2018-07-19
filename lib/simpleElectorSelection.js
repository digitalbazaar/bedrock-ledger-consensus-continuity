/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const _blocks = require('./blocks');
const crypto = require('crypto');
const {jsonld} = bedrock;
const logger = require('./logger');
const {BedrockError} = bedrock.util;

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

api.getBlockElectors = (
  {ledgerNode, ledgerConfiguration, blockHeight}, callback) => {
  async.auto({
    // NOTE: events *must* be expanded here
    latestBlock: callback => _blocks.getLatest(ledgerNode, (err, result) => {
    // latestBlock: callback => ledgerNode.storage.blocks.getLatest(
      // (err, result) => {
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
    electors: ['latestBlock', (results, callback) => {
      // get previous consensus events
      const previousEvents = results.latestBlock.eventBlock.block.event;

      // FIXME: this uses the key ID for the elector ID ... which has been
      //   made to be the same as the voter ID ... need to make sure this
      //   has no unintended negative consequences

      // aggregate recommended electors
      let electors = [];
      previousEvents.forEach(event => {
        if(!jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
          // regular event
          return;
        }
        // TODO: is `e.proof.creator` check robust enough? Can it assume
        //   a single signature and that it's by the voter? (merge events are
        //   only meant to be signed by the voter)
        electors.push(event.proof.creator);
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

      // TODO: we should be able to easily remove previously detected
      // byzantine nodes (e.g. those that forked at least) from the electors

      // TODO: simply count consensus event signers once and proof signers
      //   twice for now -- add comprehensive elector selection and
      //   recommended elector vote aggregating algorithm in v2
      const aggregate = {};
      electors = _.uniq(electors).forEach(
        e => aggregate[e] = {id: e, weight: 1});
      // TODO: weight previous electors more heavily to encourage continuity
      const consensusProof =
        results.latestBlock.eventBlock.block.consensusProof;
      _.uniq(consensusProof.map(e => e.proof.creator))
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
      const electorCount =
        ledgerConfiguration.electorCount || MAX_ELECTOR_COUNT;

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

      logger.verbose(
        'Continuity2017 electors for ledger node ' + ledgerNode.id +
        ' at block height ' + blockHeight,
        {ledgerNode: ledgerNode.id, blockHeight, electors});

      callback(null, electors);
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.electors));
};

function _sha256(x) {
  return crypto.createHash('sha256').update(x).digest('hex');
}
