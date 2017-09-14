/*
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const crypto = require('crypto');
const jsonld = bedrock.jsonld;
const jsigs = require('jsonld-signatures')();
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const logger = require('./logger');
const niUri = require('ni-uri');
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
api._createEventManifest = _createEventManifest;
api._getManifest = _getManifest;
api._recommendElectors = _recommendElectors;

/**
 * Create a hash of a manifest.
 *
 * @param items the items in the manifest.
 * @returns hash the hash of the items.
 */
api.createManifestHash = items =>
  niUri.digest('sha-256', items.join('\n'), true);

/**
 * Check if a  RollCall manifest has the required number of votes.
 *
 * @param manifest the manifest to check.
 * @param ledgerNode the ledgerNode that is tracking the manifest.
 * @param callback(err) called once the operation is complete.
 */
api.validateRollCallManifest = (manifest, ledgerNode, callback) => {
  if(manifest.type !== 'RollCall') {
    return callback(new BedrockError(
      'The manifest `type` must be `RollCall`',
      'DataError', {httpStatusCode: 400, public: true, manifest}));
  }

  const blockHeight = manifest.blockHeight;
  const manifestItemCount = manifest.item.length;

  // TODO: consider passing electors through to optimize away this look up
  api.getBlockElectors(ledgerNode, blockHeight, (err, result) => {
    if(err) {
      return callback(err);
    }
    const twoThirds = _twoThirdsMajority(result.length);
    if(manifestItemCount < twoThirds) {
      return callback(new BedrockError(
        'The manifest does not contain the required two-thirds majority.',
        'ValidationError', {
          httpStatusCode: 400,
          public: true,
          manifest,
          manifestItemCount,
          twoThirds,
        }));
    }
    callback();
  });
};

/**
 * Get the voter population for the given ledger node and block height.
 *
 * The voters will be passed to the given callback using the given
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
    latestBlock: callback =>
      ledgerNode.storage.blocks.getLatest((err, result) => {
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
      // get previous votes
      const previousVotes =
        results.latestBlock.eventBlock.block.electionResults || [];

      // aggregate recommended electors
      let electors = [];
      previousVotes.forEach(vote => {
        electors.push(...jsonld.getValues(vote, 'recommendedElector'));
      });
      const aggregate = {};
      electors.forEach(e => {
        let x;
        if(e.id in aggregate) {
          x = aggregate[e.id];
          x.count++;
        } else {
          x = aggregate[e.id] = {
            id: e.id,
            count: 1
          };
        }
        if(e.sameAs) {
          x.sameAs = e.sameAs;
        }
      });
      electors = Object.keys(aggregate).map(k => aggregate[k]);

      // get elector count, defaulting to MAX_ELECTOR_COUNT if not set
      // (hardcoded, all nodes must do the same thing -- but ideally this would
      // *always* be set)
      const electorCount = results.config.electorCount || MAX_ELECTOR_COUNT;

      // compute super majority requirement
      const twoThirds = _twoThirdsMajority(previousVotes.length);

      // for electors with recommended count >= *super* majority, use them
      electors = electors.filter(e => e.count >= twoThirds);

      if(electors.length < electorCount) {
        // not enough electors received super majority recommendations; add
        // previous electors whose votes were accepted
        previousVotes.forEach(vote => {
          // do not duplicate
          if(!electors.some(e => e.id === vote.voter)) {
            electors.push({id: vote.voter, count: 0});
          }
        });
      }

      // it's possible `electors.length` will be less than `electorCount` if
      // a config change happened -- which we allow here; in theory more
      // electors will be recommended later to fill the gap
      if(electors.length > electorCount) {
        // TODO: could optimize by only sorting tied electors if helpful
        /*
        // fill positions
        let idx = -1;
        for(let i = 0; i < electorCount; ++i) {
          if(electors[i].count > electors[i + 1].count) {
            idx = i;
          }
        }
        // fill positions with non-tied electors
        const positions = electors.slice(0, idx + 1);
        if(positions.length < electorCount) {
          // get tied electors
          const tied = electors.filter(
            e => e.count === electors[idx + 1].count);
          // TODO: sort tied electors
        }
        }*/

        // break ties via sorting
        electors.sort((a, b) => {
          // 1. sort descending by count
          if(a.count !== b.count) {
            return b.count - a.count;
          }

          // 2. sort by previous elector status
          const aPreviousElector = previousVotes.some(v => v.voter === a.id);
          const bPreviousElector = previousVotes.some(v => v.voter === b.id);
          if(aPreviousElector !== bPreviousElector) {
            return aPreviousElector - bPreviousElector;
          }

          // generate and cache hashes
          // the hash of the previous block is combined with the elector id to
          // prevent any elector from *always* being sorted to the top
          a.hash = a.hash || _sha256(
            results.latestBlock.eventBlock.meta.blockHash + _sha256(a.id));
          b.hash = b.hash || _sha256(
            results.latestBlock.eventBlock.meta.blockHash + _sha256(b.id));

          // 3. sort by hash
          return a.hash.localeCompare(b.hash);
        });

        // select first `electorCount` electors
        electors = electors.slice(0, electorCount + 1);
      }

      // TODO: if there were no electors chosen or insufficient electors,
      // add electors from config

      callback(null, electors.map(e => {
        // only include `id` and `sameAs`
        const elector = {id: e.id};
        if(e.sameAs) {
          elector.sameAs = e.sameAs;
        }
        return elector;
      }));
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
 * Verifies and stores each vote.
 *
 * @param ledgerNode the ledger node.
 * @param electionTopic the election topic ('Events' or 'RollCall').
 * @param votes the votes to certify.
 * @param callback(err) called once the operation completes.
 */
api.certify = (ledgerNode, electionTopic, votes, callback) => {
  async.auto({
    config: callback => _getLatestConfig(ledgerNode, callback),
    certify: ['config', (results, callback) => {
      const maxElectorCount = results.config.electorCount || MAX_ELECTOR_COUNT;

      let voteRound = 0;
      let voteCount = 0;
      do {
        voteRound++;
        voteCount = votes.filter(v => v.voteRound === voteRound).length;
        if(voteCount > maxElectorCount) {
          return callback(new BedrockError(
            'The vote count exceeds the maximum number of electors.',
            'ValidationError', {
              electionTopic,
              votes,
              voteCount: votes.length,
              maxElectorCount,
            }));
        }
      } while(voteCount > 0);

      async.each(votes, (vote, callback) => {
        async.auto({
          voteHash: callback => api._hasher(vote, callback),
          voteExists: ['voteHash', (results, callback) =>
            api._storage.votes.exists(ledgerNode.id, results.voteHash, callback)
          ],
          verify: ['voteExists', (results, callback) => {
            if(results.voteExists === true) {
              // vote already exists, do not need to verify or store again
              return callback();
            }
            _verifyVote(ledgerNode, vote, electionTopic, err => {
              if(err) {
                // ignore failed votes, do not store them
                logger.verbose('Non-critical error in _verifyVote.', err);
                return callback();
              }
              api._storage.votes.add(
                ledgerNode.id, electionTopic, vote, {
                  meta: {voteHash: results.voteHash}
                }, err => {
                  if(err && err.name === 'DuplicateError') {
                    // this vote has already been added, via another process,
                    // can safely ignore this error and proceed as if we wrote
                    // the vote
                    return callback();
                  }
                  callback(err);
                });
            });
          }]
        }, callback);
      }, callback);
    }]
  }, err => callback(err));
};

/**
 * Tallies a vote for the passed block height. If there is a winning manifest
 * for the block, its hash will be returned as `winner` with the manifestHash
 * and round set, otherwise `winner` will be `null`.
 *
 * @param ledgerNodeId the ID of the ledger node that is tallying votes.
 * @param blockHeight the height of the block to tally the vote for.
 * @param electors the voting population for the block height.
 * @param electionType the type of votes to tally ('Events' or 'RollCall').
 * @param callback(err, {manifestHash, round}) called once the operation
 *          completes.
 */
api.tally = (ledgerNodeId, blockHeight, electors, electionType, callback) => {
  /* Go through rounds and see if 2/3rds have voted yet in the round. If not,
    then finish the tally because we need to wait for more votes to arrive. If
    so, and a 2/3rds majority has voted for a particular manifest, finish tally
    and return the winner. */
  let done = false;
  const twoThirds = _twoThirdsMajority(electors.length);
  const result = {winner: null};
  let round = 0;
  async.until(() => done, callback => {
    round++;
    api._storage.votes.tally(
      ledgerNodeId, blockHeight, electionType, round, (err, votes) => {
      if(err) {
        return callback(err);
      }
      const total = votes.reduce((sum, v) => sum + v.count, 0);
      result.currentRound = round;
      if(total < twoThirds) {
        // not enough votes this round, break out to collect more votes
        done = true;
      } else if(votes[0].count >= twoThirds) {
        // we have a winner in this round
        done = true;
        result.winner = {manifestHash: votes[0].manifestHash, round};
      }
      callback();
    });
  }, err => callback(err, result));
};

/**
 * Performs a vote for the ledger node identified by the `voter` information
 * if the voter has not voted in the current round yet. The vote will be for a
 * manifest to use to create the next block which is identified by
 * `blockHeight`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the next block.
 * @param electors all electors for the next block.
 * @param round the current round.
 * @param callback(err) called once the operation completes.
 */
api.voteForEvents = (
  {ledgerNode, voter, blockHeight, electors, round}, callback) => {
  // get last vote cast by voter for the block
  api._storage.votes.getLast(
    ledgerNode.id, blockHeight, 'Events', voter.id, (err, vote) => {
    if(err) {
      return callback(err);
    }
    if(vote) {
      if(vote.voteRound >= round) {
        // already voted this round
        return callback();
      }
      // previous vote has occurred, so do instant run-off and vote again
      return _createInstantRunoffEventVote(
        ledgerNode, vote, voter, electors, callback);
    }
    _createFirstRoundEventVote(
      ledgerNode, voter, blockHeight, electors, callback);
  });
};

/**
 * Performs a vote for the ledger node identified by the `voter` information
 * if the voter has not voted in the current around yet. The vote will be for
 * a manifest to use to determine the roll call for the previous election of
 * the events for the next block.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the next block.
 * @param electors the voting population for the block height.
 * @param round the current round.
 * @param callback(err) called once the operation completes.
 */
api.voteForRollCall = (
  {ledgerNode, voter, blockHeight, electors, round}, callback) => {
  // get last vote cast by voter for the block
  api._storage.votes.getLast(
    ledgerNode.id, blockHeight, 'RollCall', voter.id, (err, vote) => {
    if(err) {
      return callback(err);
    }
    if(vote) {
      if(vote.voteRound >= round) {
        // already voted this round
        return callback();
      }
      // previous vote has occurred, so do instant run-off and vote again
      return _createInstantRunoffRollCallVote(
        ledgerNode, vote, voter, electors, callback);
    }
    _createFirstRoundRollCallVote(
      ledgerNode, voter, blockHeight, electors, callback);
  });
};

/**
 * Create and store a first round vote based off of all of the current events
 * that have not yet reached consensus.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter that is voting.
 * @param blockHeight the height of the next block.
 * @param electors all electors for the next block.
 * @param callback(err) called once the operation completes.
 */
function _createFirstRoundEventVote(
  ledgerNode, voter, blockHeight, electors, callback) {
  // create event manifest to vote on
  _createEventManifest(ledgerNode, blockHeight, (err, manifest) => {
    if(err) {
      return callback(err);
    }
    if(!manifest) {
      // nothing to cast vote for yet
      return callback();
    }

    // recommend electors based on manifest
    // (call via `api` to enable mocking for tests)
    api._recommendElectors(
      ledgerNode, voter, electors, manifest, (err, recommendedElectors) => {
      if(err) {
        return callback(err);
      }
      // create vote for manifest
      const vote = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        blockHeight: blockHeight,
        manifestHash: manifest.id,
        voteRound: 1,
        voter: voter.id,
        recommendedElector: recommendedElectors
      };
      _createVote(ledgerNode, 'Events', vote, voter, err => callback(err));
    });
  });
}

/**
 * Creates a manifest from all current events that have not yet reached
 * consensus and stores it in the database.
 *
 * @param ledgerNode the ledger node.
 * @param blockHeight the height of the block.
 * @param callback(err, manifest) called once the operation completes.
 */
function _createEventManifest(ledgerNode, blockHeight, callback) {
  // TODO: limit number of events according to continuity config block?
  async.auto({
    getHashes: callback => ledgerNode.storage.events.getHashes(
      {consensus: false, sort: 1}, callback),
    validate: ['getHashes', (results, callback) =>
      _validateEvents(ledgerNode, results.getHashes, blockHeight, callback)
    ],
    create: ['validate', (results, callback) => {
      if(results.validate.hashes.length === 0) {
        return callback();
      }
      _createManifest(
        ledgerNode.id, blockHeight, 'Events', results.validate.hashes,
        callback);
    }]
  }, (err, results) => {
    callback(err, results.create);
  });
}

/**
 * Recommends the next set of electors.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter.
 * @param electors all electors for the next block.
 * @param manifest the manifest selected by voter in the first round of voting.
 * @param callback(err, recommendedElectors) called once the operation
 *          completes.
 */
function _recommendElectors(ledgerNode, voter, electors, manifest, callback) {
  // TODO: determine if a better deterministic algorithm (better == more secure,
  // performant) could be used here that uses given parameters and blockchain
  // data to recommend electors for the next block

  let recommendedElectors = [];

  // TODO: implement option to add all nodes that sent in events to the
  // previous block to the elector pool that can be contacted

  async.auto({
    config: callback => _getLatestConfig(ledgerNode, callback),
    latestBlock: callback => ledgerNode.storage.blocks.getLatest(callback),
    latestVoters: ['latestBlock', (results, callback) => {
      // there may not be an event block yet
      const voters =
        _.get(results, 'latestBlock.eventBlock.block.electionResults', [])
          .map(v => ({id: v.voter}));
      recommendedElectors.push(...voters);
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
      // TODO: was expecting to have to check full URL:
      //   `https://w3id.org/webledger#Continuity2017Peer`... but doesn't
      //   seem to be the case with what comes back from jsigs
      if(jsonld.hasValue(owner, 'type', 'Continuity2017Peer')) {
        owners.push({id: owner.id});
      }
      callback(null, true);
    }
  }, err => {
    if(err) {
      // ignore bad or missing signature; no event peer can be found
      logger.verbose('Non-critical error in _getEventPeers.', err);
      return callback(null, []);
    }
    // TODO: do a more robust check to ensure that the peer is up-to-date
    // with the current blockHeight (block status phase is `consensus`)
    const blockHeight = 0;
    async.filter(
      owners, (owner, callback) => api._client.getBlockStatus(
        blockHeight, owner.id, (err, status) =>
          callback(null, !err && status.ledger === ledgerNode.ledger)),
      callback);
  });
}

/**
 * Create and store the event manifest to vote for in the next round.
 *
 * @param ledgerNode the ledger node.
 * @param vote the previous round's vote.
 * @param voter the voter information.
 * @param electors the electors for the next block.
 * @param callback(err, manifestHash) called once the operation completes.
 */
function _createInstantRunoffEventVote(
  ledgerNode, vote, voter, electors, callback) {
  // TODO: consolidate with other instant runoff vote helper function
  _selectInstantRunoffEventManifest(
    ledgerNode, vote, electors, (err, manifestHash) => {
    if(err) {
      return callback(err);
    }
    const newVote = Object.assign({}, vote, {
      manifestHash: manifestHash,
      voteRound: vote.voteRound + 1
    });
    delete newVote.signature;
    _createVote(ledgerNode, 'Events', newVote, voter, err => callback(err));
  });
}

/**
 * Select the manifest to vote for in the next round after running an
 * "instant runoff" algorithm.
 *
 * @param ledgerNode the ledger node.
 * @param vote the previous round's vote.
 * @param electors the electors for the next block.
 * @param callback(err, manifestHash) called once the operation completes.
 */
function _selectInstantRunoffEventManifest(
  ledgerNode, vote, electors, callback) {
  /* Note: Get a count of all of the events in every manifest that received
    a vote. Put every event that received a majority vote into a manifest
    and select it. If there would be zero events in the manifest, then instead
    create a manifest that has every single event in it and select that. */
  const majority = _majority(electors.length);
  api._storage.votes.tally(
    ledgerNode.id, vote.blockHeight, 'Events', vote.voteRound,
    (err, tallies) => {
    if(err) {
      return callback(err);
    }

    // get unique list of manifests that were voted for
    const manifestHashes = _unique(tallies.map(tally => tally.manifestHash));

    // get a count of every event in the manifests that were voted for
    api._storage.manifests.getItemCount(
      ledgerNode.id, manifestHashes, (err, results) => {
      if(err) {
        return callback(err);
      }

      // filter out events that got a majority vote
      let eventHashes = results
        .filter(item => item.count >= majority)
        .map(item => item.id);

      if(eventHashes.length === 0) {
        // no events received a majority vote; use *all* of them
        eventHashes = results.map(item => item.id);
      }

      _createManifest(
        ledgerNode.id, vote.blockHeight, 'Events', eventHashes,
        (err, manifest) => err ? callback(err) : callback(null, manifest.id));
    });
  });
}

/**
 * Create and store a first round vote based off of all of the votes that
 * have been collected for the chosen events manifest.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter that is voting.
 * @param blockHeight the height of the block.
 * @param electors the voting population for the block height.
 * @param callback(err) called once the operation completes.
 */
function _createFirstRoundRollCallVote(
  ledgerNode, voter, blockHeight, electors, callback) {
  _createRollCallManifest(
    ledgerNode, blockHeight, electors, (err, manifest) => {
      if(err) {
        return callback(err);
      }
      // create vote for manifest
      const vote = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        blockHeight: blockHeight,
        manifestHash: manifest.id,
        voteRound: 1,
        voter: voter.id
      };
      _createVote(ledgerNode, 'RollCall', vote, voter, err => callback(err));
    });
}

/**
 * Creates a manifest from all current events that have not yet reached
 * consensus and stores it in the database.
 *
 * @param ledgerNode the ledger node.
 * @param blockHeight the height of the block.
 * @param electors the voting population for the block height.
 * @param callback(err, manifestHash) called once the operation completes.
 */
function _createRollCallManifest(ledgerNode, blockHeight, electors, callback) {
  async.auto({
    votes: callback =>
      api._storage.votes.get(ledgerNode.id, blockHeight, 'Events', callback),
    hashes: ['votes', (results, callback) => {
      if(results.votes.length === 0) {
        return callback();
      }
      // only include votes from the winning `voteRound`
      api.tally(
        ledgerNode.id, blockHeight, electors, 'Events', (err, result) => {
          if(err) {
            return callback(err);
          }
          const winningRound = result.winner.round;
          const hashes = results.votes
            .filter(v => v.vote.voteRound === winningRound)
            .map(v => v.meta.voteHash);
          callback(null, hashes);
        });
    }],
    manifest: ['hashes', (results, callback) => {
      if(results.votes.length === 0) {
        // no votes, don't create empty manifest
        return callback(null, null);
      }
      _createManifest(
        ledgerNode.id, blockHeight, 'RollCall', results.hashes, callback);
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.manifest));

}

/**
 * Create and store the roll call manifest to vote for in the next round.
 *
 * @param ledgerNode the ledger node.
 * @param vote the previous round's vote.
 * @param voter the voter information.
 * @param electors the electors for the next block.
 * @param callback(err, manifestHash) called once the operation completes.
 */
function _createInstantRunoffRollCallVote(
  ledgerNode, vote, voter, electors, callback) {
  // TODO: consolidate with other instant runoff vote helper function
  _selectInstantRunoffRollCallManifest(
    ledgerNode, vote, electors, (err, manifestHash) => {
    if(err) {
      return callback(err);
    }
    const newVote = Object.assign({}, vote, {
      manifestHash: manifestHash,
      voteRound: vote.voteRound + 1
    });
    delete newVote.signature;
    _createVote(ledgerNode, 'RollCall', newVote, voter, err => callback(err));
  });
}

/**
 * Select the manifest to vote for in the next round after running an
 * "instant runoff" algorithm.
 *
 * @param ledgerNode the ledger node.
 * @param vote the previous round's vote.
 * @param electors the electors for the next block.
 * @param callback(err, manifestHash) called once the operation completes.
 */
function _selectInstantRunoffRollCallManifest(
  ledgerNode, vote, electors, callback) {
  /* Note: If any particular roll call manifest has 51% or more votes, choose
    it. Otherwise, pick the roll call manifest with the greatest number of
    electors (manifest length). Break ties using lexicographically least
    manifest hash. */
  const majority = _majority(electors.length);
  api._storage.votes.tally(
    ledgerNode.id, vote.blockHeight, 'RollCall', vote.voteRound,
    (err, tallies) => {
    if(tallies[0].count >= majority) {
      // there's a majority of votes for a particular manifestHash; select it
      return callback(null, tallies[0].manifestHash);
    }

    // filter tallies by highest count
    const highCount = tallies[0].count;
    tallies = tallies.filter(tally => tally.count === highCount);

    // no majority, choose roll call manifest with greatest number of electors
    api._storage.manifests.getAllByLength(
      ledgerNode.id, vote.blockHeight, 'RollCall', (err, results) => {
      // add manifest length to tallies
      const lengths = {};
      results.forEach(r => lengths[r.manifestHash] = r.length);
      for(let i = 0; i < tallies.length; ++i) {
        const tally = tallies[i];
        if(!tally.manifestHash in lengths) {
          // should never happen; manifests must be stored before votes are
          // stored according to algorithm
          return callback(new BedrockError(
            'Roll call manifest missing.', 'InvalidStateError', {
              manifestHash: tally.manifestHash
            }));
        }
        tally.manifestLength = lengths[tally.manifestHash];
      }

      // sort tallies by manifest length
      tallies.sort((a, b) => a.manifestLength - b.manifestLength);

      // filter tallies by longest manifest
      const highLength = tallies[0].manifestLength;
      tallies = tallies.filter(tally => tally.manifestLength === highLength);

      if(tallies.length > 0) {
        // still no single tally has won, select by manifest hash
        tallies.sort((a, b) => a.localeCompare(b));
      }

      callback(null, tallies[0].manifestHash);
    });
  });
}

function _verifyVote(ledgerNode, vote, electionTopic, callback) {
  // 1. Verify signature on vote.
  _verifyVoteSignature(vote, err => {
    if(err) {
      return callback(err);
    }
    // 2. Obtain (and verify) manifest.
    _getManifest(
      ledgerNode, vote.voter, vote.manifestHash, electionTopic, callback);
  });
}

function _verifyVoteSignature(vote, callback) {
  jsigs.verify(vote, {}, (err, result) => {
    if(err) {
      return callback(err);
    }
    if(!result.verified) {
      return callback(new BedrockError(
        'Vote signature verification failed.',
        'AuthenticationError', {vote, keyResults: result.keyResults}));
    }
    callback();
  });
}

function _getManifest(ledgerNode, peerId, manifestHash, type, callback) {
  // TODO: verify node signature on each event? (not in alpha version)

  // try to get manifest from local storage first
  api._storage.manifests.get(ledgerNode.id, manifestHash, (err, result) => {
    if(err) {
      if(err.name === 'NotFoundError') {
        // local storage does not have manifest, obtain from peer
        return _getManifestFromPeer(
          ledgerNode, peerId, manifestHash, type, callback);
      }
      return callback(err);
    }
    callback(null, result);
  });
}

function _getManifestFromPeer(
  ledgerNode, peerId, manifestHash, type, callback) {
  async.auto({
    manifest: callback => api._client.getManifest(
      manifestHash, peerId, callback),
    neededItems: ['manifest', (results, callback) => {
      const items = results.manifest.item;
      if(type === 'RollCall') {
        return api._storage.votes.difference(ledgerNode.id, items, callback);
      }
      if(type === 'Events') {
        return ledgerNode.storage.events.difference(items, callback);
      }
      callback();
    }],
    checkMajority: ['manifest', (results, callback) => {
      const manifest = results.manifest;
      if(manifest.type !== 'RollCall') {
        return callback();
      }
      // ensure that RollCall manifest contains the required number of votes
      api.validateRollCallManifest(manifest, ledgerNode, callback);
    }],
    sync: ['neededItems', 'checkMajority', (results, callback) => {
      if(type === 'Events') {
        return async.each(results.neededItems, (eventHash, callback) =>
          _getEventFromPeer(ledgerNode, peerId, eventHash, callback), callback);
      }
      if(type === 'RollCall') {
        return async.each(results.neededItems, (voteHash, callback) =>
          _getVoteFromPeer(
            ledgerNode.id, peerId, voteHash, 'Events', callback), callback);
      }
      callback();
    }],
    validate: ['sync', (results, callback) => {
      const manifest = results.manifest;
      if(type === 'Events') {
        return _validateEvents(
          ledgerNode, manifest.item, manifest.blockHeight, callback);
      }

      // type === 'RollCall', we already have the events votes from the peer
      // in the database, so we just have to make sure that those listed in the
      // manifest also exist in our database or else the manifest is bogus
      api._storage.votes.exists(ledgerNode.id, manifest.item, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result === false) {
          return callback(new BedrockError(
            'Some votes in the manifest could not be validated.',
            'ValidationError', {manifest}));
        }
        callback();
      });
    }],
    store: ['validate', (results, callback) => {
      if(type === 'Events' &&
        results.manifest.item.length !== results.validate.hashes.length) {
        // TODO: provide more information about failed events
        return callback(new BedrockError(
          'Some events in the manifest could not be validated.',
          'ValidationError', {
            manifest: results.manifest
          }));
      }
      api._storage.manifests.add(ledgerNode.id, results.manifest, err => {
        if(err && err.name === 'DuplicateError') {
          // this manifest has already happened, via another process,
          // can safely ignore this error and proceed as if we wrote the
          // manifest
          return callback();
        }
        callback(err);
      });
    }]
  }, (err, results) => callback(err, results.manifest));
}

function _getEventFromPeer(ledgerNode, peerId, eventHash, callback) {
  async.auto({
    getEvent: callback => api._client.getEvent(eventHash, peerId, callback),
    hashEvent: ['getEvent', (results, callback) =>
      api._hasher(results.getEvent, callback)],
    addEvent: ['hashEvent', (results, callback) => {
      if(eventHash !== results.hashEvent) {
        return callback(new BedrockError(
          'eventHash does not match the hash provided by the peer.',
          'OperationError', {
            manifest: results.manifest,
            peerEventHash: eventHash,
            localEventHash: results.hashEvent,
            event: results.getEvent
          }));
      }
      ledgerNode.events.add(
        results.getEvent, {
          continuity2017: {peer: true}
        }, callback);
    }]
  }, callback);
}

function _getVoteFromPeer(
  ledgerNodeId, peerId, voteHash, electionTopic, callback) {
  async.auto({
    vote: callback => api._client.getVote(voteHash, peerId, callback),
    hash: ['vote', (results, callback) => api._hasher(results.vote, callback)],
    store: ['hash', (results, callback) => api._storage.votes.add(
      ledgerNodeId, electionTopic, results.vote, {
        meta: {voteHash: results.hash}
      }, err => {
        if(err && err.name === 'DuplicateError') {
          // this vote has already happened, via another process,
          // can safely ignore this error and proceed as if we wrote the vote
          return callback();
        }
        callback(err);
      })]
  }, callback);
}

function _createManifest(ledgerNodeId, blockHeight, type, items, callback) {
  // create manifest hash (EOL delimited) and store manifest
  const hash = niUri.digest('sha-256', items.join('\n'), true);
  const manifest = {
    id: hash,
    type: type,
    blockHeight: blockHeight,
    item: items
  };
  api._storage.manifests.add(ledgerNodeId, manifest, err => {
    if(err && err.name === 'DuplicateError') {
      // ignore duplicate manifest entries; they only mean that another
      // process has already added the same manifest to storage
      err = null;
    }
    callback(null, manifest);
  });
}

function _createVote(ledgerNode, electionTopic, vote, voter, callback) {
  // TODO: add logger.verbose() with the vote information here
  // console.log(voter.id.split('/').pop() + ' votes for ' + electionTopic +
  //   ', manifestHash=' + vote.manifestHash + ', round=' + vote.voteRound);
  async.auto({
    sign: callback => jsigs.sign(vote, {
      algorithm: 'LinkedDataSignature2015',
      privateKeyPem: voter.publicKey.privateKey.privateKeyPem,
      creator: voter.publicKey.id
    }, callback),
    hash: ['sign', (results, callback) => api._hasher(results.sign, callback)],
    store: ['sign', 'hash', (results, callback) => api._storage.votes.add(
      ledgerNode.id, electionTopic, results.sign, {
        meta: {voteHash: results.hash}
      }, err => {
      if(err && err.name === 'DuplicateError') {
        // this vote has already happened, via another process,
        // can safely ignore this error and proceed as if we wrote the vote
        return callback(null, results.sign);
      }
      callback(err, results.sign);
    })]
  }, (err, results) => err ? callback(err) : callback(null, results.store));
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

function _unique(array) {
  return [...new Set(array)];
}

function _sha256(x) {
  return crypto.createHash('sha256').update(x).digest('hex');
}

function _majority(count) {
  // special case when electors < 3 -- every elector must agree.
  return (count < 3) ? count : Math.floor(count / 2) + 1;
}

function _twoThirdsMajority(count) {
  // special case when electors < 3 -- every elector must agree.
  return (count < 3) ? count : Math.floor(count / 3) * 2 + 1;
}

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
