/*
 * Web Ledger Continuity2017 consensus election functions.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const crypto = require('crypto');
const jsonld = require('jsonld');
const niUri = require('ni-uri');
const BedrockError = bedrock.util.BedrockError;

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

api._client = require('./client');
api._storage = require('./storage');
api._voters = require('./voters');

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
    config: callback =>
      ledgerNode.storage.events.getLatestConfig((err, result) => {
      if(err) {
        return callback(err);
      }
      const config = result.event.input[0];
      // FIXME: uncomment once config is updated
      /*if(config.consensusMethod !== 'Continuity2017') {
        return callback(new BedrockError(
          'Consensus method must be "Continuity2017".', 'InvalidStateError', {
            consensusMethod: config.consensusMethod
          }));
      }*/
      callback(null, config);
    }),
    // TODO: `latestBlock` should match up with `blockHeight` - 1... or
    // else there's an error we must throw
    latestBlock: callback => ledgerNode.storage.blocks.getLatest(callback),
    electors: ['config', 'latestBlock', (results, callback) => {
      // get previous votes
      const previousVotes = results.latestBlock.electionResults || [];

      // aggregate recommended electors
      let electors = [];
      previousVotes.forEach(vote => {
        electors.concat(
          jsonld.getValues(vote, 'recommendedElectors'));
      });
      let aggregate = {};
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

      // get elector count, defaulting to 10 if not set (hardcoded, all nodes
      // must do the same thing -- but ideally this would *always* be set)
      const electorCount = results.config.electorCount || 10;

      // compute super majority requirement
      const twoThirds = _twoThirdsMajority(electorCount.length);

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

          // 3. sort by hex hash(manifest + voter.id)
          // TODO: cache hashes to avoid recomputation
          return _sha256(a).localCompare(_sha256(b));
        });

        // select first `electorCount` electors
        electors = electors.slice(0, electorCount + 1);
      }

      // TODO: if there were no electors chosen or insufficient electors,
      // add electors from config
      // FIXME: temporarily adding voter that was passed in, REMOVE
      if(electors.length < electorCount) {
        return api._voters.get(ledgerNode.id, (err, voter) => {
          if(err) {
            return callback(err);
          }
          callback(null, [voter]);
        });
      }

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
  async.each(votes, (vote, callback) => {
    _verifyVote(vote, err => {
      if(err) {
        // ignore failed votes, do not store them
        return callback();
      }
      // TODO: reject votes with more than X number of recommended electors
      // based on continuity config
      api._storage.votes.add(
        ledgerNode.id, electionTopic, vote, err => callback(err));
    });
  }, callback);
};

/**
 * Tallies a vote for the passed block height. If there is a winning manifest
 * for the block, its hash will be returned, otherwise `null` will be returned.
 *
 * @param ledgerNodeId the ID of the ledger node that is tallying votes.
 * @param blockHeight the height of the block to tally the vote for.
 * @param electors the voting population for the block height.
 * @param electionType the type of votes to tally ('EventOrder' or 'RollCall').
 * @param callback(err, manifestHash) called once the operation completes.
 */
api.tally = (ledgerNodeId, blockHeight, electors, electionType, callback) => {
  /* Go through rounds and see if 2/3rds have voted yet in the round. If not,
    then finish the tally because we need to wait for more votes to arrive. If
    so, and a 2/3rds majority has voted for a particular manifest, finish tally
    and return the winner. */
  let done = false;
  let round = 1;
  const twoThirds = _twoThirdsMajority(electors.length);
  let winner = null;
  async.until(() => done, callback => {
    api._storage.votes.tally(
      ledgerNodeId, blockHeight, electionType, round++, (err, votes) => {
      if(err) {
        return callback(err);
      }
      if(votes.length < twoThirds) {
        // not enough votes this round, break out to collect more votes
        done = true;
      } else if(votes[0].count >= twoThirds) {
        // we have a winner in this round
        done = true;
        winner = votes[0].manifestHash;
      }
      callback();
    });
  }, err => callback(err, winner));
};

/**
 * Performs a vote for the ledger node identified by the `voter` information.
 * The vote will be for a manifest to use to create the next block which is
 * identified by `blockHeight`.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the next block.
 * @param electors all electors for the next block.
 * @param callback(err) called once the operation completes.
 */
api.voteForEvents = (ledgerNode, voter, blockHeight, electors, callback) => {
  // get last vote cast by voter for the block
  api._storage.votes.getLast(
    ledgerNode.id, blockHeight, 'Events', voter.id, (err, vote) => {
    if(err) {
      return callback(err);
    }
    if(vote) {
      // previous vote has occurred, so do instant run-off and vote again
      return _createInstantRunoffEventVote(
        ledgerNode, vote, voter, electors, callback);
    }
    _createFirstRoundEventVote(
      ledgerNode, voter, blockHeight, electors, callback);
  });
};

/**
 * Performs a vote for the ledger node identified by the `voter` information.
 * The vote will be for a manifest to use to determine the roll call for
 * the previous election of the events for the next block.
 *
 * @param ledgerNode the ledger node.
 * @param voter the voter information for the ledger node.
 * @param blockHeight the height of the next block.
 * @param callback(err) called once the operation completes.
 */
api.voteForRollCall = (ledgerNode, voter, blockHeight, electors, callback) => {
  // get last vote cast by voter for the block
  api._storage.votes.getLast(
    ledgerNode.id, blockHeight, 'RollCall', voter.id, (err, vote) => {
    if(err) {
      return callback(err);
    }
    if(vote) {
      // previous vote has occurred, so do instant run-off and vote again
      return _createInstantRunoffRollCallVote(
        ledgerNode, vote, voter, electors, callback);
    }
    _createFirstRoundRollCallVote(ledgerNode, voter, blockHeight, callback);
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
    _recommendElectors(
      ledgerNode, voter, electors, manifest, (err, recommendedElectors) => {
      if(err) {
        return callback(err);
      }
      // create vote for manifest
      const vote = {
        // TODO: add `@context`
        blockHeight: blockHeight,
        manifestHash: manifest.id,
        round: 1,
        voter: voter.id,
        recommendedElectors: recommendedElectors
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
  ledgerNode.storage.events.getHashes({
    consensus: false,
    // limit: x,
    sort: 1
  }, (err, hashes) => {
    if(err) {
      return callback(err);
    }
    if(hashes.length === 0) {
      // no events to vote on, don't create empty manifest
      return callback(null, null);
    }
    _createManifest(ledgerNode.id, blockHeight, 'Events', hashes, callback);
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
  // TODO: use given parameters and blockchain to determine a set of
  // electors to recommend for the next block

  // FIXME: temporary mocked version reuses existing electors
  callback(null, electors.map(elector => {id: elector.id}));
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
      round: vote.round + 1
    });
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
  let majority = _majority(electors.length);
  api._storage.votes.tally(
    ledgerNode.id, vote.blockHeight, 'Events', vote.round, (err, tallies) => {
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
        ledgerNode.id, vote.blockHeight, 'Events', eventHashes, callback);
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
 * @param callback(err) called once the operation completes.
 */
function _createFirstRoundRollCallVote(
  ledgerNode, voter, blockHeight, callback) {
  _createRollCallManifest(ledgerNode, blockHeight, (err, manifest) => {
    if(err) {
      return callback(err);
    }
    // create vote for manifest
    const vote = {
      // TODO: add `@context`
      blockHeight: blockHeight,
      manifestHash: manifest.id,
      round: 1,
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
 * @param callback(err, manifestHash) called once the operation completes.
 */
function _createRollCallManifest(ledgerNode, blockHeight, callback) {
  api._storage.votes.get(
    ledgerNode.id, blockHeight, 'Events', (err, votes) => {
    if(err) {
      return callback(err);
    }
    if(votes.length === 0) {
      // no votes, don't create empty manifest
      return callback(null, null);
    }
    votes = votes.map(vote => vote.id);
    _createManifest(ledgerNode.id, blockHeight, 'RollCall', votes, callback);
  });
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
      round: vote.round + 1
    });
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
  let majority = _majority(electors.length);
  api._storage.votes.tally(
    ledgerNode.id, vote.blockHeight, 'RollCall', vote.round, (err, tallies) => {
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
        tallies.sort((a, b) => a.localCompare(b));
      }

      callback(null, tallies[0].manifestHash);
    });
  });
}

function _verifyVote(vote, callback) {
  // 1. Verify signature on vote.
  _verifyVoteSignature(vote, err => {
    if(err) {
      return callback(err);
    }
    // 2. Obtain (and verify) manifest.
    _getManifest(vote.id, vote.manifest, callback);
  });
}

function _verifyVoteSignature(vote, callback) {
  // TODO: implement
  return callback();
  callback(new BedrockError(null, 'NotImplemented'));
}

function _getManifest(voterId, manifest, callback) {
  // TODO: check storage for manifest first, if not present, obtain from peer

  // TODO: get `peer` via voter ID

  // TODO: download manifest from peer, use queue to optimize both the
  // retrieval of the manifest and its events
  // TODO: would be more efficient to get a list of all event hashes across
  // manifests? but that does not check manifest hashes... and may be
  // less efficient during voting

  // GET <endpoint>/manifests/<manifestHash>

  // {
  //   eventHash: [<eventHash1>, <eventHash2>, ...]
  // }

  // For each unknown <eventHash>, fetch full event:

  // GET <endpoint>/events/<eventHash>

  // { full event }

  // TODO: verify each event? (as in, do we need another layer of signatures
  // from nodes on each event, or will the hashes do?... do we only need
  // signatures for the voting part?)

  callback(new BedrockError(null, 'NotImplemented'));
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
  // TODO: replace with real jsigs sign
  function jsigsMockSign(vote, voter, callback) {
    callback(null, vote);
  }

  jsigsMockSign(vote, voter, (err, signedVote) => {
    if(err) {
      return callback(err);
    }
    api._storage.votes.add(
      ledgerNode.id, electionTopic, signedVote, err => {
      if(err && err.name === 'DuplicateError') {
        // this vote has already happened, via another process,
        // can safely ignore this error and proceed as if we wrote the vote
        callback(null, signedVote);
      }
      callback(err, signedVote);
    });
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
  return (count < 3) ? count : Math.floor(count / 3) * 2;
}
