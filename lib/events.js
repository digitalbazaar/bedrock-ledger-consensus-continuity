/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _storage = require('./storage');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const config = bedrock.config;
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const continuityStorage = require('./storage');
const database = require('bedrock-mongodb');
const hasher = brLedgerNode.consensus._hasher;
const jsigs = require('jsonld-signatures')();
const jsonld = bedrock.jsonld;
const logger = require('./logger');
const signature = require('./signature');
const validate = require('bedrock-validation').validate;
const voters = require('./voters');
const zlib = require('zlib');
const BedrockError = bedrock.util.BedrockError;

const api = {};
module.exports = api;

jsigs.use('jsonld', brDidClient.jsonld);

/**
 * Adds a new event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 * @param options the options to use.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (event, ledgerNode, options, callback) => {
  if(typeof options === 'function') {
    callback = options;
    options = {};
  }
  const ledgerNodeId = ledgerNode.id;
  const storage = ledgerNode.storage;
  _writeEvent(ledgerNode, event, options, (err, eventRecord) => {
    if(err) {
      return callback(err);
    }
    if(!options.genesis) {
      // no need to create genesis block, return early
      return callback(null, eventRecord);
    }

    // need to write the genesis block, either from `options.genesisBlock`
    // to mirror an existing ledger, or create it ourselves for a new ledger
    const now = Date.now();
    async.auto({
      localCreatorId: callback => voters.get(ledgerNodeId, (err, result) =>
        err ? callback(err) : callback(null, result.id)),
      // add indexes specific to Continuity
      createIndexes: ['localCreatorId', (results, callback) => {
        const collection = storage.events.collection.s.name;
        database.createIndexes([{
          collection,
          fields: {
            'event.treeHash': 1,
            'event.type': 1,
            'meta.continuity2017.creator': 1
          },
          options: {background: false, name: 'continuity1'}
        }, {
          // not for searches but to ensure that local node never forks
          collection,
          fields: {
            // 'meta.continuity2017.creator': 1,
            'meta.continuity2017.generation': 1
          },
          options: {
            unique: true, background: false, name: 'continuity2',
            partialFilterExpression: {
              'event.type': 'ContinuityMergeEvent',
              'meta.continuity2017.creator': results.localCreatorId
            }
          }
        }, {
          collection,
          fields: {
            'meta.continuity2017.creator': 1,
            'meta.continuity2017.generation': 1,
            'eventHash': 1,
            'meta.deleted': 1,
          },
          options: {
            sparse: true, unique: false, background: false, name: 'continuity3'
          }
        }, {
          collection,
          fields: {
            'event.type': 1,
            'meta.continuity2017.generation': 1,
            'meta.continuity2017.creator': 1,
            'eventHash': 1,
          },
          options: {
            sparse: true, unique: false, background: false, name: 'continuity4'
          }
        }], callback);
      }],
      mergeEvent: callback => {
        if(options.genesisBlock) {
          const event = options.genesisBlock.consensusProof[0];
          return signature.verify(
            {doc: event, ledgerNodeId}, (err, result) => {
              if(err) {
                return callback(err);
              }
              callback(null, {creator: result.keyOwner, event});
            });
        }
        _genesisProofCreate(
          {eventHash: eventRecord.meta.eventHash, ledgerNode}, callback);
      },
      eventHash: ['mergeEvent', (results, callback) =>
        hasher(results.mergeEvent.event, callback)],
      mergeEventRecord: ['eventHash', (results, callback) => {
        const genesisMergeCreatorId = results.mergeEvent.creator.id;
        const {eventHash, mergeEvent} = results;
        const meta = {
          consensus: true,
          consensusDate: now,
          continuity2017: {creator: genesisMergeCreatorId, generation: 0},
          eventHash,
        };
        storage.events.add(mergeEvent.event, meta, callback);
      }],
      createBlock: ['mergeEventRecord', (results, callback) => {
        if(options.genesisBlock) {
          // use given genesis block, mirroring an existing ledger
          return callback(null, options.genesisBlock);
        }
        // create genesis block, creating a new ledger
        const block = {
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          id: eventRecord.event.ledgerConfiguration.ledger + '/blocks/0',
          type: 'WebLedgerEventBlock',
          consensusMethod: 'Continuity2017',

          // FIXME: block needs to be hashed with event documents, not hashes
          // but the block *should* be stored with just hashes
          // event: [eventRecord.meta.eventHash],
          // consensusProof: [results.mergeEventRecord.meta.eventHash],

          event: [eventRecord.event],
          consensusProof: [results.mergeEventRecord.event],
          blockHeight: 0,
          publicKey: [{
            id: results.mergeEvent.creator.publicKey.id,
            type: 'Ed25519VerificationKey2018',
            owner: results.mergeEvent.creator.id,
            publicKeyBase58:
              results.mergeEvent.creator.publicKey.publicKeyBase58
          }]
        };
        callback(null, block);
      }],
      // FIXME: is meta.created/timestamp on genesisBlock an important
      // consideration?
      writeBlock: ['createBlock', (results, callback) => _writeGenesisBlock(
        storage, results.createBlock, callback)],
      // mark event has having achieved consensus
      updateEvent: ['writeBlock', (results, callback) => storage.events.update(
        eventRecord.meta.eventHash, [{
          op: 'set',
          changes: {
            meta: {
              consensus: true,
              consensusDate: now
            }
          }
        }], callback)],
      updateKey: ['writeBlock', (results, callback) => {
        const block = results.createBlock;
        const key = Object.assign({}, block.publicKey[0]);
        key['@context'] = config.constants.WEB_LEDGER_CONTEXT_V1_URL;
        key.seeAlso = block.id;
        continuityStorage.keys.addPublicKey(ledgerNode.id, key, err => {
          if(err && err.name === 'DuplicateError') {
            // attempt to update key instead
            return continuityStorage.keys.updatePublicKey(
              ledgerNode.id, key, callback);
          }
          callback(err);
        });
      }]
    }, err => err ? callback(err) : callback(null, eventRecord));
  });
};

api.aggregateHistory = ({
  creatorFilter, creatorRestriction, eventTypeFilter, fullEvent = false,
  ledgerNode, startHash
}, callback) => {
  logger.debug('Start aggregateHistory');
  const collection = ledgerNode.storage.events.collection;
  const restrictSearchWithMatch = {$nor: []};
  if(creatorRestriction && creatorRestriction.length !== 0) {
    creatorRestriction.forEach(r => restrictSearchWithMatch.$nor.push({
      $and: [
        // regular events *should* be included, regardless of created
        {'event.type': 'ContinuityMergeEvent'},
        {'meta.continuity2017.creator': r.creator},
        {'meta.continuity2017.generation': {$lte: r.generation}},
      ]
    }));
  }

  if(Array.isArray(creatorFilter) && creatorFilter.length !== 0) {
    restrictSearchWithMatch.$nor.push(
      {'meta.continuity2017.creator': {$in: creatorFilter}});
  }

  // if(Array.isArray(creatorFilter) && creatorFilter.length !== 0) {
  //   restrictSearchWithMatch.$nor.push({
  //     $and: [
  //       {'meta.continuity2017.creator': {$in: creatorFilter}},
  //       {'event.type': 'ContinuityMergeEvent'}
  //     ]
  //   });
  // }
  if(eventTypeFilter) {
    // FIXME: filtering for ONLY WebLedgerOperationEvent (regular event)
    // involves special syntax because because `event.type` can be a string
    // or array
    restrictSearchWithMatch['event.type'] = eventTypeFilter;
  }

  // genesisMerge should never be included in the history
  restrictSearchWithMatch.$nor.push({
    'meta.continuity2017.generation': 0
  });

  // used by server to give only hashes of merge events
  if(!fullEvent) {
    return collection.aggregate([{
      $match: {eventHash: startHash},
    }, {
      $graphLookup: {
        from: collection.s.name,
        startWith: '$eventHash',
        connectFromField: 'event.parentHash',
        connectToField: 'eventHash',
        as: '_parents',
        restrictSearchWithMatch
      }
    },
    {$project: {_id: 0, _parents: 1, eventHash: 1, 'meta.created': 1}},
    {$unwind: '$_parents'},
    {$replaceRoot: {newRoot: '$_parents'}},
    // the order of events is unpredictable without this sort, and we
    // must ensure that events are added in chronological order
    {$sort: {'meta.created': 1}},
    // $push must be used here instead of $addToSet to maintain sort order
    {$group: {_id: null, mergeEventHashes: {$push: '$eventHash'}}}
    ], {allowDiskUse: true}).toArray((err, result) => {
      if(err) {
        return callback(err);
      }
      if(result.length === 0) {
        return callback(null, []);
      }
      callback(null, result[0].mergeEventHashes);
    });
  }
  // used for push gossip
  // console.log('TTTTTTTt', JSON.stringify(restrictSearchWithMatch, null, 2));

  // console.log('XXXXx', JSON.stringify(restrictSearchWithMatch, null, 2));

  collection.aggregate([{
    $match: {eventHash: startHash},
  }, {
    $graphLookup: {
      from: collection.s.name,
      startWith: '$eventHash',
      connectFromField: 'event.parentHash',
      connectToField: 'eventHash',
      as: '_parents',
      restrictSearchWithMatch
    }
  },
  {$unwind: '$_parents'},
  {$replaceRoot: {newRoot: '$_parents'}},
  // the order of events is unpredictable without this sort, and we
  // must ensure that events are added in chronological order
  // {$sort: {'meta.created': 1}},
  ], {allowDiskUse: true}).toArray((err, result) => {
    if(err) {
      return callback(err);
    }

    if(result.length === 0) {
      return callback(null, []);
    }
    // push is more efficient, the array will be reversed later
    const hashMap = {};
    result.forEach(r => {
      r._children = [];
      hashMap[r.eventHash] = r;
    });

    result.forEach(r => {
      r.event.parentHash.forEach(h => {
        if(!hashMap[h]) {
          return;
        }
        // dups here?
        hashMap[h]._children.push(r.eventHash);
      });
    });

    const sortedHistory = [];
    const S = [];
    // this moves startHash into S
    S.push(..._.remove(result, (e => e.eventHash === startHash)));
    while(S.length > 0) {
      const n = S.pop();
      sortedHistory.push(n);
      // remove edge e from the graph
      result.forEach(e => _.pull(e._children, n.eventHash));
      // if m has no other incoming edges then insert m into S
      S.push(..._.remove(result, e => e._children.length === 0));
    }
    if(result.length !== 0) {
      throw new Error('matt');
    }
    // reverse the history
    sortedHistory.reverse();
    callback(null, sortedHistory);
  });
};

api.compressedHistory = (
  {creatorHeads, creatorId, maxEvents, peerId}, callback) => {
  async.auto({
    partition: callback => api.partitionHistory({
      creatorHeads, creatorId, fullEvent: true, peerId
    }, callback),
    compress: ['partition', (results, callback) => {
      if(results.partition.history.length === 0) {
        return callback(new BedrockError(
          'No events match the query.',
          'NotFoundError', {
            httpStatusCode: 404,
            public: true,
          }));
      }
      const events = results.partition.history
        .map(h => ({event: h.event, eventHash: h.eventHash}));
      api.compressEvents({events, maxEvents}, callback);
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.compress));
};

api.headDifference = ({creatorHeads, eventsCollection}, callback) => {
  // if the event does not exist here, we can assume that the caller is
  // ahead on this branch, so we can exclude all events by the creator
  const headMap = {};
  Object.keys(creatorHeads).forEach(creatorId => {
    headMap[creatorHeads[creatorId]] = creatorId;
  });
  const eventHashes = Object.keys(headMap);
  eventsCollection.find({
    eventHash: {$in: eventHashes}}, {_id: 0, eventHash: 1}
  ).forEach(e => delete headMap[e.eventHash], err => {
    err ? callback(err) : callback(null, headMap);
  });
};

// called from consensus worker
api.getRecentHistory = (
  {creatorId, ledgerNode, excludeLocalRegularEvents = false, link = false},
  callback) => {
  logger.debug('Start getRecentHistory');
  const startTime = Date.now();
  const eventMap = {};
  const events = [];
  const storage = ledgerNode.storage;
  async.auto({
    creator: callback => {
      if(creatorId) {
        return callback();
      }
      voters.get(ledgerNode.id, (err, result) => {
        if(err) {
          return callback(err);
        }
        creatorId = result.id;
        callback();
      });
    },
    treeHash: ['creator', (results, callback) => api._getLocalBranchHead({
      eventsCollection: storage.events.collection,
      creatorId,
      ledgerNodeId: ledgerNode.id
    }, (err, result) => {
      if(err) {
        return callback(err);
      }
      if(!result) {
        return callback(new BedrockError(
          'Could not locate branch head.',
          'InvalidStateError', {
            httpStatusCode: 400,
            public: true,
          }));
      }
      callback(null, result);
    })],
    events: callback => {
      const query = {'meta.consensus': {$exists: false}};
      if(excludeLocalRegularEvents) {
        // simply get all merge events
        _.assign(query, {
          'event.type': 'ContinuityMergeEvent',
          'meta.continuity2017.generation': {$exists: true},
          'meta.continuity2017.creator': {$exists: true},
        });
      } else {
        // get all events from creator and only merge events from non-creator
        query.$or = [{
          'meta.continuity2017.creator': creatorId
        }, {
          'event.type': 'ContinuityMergeEvent',
          'meta.continuity2017.generation': {$exists: true},
          'meta.continuity2017.creator': {$ne: creatorId},
        }];
      }

      // FIXME: what should happen if no results are found here?
      const projection = {
        _id: 0,
        eventHash: 1,
        'meta.continuity2017': 1,
        'event.parentHash': 1,
        'event.treeHash': 1,
        'event.type': 1
      };
      storage.events.collection.find(query, projection)
        .forEach(doc => {
          doc._children = [];
          // TODO: remove _parents if it is unused
          doc._parents = [];
          events.push(doc);
          eventMap[doc.eventHash] = doc;
        }, callback);
    },
    process: ['treeHash', 'events', (results, callback) => {
      const history = {events, eventMap, localBranchHead: results.treeHash};
      if(link) {
        // TODO: move to a utility function
        // build history links
        for(const e of events) {
          for(const parentHash of e.event.parentHash) {
            const parent = eventMap[parentHash];
            if(!parent) {
              continue;
            }
            e._parents.push(parent);
            parent._children.push(e);
          }
        }
        history.linked = true;
      }
      logger.debug('End getRecentHistory', {
        duration: Date.now() - startTime
      });
      callback(null, history);
    }],
  }, (err, results) => callback(err, results.process));
};

// FIXME: documentation
// NOTE: mutates history
api.mergeBranches = ({ledgerNode, history}, callback) => {
  if(ledgerNode === undefined || history === undefined) {
    throw new TypeError('`ledgerNode` and `history` are required.');
  }
  const ledgerNodeId = ledgerNode.id;

  if(history.events.length === 0) {
    // nothing to do
    return callback();
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

  // determine if there are events with no children that can be merged
  const eventsWithNoChildren = history.events
    .filter(e => !e._children.length);
  if(eventsWithNoChildren.length === 1 &&
    eventsWithNoChildren[0].eventHash === history.localBranchHead.eventHash) {
    // nothing to merge, return `null`
    logger.debug('mergeBranches nothing to merge.');
    return callback(null, null);
  }

  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
  };

  // if no local events have been added, it is correct that
  // localBranchHead would be included along with events from other nodes
  const parentHashes = eventsWithNoChildren.map(e => e.eventHash);
  // TODO: make limit configurable
  // limit number of parents
  parentHashes.length = Math.min(250, parentHashes.length);

  event.treeHash = history.localBranchHead.eventHash;
  if(parentHashes.includes(event.treeHash)) {
    event.parentHash = parentHashes;
  } else {
    event.parentHash = [event.treeHash, ...parentHashes];
  }
  const nextGeneration = history.localBranchHead.generation + 1;
  async.auto({
    // FIXME: maybe creator will be supplied as a parameter?
    creator: callback => voters.get(ledgerNodeId, callback),
    sign: ['creator', (results, callback) => jsigs.sign(event, {
      algorithm: 'Ed25519Signature2018',
      privateKeyBase58: results.creator.publicKey.privateKey.privateKeyBase58,
      creator: results.creator.publicKey.id
    }, callback)],
    eventHash: ['sign', (results, callback) => hasher(results.sign, callback)],
    // local merge events must be written directly to the database
    write: ['eventHash', (results, callback) => {
      const {eventHash, creator} = results;
      const meta = {continuity2017: {
        creator: creator.id,
        generation: nextGeneration
      }, eventHash};
      ledgerNode.storage.events.add(results.sign, meta, callback);
    }],
    cache: ['write', (results, callback) => {
      const creatorId = results.creator.id;
      const headGenerationKey = _cacheKey.headGeneration(
        {creatorId, ledgerNodeId});
      const headKey = _cacheKey.head({creatorId, ledgerNodeId});
      cache.client.mset(
        headGenerationKey, nextGeneration, headKey, results.eventHash, err => {
          if(err) {
            // FIXME: fail gracefully
            // failure here means head information would be corrupt which
            // cannot be allowed
            logger.error('Could not set head.', {
              creatorId,
              error: err,
              headGenerationKey,
              headKey,
              ledgerNodeId,
              nextGeneration,
            });
            throw err;
          }
          callback();
        });
    }],
  // results are not being utillized
  }, err => callback(err));
};

api.getCreatorHeads = (
  {ledgerNode, peerCreators, peerId}, callback) => async.auto({
  // get ids for all the creators known to the node
  localCreators: callback => database.collections.continuity2017_key.find(
    {ledgerNodeId: ledgerNode.id}, {_id: 0, 'publicKey.owner': 1}
  ).toArray((err, result) =>
    err ? callback(err) : callback(null, result.map(k => k.publicKey.owner))),
  heads: ['localCreators', (results, callback) => {
    // this should never happen, the node attempts to contact nodes based
    // on evens from blocks, and therefore the key should be in the cache
    // however, in tests, nodes are instructed to gossip with peers that
    // they have never heard of before, the head for this peer will end
    // up being the genesis merge event
    if(peerId && !results.localCreators.includes(peerId)) {
      // FIXME: can this be optimized to just set head to genesisMergeHash?
      results.localCreators.push(peerId);
    }
    const combinedCreators = peerCreators ?
      _.uniq([...peerCreators, ...results.localCreators]) :
      results.localCreators;
    // get the branch heads for every creator
    const creatorHeads = {};
    async.each(combinedCreators, (creatorId, callback) =>
      api._getLocalBranchHead({creatorId, ledgerNode}, (err, result) => {
        if(err) {
          return callback(err);
        }
        creatorHeads[creatorId] = result;
        callback();
      }), err => err ? callback(err) : callback(null, creatorHeads));
  }],
}, callback);

// used in server and for push gossip
api.partitionHistory = ({
  creatorHeads, creatorId, eventTypeFilter, fullEvent = false, peerId
}, callback) => {
  const _creatorHeads = bedrock.util.clone(creatorHeads);
  // NOTE: it is normal for creatorHeads not to include creatorId (this node)
  // if this node has never spoken to the peer before

  // *do not!* remove the local creator from the heads
  // delete _creatorHeads[creatorId];
  async.auto({
    // FIXME: voter and ledgerNode can be passed in some cases
    voter: callback => _storage.voters.get({voterId: creatorId}, callback),
    ledgerNode: ['voter', (results, callback) =>
      brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
    genesisMerge: ['ledgerNode', (results, callback) =>
      api.getGenesisMergeSummary({ledgerNode: results.ledgerNode}, callback)],
    // FIXME: use an API
    creatorFilter: ['ledgerNode', (results, callback) => {
      // returns a map {<eventHash>: <creatorId>} which only contains
      // hashes for events that this node does not know about
      const creatorFilter = [];
      const _heads = bedrock.util.clone(creatorHeads);
      // no need to check local creator
      delete _heads[creatorId];

      // automatic filter on peerId
      creatorFilter.push(peerId);
      delete _heads[peerId];

      api.headDifference({
        creatorHeads: _heads,
        eventsCollection: results.ledgerNode.storage.events.collection,
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        creatorFilter.push(..._.values(result));
        callback(null, creatorFilter);
      });
    }],
    localHeads: ['ledgerNode', (results, callback) => api.getCreatorHeads({
      ledgerNode: results.ledgerNode, peerCreators: Object.keys(creatorHeads),
    }, callback)],
    startHash: ['localHeads', 'genesisMerge', (results, callback) => {
      const localNodeHead = results.localHeads.heads[creatorId].eventHash;
      return callback(null, localNodeHead);
      // // TODO: make this configurable
      // const maxDepth = 15;
      // const peerLocalNodeHead = _creatorHeads[creatorId] ||
      //   results.genesisMerge.eventHash;
      // console.log('11111111111', localNodeHead);
      // console.log('22222222222', peerLocalNodeHead);
      // if(peerLocalNodeHead === localNodeHead) {
      //   return callback(new BedrockError(
      //     'Peer has provided current local head.  Nothing to do.',
      //     'AbortError', {localNodeHead, public: true}));
      // }
      // const collection = results.ledgerNode.storage.events.collection;
      // collection.aggregate([
      //   {$match: {eventHash: peerLocalNodeHead}},
      //   {$graphLookup: {
      //     from: collection.s.name,
      //     startWith: '$eventHash',
      //     connectFromField: 'eventHash',
      //     connectToField: 'event.treeHash',
      //     as: '_ancestors',
      //     // this is the key to limiting size of history
      //     maxDepth,
      //     restrictSearchWithMatch: {
      //       'event.type': 'ContinuityMergeEvent',
      //       'meta.continuity2017.creator': creatorId
      //     }
      //   }},
      //   {$project: {_id: 0, _ancestors: 1, eventHash: 1}},
      //   {$unwind: '$_ancestors'},
      //   {$replaceRoot: {newRoot: '$_ancestors'}},
      //   {$sort: {'meta.created': 1}},
      //   {$group: {_id: null, _mergeEventHashes: {$push: '$eventHash'}}},
      // ], (err, result) => {
      //   if(err) {
      //     return callback(err);
      //   }
      //   // result will always have length 1
      //   const mergeHistory = result[0]._mergeEventHashes;
      //   // if the current local head is within maxDepth, use it
      //   if(mergeHistory.includes(localNodeHead)) {
      //     return callback(null, localNodeHead);
      //   }
      //   // the peer is more than maxDepth behind, give them last hash within
      //   // maximum depth
      //   const computedHead = mergeHistory[mergeHistory.length - 1];
      //   logger.debug('Peer is catching up. Truncating history.', {
      //     computedHead,
      //     localNodeHead,
      //     peerLocalNodeHead,
      //   });
      //   callback(null, computedHead);
      // });
    }],
    history: ['creatorFilter', 'startHash', (results, callback) => {
      const ledgerNode = results.ledgerNode;

      // aggregate search starts with the local head
      const startHash = results.startHash;

      // if local node knows about a head that the peer does not, set head
      // to genesis for that creator
      // NOTE: if this is the first contact with the peer, the head
      // for the local node will get filled in here as well

      // FIXME: maybe not need since peer has provided generation info
      // results.localHeads.localCreators.forEach(c => {
      //   if(!_creatorHeads[c]) {
      //     _creatorHeads[c] = results.genesisMerge;
      //   }
      // });

      // creator filter (if needed), needs to be reimplemented
      // const creatorFilter = results.creatorFilter;
      // // remove the creators in the filter from the list of heads
      // creatorFilter.forEach(c => delete _creatorHeads[c]);

      // remove the head for the peer
      const creatorRestriction = Object.keys(_creatorHeads).map(c => ({
        creator: c, generation: _creatorHeads[c].generation
      }));
      api.aggregateHistory({
        // creatorFilter,
        // creatorRestriction,
        creatorId,
        // NOTE: server filters continuity merge event, push does not
        eventTypeFilter,
        fullEvent,
        ledgerNode,
        startHash,
      }, callback);
    }],
  }, (err, results) => {
    // do not pass AbortError up the chain
    if(err && err.name !== 'AbortError') {
      return callback(err);
    }
    callback(null, {
      creatorHeads: results.localHeads.heads,
      // return [] incase of AbortError
      history: results.history || []
    });
  });
};

// FIXME: make this a regular API call.  It's used in worker.js as well.
// and use an appropriate name since
// this is not just for local branch, this finds the latest merge event from
// the specified creator
api._getLocalBranchHead = ({creatorId, ledgerNode}, callback) => {
  const ledgerNodeId = ledgerNode.id;
  const eventsCollection = ledgerNode.storage.events.collection;
  const headCacheKey = _cacheKey.head({creatorId, ledgerNodeId});
  const headGenerationCacheKey = _cacheKey.headGeneration(
    {creatorId, ledgerNodeId});
  async.auto({
    headCache: callback => cache.client.mget(
      headCacheKey, headGenerationCacheKey, (err, result) => {
        if(err) {
          return callback(err);
        }
        // redis returns null if key is not found
        if(result[0] === null) {
          return callback();
        }
        callback(null, {
          eventHash: result[0],
          generation: parseInt(result[1], 10)
        });
      }),
    localMerge: ['headCache', (results, callback) => {
      if(results.headCache) {
        return callback(null, results.headCache);
      }
      // find the latest merge event
      const query = {
        'event.type': 'ContinuityMergeEvent',
        'meta.deleted': {$exists: false},
        'meta.continuity2017.creator': creatorId,
        'meta.continuity2017.generation': {$exists: true}
      };
      const projection = {
        _id: 0, 'eventHash': 1, 'meta.continuity2017.generation': 1
      };
      eventsCollection.find(query, projection)
        .sort({'meta.continuity2017.generation': -1})
        .limit(1).toArray((err, result) => {
          if(err) {
            return callback(err);
          }
          if(result.length === 0) {
            return callback();
          }
          const eventHash = result[0].eventHash;
          const generation = result[0].meta.continuity2017.generation;
          cache.client.mset(
            headCacheKey, eventHash,
            headGenerationCacheKey, generation,
            err => {
              if(err) {
                return callback(err);
              }
              callback(null, {eventHash, generation});
            });
        });
    }],
    genesisMerge: ['localMerge', (results, callback) => {
      if(results.localMerge) {
        return callback();
      }
      api.getGenesisMergeSummary({ledgerNode}, callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    results.localMerge ? callback(null, results.localMerge) :
      callback(null, results.genesisMerge);
  });
};

api.getGenesisMergeSummary = ({ledgerNode}, callback) => {
  const eventsCollection = ledgerNode.storage.events.collection;
  const genesisCacheKey = _cacheKey.genesis(ledgerNode.ledger);
  async.auto({
    genesisCache: callback => cache.client.get(
      genesisCacheKey, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return callback();
        }
        // generation for genesis is always zero
        callback(null, {eventHash: result, generation: 0});
      }),
    genesisMerge: ['genesisCache', (results, callback) => {
      if(results.genesisCache) {
        return callback(null, results.genesisCache);
      }
      const query = {'event.type': 'ContinuityMergeEvent'};
      const projection =
        {_id: 0, 'eventHash': 1, 'meta.continuity2017.generation': 1};
      eventsCollection.find(query, projection)
        .sort({'meta.continuity2017.generation': 1}).limit(1)
        .toArray((err, result) => {
          if(err) {
            return callback(err);
          }
          if(result.length === 0 ||
            result[0].meta.continuity2017.generation !== 0) {
            return callback(new BedrockError(
              'The genesis merge event was not found.',
              'InvalidStateError', {
                httpStatusCode: 400,
                public: true,
              }));
          }
          callback(null, {
            eventHash: result[0].eventHash,
            generation: 0
          });
        });
    }],
    setCache: ['genesisMerge', (results, callback) => {
      if(results.genesisCache) {
        return callback();
      }
      cache.client.set(
        genesisCacheKey, results.genesisMerge.eventHash, callback);
    }]
  }, (err, results) => err ? callback(err) :
    callback(null, results.genesisMerge)
  );

};

// NOTE: callback is optional
api.processCompressed = ({creatorId, eventsGz}, callback) => {
  callback = callback || function() {};
  async.auto({
    unzip: callback => zlib.gunzip(eventsGz, (err, result) => {
      if(err) {
        return callback(err);
      }
      let events;
      try {
        events = JSON.parse(result.toString('utf8'));
      } catch(e) {
        return callback(new BedrockError(
          'Parsing error.', 'NetworkError', {}, e));
      }
      callback(null, events);
    }),
    voter: ['unzip', (results, callback) =>
      _storage.voters.get({voterId: creatorId}, callback)],
    ledgerNode: ['voter', (results, callback) =>
      brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
    process: ['ledgerNode', (results, callback) => {
      const ledgerNode = results.ledgerNode;
      logger.debug('Processing compressed events.', {
        eventCount: results.unzip.length
      });
      // using series to ensure that events are stored in the proper order
      async.eachSeries(results.unzip, (e, callback) => async.auto({
        queue: callback => cache.client.exists(_cacheKey.event(
          {eventHash: e.eventHash, ledgerNodeId: ledgerNode.id}), callback),
        exists: ['queue', (results, callback) => {
          if(results.queue === 1) {
            // the event was found in the queue
            // track number of dups per minute , do not wait for callback
            cache.client.incr(`dup-${Math.round(Date.now() / 60000)}`);
            return callback(null, true);
          }
          ledgerNode.storage.events.exists(e.eventHash, (err, result) => {
            if(err) {
              return callback(err);
            }
            if(result) {
              // track number of dups per minute , do not wait for callback
              cache.client.incr(`dup-${Math.round(Date.now() / 60000)}`);
            }
            callback(null, result);
          });
        }],
        addEvent: ['exists', (results, callback) => {
          if(results.exists) {
            return callback();
          }
          ledgerNode.events.add(e.event, {
            continuity2017: {peer: true}
          }, callback);
        }]
      }, callback), callback);
    }]
  }, err => callback(err));
};

api.compressEvents = ({events, maxEvents}, callback) => {
  if(events.length > maxEvents) {
    // end the event list on a merge event boundary
    let i;
    for(i = maxEvents - 1; i >= 0; --i) {
      if(jsonld.hasValue(
        events[i].event, 'type', 'ContinuityMergeEvent')) {
        break;
      }
    }
    // in the unlikely event that there a no merge events
    if(i === 0) {
      return callback(new BedrockError(
        'Regular event count exceeds `eventMax`.',
        'AbortError', {eventCount: events.length, maxEvents, public: true}));
    }
    logger.debug('Truncating history of events.', {
      originalLength: events.length,
      newLength: i + 1
    });
    events.length = i + 1;
  }
  // TODO: use deflate with a custom dictionary
  zlib.gzip(JSON.stringify(events), callback);
};

function _genesisProofCreate({ledgerNode, eventHash}, callback) {
  const mergeEvent = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash: [eventHash],
  };
  async.auto({
    creator: callback => voters.get(ledgerNode.id, callback),
    signedMergeEvent: ['creator', (results, callback) =>
      jsigs.sign(mergeEvent, {
        algorithm: 'Ed25519Signature2018',
        privateKeyBase58:
          results.creator.publicKey.privateKey.privateKeyBase58,
        creator: results.creator.publicKey.id
      }, callback)],
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, {
      event: results.signedMergeEvent,
      creator: results.creator
    });
  });
}

function _addAncestry({creatorId, event, ledgerNode}, callback) {
  api._getLocalBranchHead({creatorId, ledgerNode}, (err, result) => {
    if(err) {
      return callback(err);
    }
    const _event = bedrock.util.clone(event);
    const headHash = result.eventHash;
    _event.treeHash = headHash;
    _event.parentHash = [headHash];
    callback(null, _event);
  });
}

function _processLocalEvent({event, ledgerNode}, callback) {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => voters.get(ledgerNodeId, callback),
    ancestry: ['creator', (results, callback) => _addAncestry(
      {creatorId: results.creator.id, event, ledgerNode}, callback)],
    validate: ['ancestry', (results, callback) =>
      validate('continuity.webLedgerEvents', results.ancestry, callback)],
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const event = results.ancestry;
    const meta = {continuity2017: {creator: results.creator.id}};
    callback(null, {event, meta});
  });
}

function _processPeerMergeEvent({event, ledgerNode}, callback) {
  async.auto({
    signature: callback => signature.verify(
      {doc: event, ledgerNodeId: ledgerNode.id}, (err, result) => {
        if(err) {
          return callback(err);
        }
        // success
        callback(null, {creator: result.keyOwner.id});
      }),
    head: ['signature', (results, callback) => api._getLocalBranchHead(
      {creatorId: results.signature.creator, ledgerNode}, callback)],
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, {
      creator: results.signature.creator,
      generation: results.head.generation,
    });
  });
}

function _processPeerEvent({event, ledgerNode}, callback) {
  const ledgerNodeId = ledgerNode.id;
  const storage = ledgerNode.storage;
  const isMergeEvent = jsonld.hasValue(event, 'type', 'ContinuityMergeEvent');
  async.auto({
    mergeEvent: callback => {
      if(!isMergeEvent) {
        return callback();
      }
      _processPeerMergeEvent({event, ledgerNode}, callback);
    },
    hasAncestors: callback => {
      if(!event.parentHash.includes(event.treeHash)) {
        return callback(new BedrockError(
          'parentHash must include the value of treeHash',
          'DataError', {
            httpStatusCode: 400,
            public: true,
            event,
          }));
      }
      storage.events.exists(event.parentHash, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result) {
          // success
          return callback();
        }
        throw new Error('MISSING EVENT');
        // check the event cache queue for the missing events
        storage.events.difference(event.parentHash, (err, missing) => {
          if(err) {
            return callback(err);
          }
          cache.client.exists(missing.map(
            eventHash => _cacheKey.event({eventHash, ledgerNodeId})),
          (err, cacheResult) => {
            if(err) {
              return callback(err);
            }
            // all the missing documents are not in the queue
            if(cacheResult !== missing.length) {
              // throw new Error('Bang');
              return callback(new BedrockError(
                'Events from `parentHash` and/or `treeHash` are missing.',
                'InvalidStateError', {
                  event,
                  httpStatusCode: 400,
                  // TODO: a more accurate missing array can be computed
                  // taking into account the cache hits
                  missing,
                  public: true,
                }));
            }
            // missing events were found in the cache
            callback();
          });
        });
      });
    },
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    let _meta = {};
    if(isMergeEvent) {
      const {creator, generation} = results.mergeEvent;
      const nextGeneration = generation + 1;
      _meta = {continuity2017: {creator, generation: nextGeneration}};
    }
    callback(null, {event, meta: _meta});
  });
}

function _queueEvent({event, ledgerNodeId, meta}, callback) {
  const eventKey = _cacheKey.event({eventHash: meta.eventHash, ledgerNodeId});
  const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  cache.client.multi()
    .set(eventKey, JSON.stringify({event, meta}))
    .rpush(eventQueueKey, eventKey)
    .exec(callback);
}

function _writeEvent(ledgerNode, event, options, callback) {
  const storage = ledgerNode.storage;
  const fromPeer = (options.continuity2017 && options.continuity2017.peer);
  async.auto({
    // add creator to local events
    process: callback => {
      if(options.genesis) {
        return callback(null, {event, meta: {}});
      }
      if(fromPeer) {
        return _processPeerEvent({event, ledgerNode}, callback);
      }
      _processLocalEvent({event, ledgerNode}, callback);
    },
    // waiting here only to save cycles in event of failure in process
    config: ['process', (results, callback) => {
      if(options.genesis) {
        return callback(null, event.ledgerConfiguration);
      }
      storage.events.getLatestConfig((err, result) => {
        if(err) {
          return callback(err);
        }
        callback(null, result.event.ledgerConfiguration);
      });
    }],
    validator: ['config', (results, callback) => {
      if(!fromPeer) {
        // ledgerConfiguration and operations in events added locally have
        // already been validated
        return callback();
      }

      if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
        // no operations or config to validate
        return callback();
      }

      if(event.type === 'WebLedgerOperationEvent') {
        // validate every operation
        return async.each(event.operation || [], (operation, callback) => {
          ledgerNode.operations.validate(operation, callback);
        }, callback);
      }

      // remaining type (per above logic) must be `WebLedgerConfigurationEvent`
      ledgerNode.config.validate(event.ledgerConfiguration, callback);
    }],
    eventHash: ['validator', (results, callback) =>
      hasher(results.process.event, callback)],
    writeEvent: ['eventHash', (results, callback) => {
      const {event, meta} = results.process;
      meta.eventHash = results.eventHash;
      if(fromPeer) {
        // events from peers that can be re-acquired go into redis queue
        return _queueEvent({event, ledgerNodeId: ledgerNode.id, meta}, err => {
          if(err) {
            return callback(err);
          }
          // return same data as mongo
          callback(null, {event, meta});
        });
      }
      // only local regular events pass here
      storage.events.add(event, meta, callback);
    }],
  }, (err, results) => {
    if(err) {
      logger.error('Error in write event', {error: err});
      return callback(err);
    }
    callback(null, results.writeEvent);
  });
}

function _writeGenesisBlock(storage, block, callback) {
  async.auto({
    hashBlock: callback => hasher(block, callback),
    writeBlock: ['hashBlock', (results, callback) => {
      const meta = {
        blockHash: results.hashBlock,
        consensus: true,
        consensusDate: Date.now()
      };
      storage.blocks.add(block, meta, callback);
    }]
  }, callback);
}
