/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _blocks = require('./blocks');
const _cacheKey = require('./cache-key');
const _operations = require('./operations');
const _util = require('./util');
const _voters = require('./voters');
const async = require('async');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const config = bedrock.config;
const brLedgerNode = require('bedrock-ledger-node');
const continuityStorage = require('./storage');
const database = require('bedrock-mongodb');
const hasher = brLedgerNode.consensus._hasher;
const jsonld = bedrock.jsonld;
const logger = require('./logger');
const signature = require('./signature');
const uuid = require('uuid/v4');
const validate = require('bedrock-validation').validate;
const voters = require('./voters');
const zlib = require('zlib');
const BedrockError = bedrock.util.BedrockError;

const eventsConfig = config['ledger-consensus-continuity'].events;
const operationsConfig = config['ledger-consensus-continuity'].operations;

const api = {};
module.exports = api;

/**
 * Adds a new event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 * @param options the options to use.
 * @param callback(err, record) called once the operation completes.
 */
api.add = (
  {continuity2017, event, eventHash, genesis, genesisBlock, ledgerNode, needed},
  callback) => {
  const ledgerNodeId = ledgerNode.id;
  const storage = ledgerNode.storage;
  if(genesisBlock) {
    // do not mutate genesisBlock
    genesisBlock = bedrock.util.clone(genesisBlock);
  }
  _writeEvent({
    continuity2017, event, eventHash, genesis, ledgerNode, needed
  }, (err, eventRecord) => {
    if(err) {
      return callback(err);
    }
    if(!genesis) {
      // no need to create genesis block, return early
      return callback(null, eventRecord);
    }

    // need to write the genesis block, either from `options.genesisBlock`
    // to mirror an existing ledger, or create it ourselves for a new ledger
    const now = Date.now();
    const blockHeight = 0;
    async.auto({
      localCreatorId: callback => voters.get({ledgerNodeId}, (err, result) =>
        err ? callback(err) : callback(null, result.id)),
      // add indexes specific to Continuity
      createIndexes: ['localCreatorId', (results, callback) => {
        const collection = storage.events.collection.s.name;
        database.createIndexes([{
          // FIXME: remove this `meta.eventHash` index if history lookups
          //   can be performed on smaller `eventHash` instead
          collection,
          fields: {
            'meta.eventHash': 1
          },
          options: {
            unique: true, background: false, name: 'continuity_meta_eventHash'
          }
        }, {
          // not for searches but to ensure that local node never forks
          collection,
          fields: {
            'meta.continuity2017.generation': 1
          },
          options: {
            unique: true, background: false, name: 'continuity_fork_prevention',
            partialFilterExpression: {
              'meta.continuity2017.type': 'm',
              'meta.continuity2017.creator': results.localCreatorId
            }
          }
        }, {
          collection,
          fields: {
            'meta.continuity2017.type': 1,
            'meta.continuity2017.creator': 1,
            'meta.continuity2017.generation': 1,
            'meta.eventHash': 1,
          },
          options: {
            sparse: false, unique: false, background: false, name: 'continuity1'
          }
        }, {
          collection,
          fields: {
            'meta.consensus': 1,
            'meta.continuity2017.type': 1,
            'meta.continuity2017.creator': 1,
          },
          options: {
            sparse: false, unique: false, background: false, name: 'continuity2'
          }
        }], callback);
      }],
      configEvent: ['createIndexes', (results, callback) => {
        const {eventHash} = eventRecord.meta;
        ledgerNode.storage.events.update({
          eventHash,
          patch: [{
            op: 'set',
            changes: {
              meta: {
                blockHeight,
                blockOrder: 0,
                consensus: true,
                consensusDate: now,
                updated: now
              }
            }
          }]
        }, callback);
      }],
      mergeEvent: ['configEvent', (results, callback) => {
        const {eventHash} = eventRecord.meta;
        if(genesisBlock) {
          return _verifyGenesisEvents(
            {eventHash, genesisBlock, ledgerNodeId}, callback);
        }
        _genesisProofCreate({eventHash, ledgerNode}, callback);
      }],
      mergeEventRecord: ['mergeEvent', (results, callback) => {
        const {creator, mergeHash: eventHash, mergeEvent: event} =
          results.mergeEvent;
        const meta = {
          blockHeight,
          blockOrder: 1,
          consensus: true,
          consensusDate: now,
          continuity2017: {creator: creator.id, generation: 0, type: 'm'},
          created: now,
          updated: now,
          eventHash,
        };
        storage.events.add({event, meta}, callback);
      }],
      createBlock: ['mergeEventRecord', (results, callback) => {
        if(genesisBlock) {
          // use given genesis block, mirroring an existing ledger
          // replace the full event docs with hashes
          genesisBlock.event = [
            eventRecord.meta.eventHash,
            results.mergeEventRecord.meta.eventHash
          ];
          return callback(null, genesisBlock);
        }
        // create genesis block, creating a new ledger
        const block = {
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          id: eventRecord.event.ledgerConfiguration.ledger + '/blocks/0',
          type: 'WebLedgerEventBlock',
          consensusMethod: 'Continuity2017',
          event: [
            eventRecord.meta.eventHash,
            results.mergeEventRecord.meta.eventHash
          ],

          // TODO: consensusProof should be stored as a hash
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
      writeBlock: ['createBlock', (results, callback) => _blocks.writeGenesis({
        block: results.createBlock, ledgerNode}, callback)],
      // mark event has having achieved consensus
      updateEvent: ['writeBlock', (results, callback) => storage.events.update({
        eventHash: eventRecord.meta.eventHash, patch: [{
          op: 'set',
          changes: {
            meta: {
              consensus: true,
              consensusDate: now
            }
          }
        }]
      }, callback)],
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
  creatorFilter, creatorRestriction, eventTypeFilter,
  ledgerNode, startHash
}, callback) => {
  logger.verbose('Start aggregateHistory');
  const collection = ledgerNode.storage.events.collection;
  // regular events *should* be included, regardless of created
  const restrictSearchWithMatch = {
    $nor: []
  };
  if(creatorRestriction && creatorRestriction.length !== 0) {
    creatorRestriction.forEach(r => restrictSearchWithMatch.$nor.push({
      'meta.continuity2017.type': 'm',
      'meta.continuity2017.creator': r.creator,
      'meta.continuity2017.generation': {$lte: r.generation}
    }));
  }

  if(Array.isArray(creatorFilter) && creatorFilter.length !== 0) {
    restrictSearchWithMatch.$nor.push(
      {'meta.continuity2017.creator': {$in: creatorFilter}});
  }

  if(eventTypeFilter) {
    const type = eventTypeFilter === 'ContinuityMergeEvent' ? 'm' : 'r';
    restrictSearchWithMatch['meta.continuity2017.type'] = type;
  }

  // genesisMerge should never be included in the history
  // FIXME: should not be necessary to add this
  // restrictSearchWithMatch.$nor.push({
  //   'meta.continuity2017.generation': 0
  // });
  const pipeline = [
    {$match: {'meta.eventHash': startHash}},
    {$graphLookup: {
      from: collection.s.name,
      startWith: '$meta.eventHash',
      connectFromField: 'event.parentHash',
      connectToField: 'meta.eventHash',
      as: '_parents',
      restrictSearchWithMatch
    }},
    {$project: {
      _id: 0, '_parents.meta.eventHash': 1, '_parents.event.parentHash': 1
    }},
    {$unwind: '$_parents'},
    {$replaceRoot: {newRoot: '$_parents'}},
  ];
  // FIXME: no use case right now for fullEvent === true, added $project above
  // if(!fullEvent) {
  //   pipeline.push(
  //     {$project: {
  //       _id: 0, eventHash: 1, 'event.parentHash': 1
  //     }}
  //   );
    // {$group: {
    //   _id: null,
    //   hashes: {$addToSet: '$eventHash'}
    // }});
    // pipeline.push({$project: {
    //   _id: 0, eventHash: 1, 'event.parentHash': 1, 'event.treeHash': 1,
    //   'meta.continuity2017.creator': 1, 'meta.continuity2017.generation': 1
    // }});
  // }

  const startTime = Date.now();
  collection.aggregate(pipeline, {allowDiskUse: true}).toArray(
    (err, result) => {
      if(err) {
        return callback(err);
      }
      if(result.length === 0) {
        return callback(null, []);
      }

      const hashMap = {};
      for(let i = 0; i < result.length; ++i) {
        hashMap[result[i].meta.eventHash] = result[i];
      }

      const sortedHistory = [];

      let x;
      let s = 0;
      do {
        // using _.findIndex is faster than _.find
        x = _.findIndex(result, e => !e._p, s++);
        if(x === -1) {
          continue;
        }
        visit(result[x]);
      } while(x !== -1);

      // set duration timer
      cache.client.set(`aggregate|${ledgerNode.id}`, Date.now() - startTime);
      // TODO: _.uniq??
      callback(null, sortedHistory);

      function visit(e) {
        for(let i = 0; i < e.event.parentHash.length; ++i) {
          const n = hashMap[e.event.parentHash[i]];
          if(n && !n._p) {
            visit(n);
          }
        }
        e._p = true;
        sortedHistory.push(e.meta.eventHash);
      }
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
  }, (err, results) => err ? callback(err) : callback(null, {
    creatorHeads: results.partition.creatorHeads, file: results.compress
  }));
};

api.create = ({ledgerNode}, callback) => {
  logger.verbose('Attempting to create an event.');
  const ledgerNodeId = ledgerNode.id;
  const opListKey = _cacheKey.operationList(ledgerNodeId);
  const opSelectedListKey = _cacheKey.operationSelectedList(ledgerNodeId);
  let truncated = false;
  async.auto({
    // check for selected ops which indicates a previous event creation failure
    selectedOps: callback => cache.client.lrange(
      opSelectedListKey, 0, -1, callback),
    opKeys: ['selectedOps', (results, callback) => {
      // if operations had been selected previously, repeat creation process
      if(results.selectedOps.length !== 0) {
        return callback(null, results.selectedOps);
      }
      const {maxOperations} = eventsConfig;
      cache.client.multi()
        .llen(opListKey)
        .lrange(opListKey, 0, maxOperations - 1)
        .exec((err, result) => {
          if(err) {
            return callback(err);
          }
          const listLength = result[0];
          if(listLength === 0) {
            logger.debug('No new operations.');
            return callback(new BedrockError('Nothing to do.', 'AbortError'));
          }
          if(listLength > maxOperations) {
            truncated = true;
          }
          logger.debug(`New operations found: ${listLength}`);
          callback(null, result[1]);
        });
    }],
    ops: ['opKeys', (results, callback) => {
      const {opKeys} = results;
      const multi = cache.client.multi();
      multi.mget(opKeys);
      if(results.selectedOps.length === 0) {
        // nothing to do if already operating out of selectedOps
        const opCount = opKeys.length;
        // remove selected operations from the master operation list
        // ltrim *keeps* items from start to end
        multi.ltrim(opListKey, opCount, -1);
        // record the selected keys in case this operation fails
        multi.rpush(opSelectedListKey, opKeys);
      }
      multi.exec((err, result) => {
        if(err) {
          return callback(err);
        }
        // the return value from mget
        const o = result[0];
        const ops = [];
        for(let i = 0; i < o.length; ++i) {
          ops.push(JSON.parse(o[i]));
        }
        // lexicographic sort on the hash of the operation determines the
        // order of operations in events
        _sortOperations(ops);
        const operations = [];
        const operationHash = [];
        for(let i = 0; i < ops.length; ++i) {
          operations.push(ops[i]);
          operationHash.push(ops[i].meta.operationHash);
        }
        callback(null, {operations, operationHash});
      });
    }],
    creator: ['opKeys', (results, callback) => voters.get(
      {ledgerNodeId}, callback)],
    head: ['creator', (results, callback) => {
      const creatorId = results.creator.id;
      api._getLocalBranchHead({creatorId, ledgerNode}, callback);
    }],
    eventHash: ['head', 'ops', (results, callback) => {
      const {operationHash} = results.ops;
      const {eventHash: headHash} = results.head;
      const event = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        type: 'WebLedgerOperationEvent',
        operationHash,
        parentHash: [headHash],
        treeHash: headHash,
      };
      _util.hasher(event, (err, eventHash) => {
        if(err) {
          return callback(err);
        }
        callback(null, {event, eventHash});
      });
    }],
    storeOps: ['eventHash', (results, callback) => {
      const {eventHash} = results.eventHash;
      const {operations} = results.ops;
      _operations.write({eventHash, ledgerNode, operations}, callback);
    }],
    event: ['storeOps', (results, callback) => {
      const {event, eventHash} = results.eventHash;
      api.add({event, eventHash, ledgerNode}, callback);
    }],
    cache: ['event', (results, callback) => {
      const {opKeys} = results;
      cache.client.multi()
        // remove the selected list entirely
        .del(opSelectedListKey)
        // delete the keys that contain the operation documents
        .del(opKeys)
        .exec(callback);
    }]
  }, err => {
    if(err && err.name !== 'AbortError') {
      return callback(err);
    }
    callback(null, {truncated});
  });
};

// called from consensus worker
api.getRecentHistory = ({creatorId, ledgerNode}, callback) => {
  logger.verbose('Start getRecentHistory');
  const startTime = Date.now();
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNode.id);
  async.auto({
    localBranchHead: callback => api._getLocalBranchHead(
      {creatorId, ledgerNode}, (err, result) => {
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
      }),
    keys: callback => cache.client.smembers(outstandingMergeKey, callback),
    eventData: ['keys', (results, callback) => {
      const {keys} = results;
      const events = [];
      const eventMap = {};
      if(!keys || keys.length === 0) {
        return callback(null, {events, eventMap});
      }
      cache.client.mget(keys, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(result.indexOf(null) !== -1) {
          return callback(new BedrockError(
            'One or more events are missing from the cache.',
            'InvalidStateError', {
              httpStatusCode: 400,
              public: true,
            }));
        }
        for(let i = 0; i < result.length; ++i) {
          // TODO: try/catch?
          const parsed = JSON.parse(result[i]);
          const {eventHash} = parsed.meta;
          const {parentHash, treeHash, type} = parsed.event;
          const {creator} = parsed.meta.continuity2017;
          const doc = {
            _children: [],
            _parents: [],
            eventHash,
            event: {parentHash, treeHash, type},
            meta: {continuity2017: {creator}}
          };
          events.push(doc);
          eventMap[doc.eventHash] = doc;
        }
        callback(null, {events, eventMap});
      });
    }],
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const duration = Date.now() - startTime;
    cache.client.set(`recentHistoryMergeOnly|${ledgerNode.id}`, duration);
    logger.verbose('End getRecentHistory', {duration});
    const {localBranchHead} = results;
    const {events, eventMap} = results.eventData;
    callback(null, {events, eventMap, localBranchHead});
  });
};

api.merge = ({creatorId, ledgerNode}, callback) => {
  const childlessKey = _cacheKey.childless(ledgerNode.id);
  async.auto({
    childless: callback => cache.client.smembers(
      childlessKey, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result || result.length === 0) {
          return callback(new BedrockError(
            'No childless events.', 'AbortError'));
        }
        callback(null, result);
      }),
    // if there are no outstanding regular events, there is no need to merge
    regularEvents: ['childless', (results, callback) =>
      _hasOutstandingRegularEvents({ledgerNode}, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return callback(new BedrockError(
            'No outstanding regular events.', 'AbortError'));
        }
        callback();
      })],
    head: ['regularEvents', (results, callback) => api._getLocalBranchHead(
      {creatorId, ledgerNode}, callback)],
    merge: ['head', (results, callback) => {
      const parentHashes = results.childless;
      const {eventHash: treeHash, generation} = results.head;
      const nextGeneration = generation + 1;

      let truncated = false;
      const {maxEvents} = config['ledger-consensus-continuity'].merge;
      if(parentHashes.length > maxEvents) {
        truncated = true;
        parentHashes.length = maxEvents;
      }

      const event = {
        '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
        type: 'ContinuityMergeEvent',
        parentHash: [treeHash, ...parentHashes],
        treeHash,
      };
      const ledgerNodeId = ledgerNode.id;
      async.auto({
        sign: callback => voters.sign({ledgerNodeId, doc: event}, callback),
        eventHash: ['sign', (results, callback) =>
          hasher(results.sign, callback)],
        // local merge events must be written directly to the database
        write: ['eventHash', (results, callback) => {
          const {eventHash} = results;
          const meta = {continuity2017: {
            creator: creatorId,
            generation: nextGeneration,
            type: 'm'
          }, eventHash};
          ledgerNode.storage.events.add(
            {event: results.sign, meta}, (err, result) => {
              if(err) {
                return callback(err);
              }
              result.truncated = truncated;
              callback(null, result);
            });
        }],
        cache: ['write', (results, callback) => _updateCache(
          {eventRecord: results.write, ledgerNodeId}, callback)],
      // results.write is used in unit tests
      }, (err, results) => err ? callback(err) : callback(null, results.write));
    }]
  }, (err, results) => {
    if(err && err.name !== 'AbortError') {
      return callback(err);
    }
    if(err && err.name === 'AbortError') {
      return callback();
    }
    callback(null, results.merge);
  });
};

api.repairCache = ({eventHash, ledgerNode}, callback) => {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    eventRecord: callback => ledgerNode.storage.events.get(eventHash, callback),
    updateCache: ['eventRecord', (results, callback) => {
      const {eventRecord} = results;
      _updateCache({eventRecord, ledgerNodeId}, callback);
    }]
  }, callback);
};

api.getCreatorHeads = (
  {latest = false, ledgerNode, peerId}, callback) => async.auto({
  // get ids for all the creators known to the node
  // TODO: create and use a key storage API call here
  localCreators: callback => database.collections.continuity2017_key.find(
    {ledgerNodeId: ledgerNode.id}, {_id: 0, 'publicKey.owner': 1}
  ).toArray((err, result) =>
    err ? callback(err) : callback(null, result.map(k => k.publicKey.owner))),
  heads: ['localCreators', (results, callback) => {
    const creatorHeads = {};
    // ensure that peerId is included in creators, if the peer has never been
    // contacted before, it will not be included by localCreators
    // in this case, the head for peerId will be set to genesis merge event
    const creators = _.uniq([peerId, ...results.localCreators]);
    async.each(creators, (creatorId, callback) => api._getLocalBranchHead(
      {creatorId, latest, ledgerNode}, (err, result) => {
        if(err) {
          return callback(err);
        }
        creatorHeads[creatorId] = result;
        callback();
      }), err => err ? callback(err) : callback(null, creatorHeads));
  }],
}, callback);

api.partitionHistory = (
  {creatorHeads, creatorId, eventTypeFilter, fullEvent = false, peerId},
  callback) => {
  const _creatorHeads = bedrock.util.clone(creatorHeads);
  // NOTE: it is normal for creatorHeads not to include creatorId (this node)
  // if this node has never spoken to the peer before

  // *do not!* remove the local creator from the heads
  // delete _creatorHeads[creatorId];
  async.auto({
    // FIXME: voter and ledgerNode can be passed in some cases
    ledgerNodeId: callback => _voters.getLedgerNodeId(creatorId, callback),
    ledgerNode: ['ledgerNodeId', (results, callback) =>
      brLedgerNode.get(null, results.ledgerNodeId, callback)],
    genesisMerge: ['ledgerNode', (results, callback) =>
      api.getGenesisMergeSummary({ledgerNode: results.ledgerNode}, callback)],
    localHeads: ['ledgerNode', (results, callback) => api.getCreatorHeads(
      {ledgerNode: results.ledgerNode, peerId}, callback)],
    startHash: ['localHeads', 'genesisMerge', (results, callback) => {
      // {eventhHash, generation}
      const localNodeHead = results.localHeads.heads[creatorId];
      const {maxDepth} = config['ledger-consensus-continuity'].gossip;
      // peer should be provide a head for this node, just insurance here
      const peerLocalNodeHead = _creatorHeads[creatorId] ||
        results.genesisMerge;
      if(peerLocalNodeHead.eventHash === localNodeHead.eventHash) {
        return callback(new BedrockError(
          'Peer has provided current local head.  Nothing to do.',
          'AbortError', {localNodeHead, public: true}));
      }
      // node is within range, proceed normally
      if(localNodeHead.generation - peerLocalNodeHead.generation <= maxDepth) {
        // returning the eventHash
        return callback(null, {
          eventHash: localNodeHead.eventHash, truncated: false
        });
      }
      // the peer is more than maxDepth behind, give them last hash within
      // maximum depth
      logger.debug('Truncating history, peer is catching up.', {
        localNodeHead, maxDepth, peerId, peerLocalNodeHead
      });
      const targetGeneration = peerLocalNodeHead.generation + maxDepth;
      const collection = results.ledgerNode.storage.events.collection;
      const query = {
        'meta.continuity2017.type': 'm',
        'meta.continuity2017.creator': creatorId,
        'meta.continuity2017.generation': targetGeneration
      };
      const projection = {_id: 0, 'meta.eventHash': 1};
      collection.findOne(query, projection, (err, result) => {
        if(err) {
          return callback(err);
        }
        // return the eventHash
        callback(null, {
          eventHash: result.meta.eventHash,
          truncated: true
        });
      });
    }],
    history: ['startHash', (results, callback) => {
      const ledgerNode = results.ledgerNode;

      // aggregate search starts with the local head
      const startHash = results.startHash.eventHash;

      // NOTE: if this is the first contact with the peer, the head
      // for the local node will be set to genesisMerge as well

      // localCreators contains a list of all creators this node knows about
      const peerCreators = Object.keys(creatorHeads);
      const localCreators = results.localHeads.localCreators;
      // remove heads for nodes this node doesn't know about
      peerCreators.forEach(c => {
        if(localCreators.includes(c)) {
          return;
        }
        delete _creatorHeads[c];
      });
      // if the peer did not provide a head for a locally known creator,
      // set the head to genesis merge
      localCreators.forEach(c => {
        if(_creatorHeads[c]) {
          return;
        }
        _creatorHeads[c] = results.genesisMerge;
      });

      const creatorRestriction = Object.keys(_creatorHeads).map(c => ({
        creator: c, generation: _creatorHeads[c].generation
      }));
      api.aggregateHistory({
        creatorRestriction,
        creatorId,
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
      history: _.get(results, 'history', []),
      truncated: _.get(results, 'startHash.truncated', false)
    });
  });
};

// one use case is passing in a list of treeHashes
api.getHeads = ({eventHashes, ledgerNode}, callback) => {
  // redis mget throws an error if an empty array is passed as an argument
  if(!(Array.isArray(eventHashes) && eventHashes.length !== 0)) {
    return callback();
  }
  if(!ledgerNode) {
    throw new TypeError('`ledgerNode` is required.');
  }
  // eventHash, generation
  const headGenerationMap = new Map();
  // add every hash to headGenerationMap to ensure order is preserved
  eventHashes.forEach(eventHash => headGenerationMap.set(eventHash, null));
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    redis: callback => {
      const keys = eventHashes.map(
        eventHash => _cacheKey.headGeneration({eventHash, ledgerNodeId}));
      const cacheMisses = [];
      cache.client.mget(keys, (err, result) => {
        if(err) {
          return callback(err);
        }
        // redis returns null for misses
        result.forEach((generation, i) => {
          if(generation === null) {
            // the eventHash, not the cache key
            return cacheMisses.push(eventHashes[i]);
          }
          // the eventHash, not the cache key
          headGenerationMap.set(eventHashes[i], parseInt(generation, 10));
        });
        callback(null, cacheMisses);
      });
    },
    mongo: ['redis', (results, callback) => {
      const cacheMisses = results.redis;
      if(cacheMisses.length === 0) {
        return callback();
      }
      const collection = ledgerNode.storage.events.collection;
      const query = {'meta.eventHash': {$in: cacheMisses}};
      const projection = {
        _id: 0, 'meta.eventHash': 1, 'meta.continuity2017.generation': 1
      };
      const mongo = new Map();
      collection.find(query, projection).forEach(r => {
        headGenerationMap.set(
          r.meta.eventHash, r.meta.continuity2017.generation);
        mongo.set(r.meta.eventHash, r.meta.continuity2017.generation);
      }, err => err ? callback(err) : callback(null, mongo));
    }],
    cache: ['mongo', (results, callback) => {
      if(!results.mongo || results.mongo.size === 0) {
        return callback();
      }
      const multi = cache.client.multi();
      for(const [eventHash, generation] of results.mongo) {
        const headGenerationKey = _cacheKey.headGeneration(
          {eventHash, ledgerNodeId});
        multi.set(headGenerationKey, generation, 'EX', 36000);
      }
      multi.exec(callback);
    }]
  }, err => {
    if(err) {
      return callback(err);
    }

    // if this happens, then the batch references a treeHash that does not exist
    for(const [eventHash, generation] of headGenerationMap) {
      if(generation === null) {
        return callback(new BedrockError(
          'The eventHash could not be found.',
          'NotFoundError', {
            eventHash,
            httpStatusCode: 404,
            public: true,
          }));
      }
    }
    callback(null, headGenerationMap);
  });
};

// FIXME: make this a regular API call.  It's used in worker.js as well.
// and use an appropriate name since
// this is not just for local branch, this finds the latest merge event from
// the specified creator
api._getLocalBranchHead = (
  {useCache = true, creatorId, latest = false, ledgerNode}, callback) => {
  if(!useCache && latest) {
    throw new Error('`useCache` and `latest` arguments must both be true.');
  }
  const ledgerNodeId = ledgerNode.id;
  const eventsCollection = ledgerNode.storage.events.collection;
  const headCacheKey = _cacheKey.head({creatorId, ledgerNodeId});
  async.auto({
    latest: callback => {
      if(!latest) {
        return callback();
      }
      // NOTE: latestPeerHead is the *very* latest head in the event pipeline
      // which may not have been written to mongo yet. This head is useful
      // during gossip, but not for merging events.
      const latestPeerHeadKey = _cacheKey.latestPeerHead(
        {creatorId, ledgerNodeId});
      cache.client.hmget(latestPeerHeadKey, 'h', 'g', (err, result) => {
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
      });
    },
    headCache: ['latest', (results, callback) => {
      // this means latest flag was set and latest return a result
      if(results.latest) {
        return callback(null, results.latest);
      }
      // cache has been disabled
      if(!useCache) {
        return callback();
      }
      // return regular heads from cache/mongo even if latest flage is set
      cache.client.hmget(headCacheKey, 'h', 'g', (err, result) => {
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
      });
    }],
    localMerge: ['headCache', (results, callback) => {
      if(results.headCache) {
        return callback(null, results.headCache);
      }
      // find the latest merge event
      const query = {
        'meta.continuity2017.type': 'm',
        'meta.continuity2017.creator': creatorId,
        'meta.continuity2017.generation': {$exists: true}
      };
      const projection = {
        _id: 0, 'meta.eventHash': 1, 'meta.continuity2017.generation': 1
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
          const eventHash = result[0].meta.eventHash;
          const generation = result[0].meta.continuity2017.generation;

          // do not update the cache if cache has been disabled
          if(!useCache) {
            return callback(null, {eventHash, generation});
          }

          cache.client.hmset(
            headCacheKey, 'h', eventHash, 'g', generation, err => {
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
    const head = results.localMerge ? results.localMerge : results.genesisMerge;
    return callback(null, head);
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
      const query = {'meta.continuity2017.type': 'm'};
      const projection =
        {_id: 0, 'meta.eventHash': 1, 'meta.continuity2017.generation': 1};
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
            eventHash: result[0].meta.eventHash,
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

// return hashes that do not exist in redis or mongo
function _diffManifest({ledgerNode, manifest, eventHashes}, callback) {
  if(eventHashes.length === 0) {
    return callback();
  }
  _diff({eventHashes, ledgerNode}, (err, result) => {
    if(err) {
      return callback(err);
    }
    const dupCount = eventHashes.length - result.length;
    if(dupCount !== 0) {
      cache.client.incrby(`dup-${Math.round(Date.now() / 60000)}`, dupCount);
    }
    const _manifest = bedrock.util.clone(manifest);
    _.remove(_manifest, e => !result.includes(e.eventHash));
    callback(null, _manifest);
  });
}

// all ancestors should exist in redis or mongo, diff should be empty
function _checkAncestors({ledgerNode, parentHash}, callback) {
  _diff({eventHashes: parentHash, ledgerNode}, (err, result) => {
    if(err) {
      return callback(err);
    }
    if(result.length !== 0) {
      return callback(new BedrockError(
        'Events from `parentHash` and/or `treeHash` are missing.',
        'InvalidStateError', {
          httpStatusCode: 400,
          // TODO: a more accurate missing array can be computed
          // taking into account the cache hits
          missing: result,
          public: true,
        }));
    }
    callback();
  });
}

function _diff({eventHashes, ledgerNode}, callback) {
  if(eventHashes.length === 0) {
    return callback();
  }
  const ledgerNodeId = ledgerNode.id;
  const manifestId = uuid();
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  const manifestKey = _cacheKey.manifest({ledgerNodeId, manifestId});
  // TODO: this could be implemented as smembers as well and diff the hashes
  // as an array, if the eventQueueSetKey contains a large set, then the
  // existing implementation is good
  async.auto({
    redis: callback => cache.client.multi()
      .sadd(manifestKey, eventHashes)
      .sdiff(manifestKey, eventQueueSetKey)
      .del(manifestKey)
      .exec((err, result) => {
        if(err) {
          return callback(err);
        }
        // result of `sadd` is in result[0]
        const notFound = result[1];
        callback(null, notFound);
      }),
    mongo: ['redis', (results, callback) => ledgerNode.storage.events
      .difference(results.redis, callback)]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    return callback(null, results.mongo);
  });
}

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
      _voters.getLedgerNodeId(creatorId, callback)],
    ledgerNode: ['voter', (results, callback) =>
      brLedgerNode.get(null, results.voter.ledgerNodeId, callback)],
    process: ['ledgerNode', (results, callback) => {
      const ledgerNode = results.ledgerNode;
      logger.debug('Processing compressed events.', {
        eventCount: results.unzip.length
      });
      // using series to ensure that events are stored in the proper order
      const rawManifest = results.unzip;
      const rawManifestEventHashes = rawManifest.map(e => e.eventHash);
      async.auto({
        diff: callback => _diffManifest({ledgerNode, manifest: rawManifest,
          eventHashes: rawManifestEventHashes}, callback),
        ancestors: ['diff', (results, callback) => {
          // start with de-duped manifest
          const manifest = results.diff;
          const allAncestors = [];
          for(let i = 0; i < manifest.length; ++i) {
            Array.prototype.push.apply(
              allAncestors, manifest[i].event.parentHash);
          }
          // remove dups and any ancestors that are contained in the
          // manifest itself
          const eventHashes = _.uniq(allAncestors).filter(
            a => !rawManifestEventHashes.includes(a));
          _checkAncestors({eventHashes, ledgerNode}, callback);
        }],
        add: ['ancestors', (results, callback) => async.eachSeries(
          results.diff, (e, callback) => ledgerNode.events.add({
            continuity2017: {peer: true}, event: e.event}, callback), callback)]
      }, callback);
    }]
  }, err => callback(err));
};

api.processEvents = ({events, ledgerNode}, callback) => {
  logger.verbose('Processing events.', {
    eventCount: events.length
  });
  // using series to ensure that events are stored in the proper order
  const rawManifest = events;
  const rawManifestEventHashes = rawManifest.map(e => e.eventHash);
  async.auto({
    diff: callback => _diffManifest({ledgerNode, manifest: rawManifest,
      eventHashes: rawManifestEventHashes}, callback),
    ancestors: ['diff', (results, callback) => {
      // start with de-duped manifest
      const manifest = results.diff;
      const allAncestors = [];
      for(let i = 0; i < manifest.length; ++i) {
        Array.prototype.push.apply(
          allAncestors, manifest[i].event.parentHash);
      }
      // remove dups and any ancestors that are contained in the
      // manifest itself
      const eventHashes = _.uniq(allAncestors).filter(
        a => !rawManifestEventHashes.includes(a));
      _checkAncestors({eventHashes, ledgerNode}, callback);
    }],
    add: ['ancestors', (results, callback) => async.eachSeries(
      results.diff, (e, callback) => ledgerNode.events.add({
        continuity2017: {peer: true}, event: e.event}, callback)
      , callback)]
  }, callback);
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
  const doc = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'ContinuityMergeEvent',
    parentHash: [eventHash],
  };
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => voters.get({ledgerNodeId, publicKey: true}, callback),
    mergeEvent: callback => voters.sign({doc, ledgerNodeId}, callback),
    mergeHash: ['mergeEvent', (results, callback) =>
      hasher(results.mergeEvent, callback)]
  }, callback);
}

function _hasOutstandingRegularEvents({ledgerNode}, callback) {
  const collection = ledgerNode.storage.events.collection;
  const query = {
    'meta.continuity2017.type': 'r',
    'meta.consensus': {$exists: false},
  };
  collection.findOne(query, {_id: 1}, (err, result) => callback(err, !!result));
}

// looks in queue and database for an event
function _getGeneration({eventHash, ledgerNode}, callback) {
  const headGenerationKey = _cacheKey.headGeneration(
    {eventHash, ledgerNodeId: ledgerNode.id});
  async.auto({
    cache: callback => cache.client.get(headGenerationKey, (err, result) => {
      if(err) {
        return callback(err);
      }
      if(result === null) {
        return callback();
      }
      callback(null, parseInt(result, 10));
    }),
    event: ['cache', (results, callback) => {
      if(results.cache) {
        return callback(null, results.cache);
      }
      ledgerNode.storage.events.get(eventHash, (err, result) => {
        if(err) {
          return callback(err);
        }
        callback(null, result.meta.continuity2017.generation);
      });
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, results.event);
  });
}

function _processLocalEvent({event, eventHash, ledgerNode}, callback) {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => voters.get({ledgerNodeId}, callback),
    validate: callback => validate(
      'continuity.webLedgerEvents', event, callback),
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const meta = {
      continuity2017: {creator: results.creator.id, type: 'r'},
      eventHash,
    };
    callback(null, {event, meta});
  });
}

function _processOperations({ledgerNode, operations}, callback) {
  async.map(operations, (operation, callback) => async.auto({
    validate: callback => ledgerNode.operations.validate(operation, callback),
    operationHash: ['validate', (results, callback) =>
      _util.hasher(operation, callback)]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const {operationHash} = results;
    const recordId = _util.generateRecordId({ledgerNode, operation});
    callback(null, {meta: {operationHash}, operation, recordId});
  }), callback);
}

function _processPeerConfigurationEvent({event, ledgerNode}, callback) {

  // TODO: add additional processing and triggers? Perhaps nothing more is
  // needed here since the event has not yet reached consensus.

  ledgerNode.config.validate(event.ledgerConfiguration, callback);
}

function _processPeerRegularEvent({event, ledgerNode, needed}, callback) {
  const _event = bedrock.util.clone(event);
  const {operation: ops} = _event;
  delete _event.operation;
  async.auto({
    operationRecords: callback => _processOperations(
      {ledgerNode, operations: ops}, callback),
    eventHash: ['operationRecords', (results, callback) => {
      // eventHash is calculated based on the hashes of `operation` documents
      // the order of the operationHash array does not affect the hash
      const {operationRecords} = results;
      _event.operationHash = operationRecords.map(o => o.meta.operationHash);
      _util.hasher(_event, callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const {eventHash, operationRecords} = results;
    if(!needed.includes(eventHash)) {
      return callback(new BedrockError(
        'The event supplied by the peer was not requested.',
        'DataError', {event, eventHash, needed}));
    }
    const meta = {continuity2017: {type: 'r'}, eventHash};

    // lexicographic sort on the hash of the operation determines the
    // order of operations in events
    _sortOperations(operationRecords);

    for(let i = 0; i < operationRecords.length; ++i) {
      const {meta} = operationRecords[i];
      meta.eventHash = eventHash;
      meta.eventOrder = i;
    }

    // put operation documents into _event
    _event.operationRecords = operationRecords;

    callback(null, {event: _event, meta});
  });
}

function _processPeerMergeEvent({event, ledgerNode, needed}, callback) {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    eventHash: callback => _util.hasher(event, callback),
    parentGeneration: callback => _getGeneration(
      {eventHash: event.treeHash, ledgerNode}, callback),
    localCreatorId: callback => voters.get({ledgerNodeId}, (err, result) =>
      err ? callback(err) : callback(null, result.id)),
    signature: callback => signature.verify(
      {doc: event, ledgerNodeId}, (err, result) => {
        if(err) {
          return callback(err);
        }
        // success
        callback(null, {creator: result.keyOwner.id});
      }),
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const {parentGeneration, eventHash} = results;
    if(!needed.includes(eventHash)) {
      return callback(new BedrockError(
        'The event supplied by the peer was not requested.',
        'DataError', {event, eventHash, needed}));
    }
    const {creator} = results.signature;
    if(creator === results.localCreatorId) {
      return callback(new BedrockError(
        'Merge events created by the local node cannot be added with this API.',
        'NotSupportedError', {
          httpStatusCode: 400,
          public: true,
        }));
    }
    const generation = parentGeneration + 1;
    const meta = {
      continuity2017: {creator, generation, type: 'm'},
      eventHash,
    };
    callback(null, {event, meta});
  });
}

// TODO: these processing functions are candidates to move into a worker pool
// which would parallelize hashing and signature verification
function _processPeerEvent({event, ledgerNode, needed}, callback) {
  async.auto({
    // ensure that all ancestors are in redis or mongo
    checkAncestors: callback => _checkAncestors(
      {ledgerNode, parentHash: event.parentHash}, callback),
    process: ['checkAncestors', (results, callback) => {
      if(jsonld.hasValue(event, 'type', 'WebLedgerOperationEvent')) {
        return _processPeerRegularEvent({event, ledgerNode, needed}, callback);
      }
      if(jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
        return _processPeerMergeEvent({event, ledgerNode, needed}, callback);
      }
      if(jsonld.hasValue(event, 'type', 'WebLedgerConfigurationEvent')) {
        return _processPeerConfigurationEvent({event, ledgerNode}, callback);
      }
      // unknown event type
      callback(new BedrockError(
        'Unknown event type.',
        'DataError', {
          event,
          httpStatusCode: 404,
          public: true,
        }));
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.process));
}

function _queueEvent({event, ledgerNodeId, meta}, callback) {
  const {eventHash} = meta;
  const {creator: creatorId, generation, type} = meta.continuity2017;
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  let eventJson;
  try {
    eventJson = JSON.stringify({event, meta});
  } catch(err) {
    return callback(err);
  }
  // NOTE: redis multi is an atomic operation
  const multi = cache.client.multi();
  if(type === 'r') {
    const opCountKey = _cacheKey.opCountPeer(
      {ledgerNodeId, second: Math.round(Date.now() / 1000)});
    multi.incrby(opCountKey, event.operationRecords.length);
    multi.expire(opCountKey, operationsConfig.counter.ttl);
  }
  if(type === 'm') {
    const headGenerationKey = _cacheKey.headGeneration(
      {eventHash, ledgerNodeId});
    const latestPeerHeadKey = _cacheKey.latestPeerHead(
      {creatorId, ledgerNodeId});
    // expired the key in an hour, incase the peer/creator goes dark
    multi.hmset(latestPeerHeadKey, 'h', eventHash, 'g', generation);
    multi.expire(latestPeerHeadKey, 3600);
    // this key is set to expire in the event-writer
    multi.set(headGenerationKey, generation);
  }
  // add the hash to the set used to check for dups and ancestors
  multi.sadd(eventQueueSetKey, eventHash);
  // create a key that contains the event and meta
  multi.set(eventKey, eventJson);
  // push to the list that is handled in the event-writer
  multi.rpush(eventQueueKey, eventKey);
  multi.publish(`continuity2017|peerEvent|${ledgerNodeId}`, 'new');
  multi.exec(callback);
}

// sort an array of operation records, mutates operations
function _sortOperations(operations) {
  operations.sort((a, b) => a.meta.operationHash.localeCompare(
    b.meta.operationHash));
}

function _updateCache({eventRecord, ledgerNodeId}, callback) {
  const childlessKey = _cacheKey.childless(ledgerNodeId);
  const {creator: creatorId, generation} = eventRecord.meta.continuity2017;
  const {eventHash} = eventRecord.meta;
  const headKey = _cacheKey.head({creatorId, ledgerNodeId});
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
  const {parentHash, treeHash, type} = eventRecord.event;
  const parentHashes = _.without(parentHash, treeHash);
  // no need to set a headGeneration key here, those are only used for
  // processing peer merge events
  // TODO: `creator` is quite a long URL, can a substitution be made?

  // eventHash here is a multihash
  const cacheEvent = JSON.stringify({
    event: {parentHash, treeHash, type},
    meta: {eventHash, continuity2017: {creator: creatorId}}
  });
  cache.client.multi()
    .srem(childlessKey, parentHashes)
    .set(eventKey, cacheEvent)
    .sadd(outstandingMergeKey, eventKey)
    .hmset(headKey, 'h', eventHash, 'g', generation)
    .publish(`continuity2017|event|${ledgerNodeId}`, 'merge')
    .exec((err, result) => {
      if(err) {
        // FIXME: fail gracefully
        // failure here means head information would be corrupt which
        // cannot be allowed
        logger.error('Could not set head.', {
          creatorId,
          // FIXME: fix when logger.error works properly
          err1: err,
          generation,
          headKey,
          ledgerNodeId,
        });
        throw err;
      }
      // result is inspected in unit tests
      callback(null, result);
    });
}

function _verifyGenesisEvents(
  {eventHash, genesisBlock, ledgerNodeId}, callback) {
  // genesis block must contain a config event and a merge event
  if(genesisBlock.event.length !== 2) {
    return callback(new BedrockError(
      'The genesis block must contain two events.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      }));
  }
  if(!genesisBlock.consensusProof || genesisBlock.consensusProof.length !== 1) {
    return callback(new BedrockError(
      'The genesis block `consensusProof` must contain exactly one event.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      }));
  }
  const proof = genesisBlock.consensusProof[0];
  const mergeEvent = _.find(genesisBlock.event, obj => {
    return obj.type === 'ContinuityMergeEvent';
  });
  // ensure the the gensis merge is a child of the genesis config
  // eventHash === hash of the genesis config
  // ensures events are in the proper order in `event`
  if(eventHash !== mergeEvent.parentHash[0]) {
    return callback(new BedrockError(
      'The genesis merge event is invalid.',
      'InvalidStateError', {
        httpStatusCode: 400,
        public: true,
      }));
  }
  async.auto({
    verify: callback => signature.verify(
      {doc: mergeEvent, ledgerNodeId}, (err, result) => {
        if(err) {
          return callback(err);
        }
        callback(null, {creator: result.keyOwner});
      }),
    proofHash: callback => hasher(proof, callback),
    mergeHash: callback => hasher(mergeEvent, callback),
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const {mergeHash, proofHash} = results;
    if(mergeHash !== proofHash) {
      return callback(new BedrockError(
        'The genesis proof event must be the genesis merge event.',
        'InvalidStateError', {
          httpStatusCode: 400,
          public: true,
        }));
    }
    const {creator} = results.verify;
    callback(null, {creator, mergeEvent, mergeHash});
  });
}

// eventHash is passed in for local regular events
function _writeEvent(
  {continuity2017, event, eventHash, genesis, ledgerNode, needed}, callback) {
  const ledgerNodeId = ledgerNode.id;
  const storage = ledgerNode.storage;
  const fromPeer = (continuity2017 && continuity2017.peer);
  async.auto({
    // add creator to local events
    process: callback => {
      if(fromPeer) {
        if(!needed) {
          throw new TypeError('`needed` argument is required.');
        }
        return _processPeerEvent({event, ledgerNode, needed}, callback);
      }
      if(!eventHash) {
        throw new TypeError('`eventHash` argument is required.');
      }
      if(genesis) {
        return callback(null, {event, meta: {eventHash}});
      }
      _processLocalEvent({event, eventHash, ledgerNode}, callback);
    },
    writeEvent: ['process', (results, callback) => {
      const {event, meta} = results.process;
      if(fromPeer) {
        // events from peers that can be re-acquired go into redis queue
        return _queueEvent({event, ledgerNodeId, meta}, err => {
          if(err) {
            return callback(err);
          }
          // return same data as mongo
          callback(null, {event, meta});
        });
      }
      // only local regular events pass here
      storage.events.add({event, meta}, (err, result) => {
        if(err && err.name === 'DuplicateError') {
          // ignore duplicates, may be recovering from a failed create operation
          return callback(null, result);
        }
        callback(err, result);
      });
    }],
    cacheUpdate: ['writeEvent', (results, callback) => {
      if(genesis || fromPeer) {
        return callback();
      }
      // applies to local regular events which are commited directly to mongo
      const {eventHash} = results.process.meta;
      const childlessKey = _cacheKey.childless(ledgerNodeId);
      const eventCountKey = _cacheKey.eventCountLocal(
        {ledgerNodeId, second: Math.round(Date.now() / 1000)});
      cache.client.multi()
        .sadd(childlessKey, eventHash)
        .incr(eventCountKey)
        .expire(eventCountKey, eventsConfig.counter.ttl)
        // inform ConsensusAgent and other listeners about new regular events
        .publish(`continuity2017|event|${ledgerNodeId}`, 'regular')
        .exec(callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, results.writeEvent);
  });
}
