/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
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
  {continuity2017, event, genesis, genesisBlock, ledgerNode, opHash},
  callback) => {
  const ledgerNodeId = ledgerNode.id;
  const storage = ledgerNode.storage;
  if(genesisBlock) {
    // do not mutate genesisBlock
    genesisBlock = bedrock.util.clone(genesisBlock);
  }
  _writeEvent({
    continuity2017, event, genesis, ledgerNode, opHash
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
            'eventHash': 1,
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
      mergeEvent: callback => {
        if(genesisBlock) {
          const event = genesisBlock.consensusProof[0];
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
        const creator = results.mergeEvent.creator.id;
        const {eventHash, mergeEvent} = results;
        const meta = {
          consensus: true,
          consensusDate: now,
          continuity2017: {creator, generation: 0, type: 'm'},
          eventHash,
        };
        storage.events.add({event: mergeEvent.event, meta}, callback);
      }],
      storeGenesisMerge: ['mergeEvent', (results, callback) => {
        if(!genesisBlock) {
          return callback();
        }
        const mergeEvent = genesisBlock.event[1];
        async.auto({
          eventHash: callback => hasher(mergeEvent, callback),
          store: ['eventHash', (results, callback) => {
            return callback();
            const meta = {eventHash: results.eventHash};
            storage.events.add({event, meta}, callback);
          }]
        }, (err, results) => {
          if(err) {
            return callback(err);
          }
          genesisBlock.event = [
            eventRecord.meta.eventHash, results.eventHash
          ];
          callback();
        });
      }],
      createBlock: [
        'mergeEventRecord', 'storeGenesisMerge', (results, callback) => {
          if(genesisBlock) {
            // use given genesis block, mirroring an existing ledger
            return callback(null, genesisBlock);
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

            event: [
              eventRecord.meta.eventHash,
              results.mergeEventRecord.meta.eventHash
            ],

            // FIXME: make this work
            // consensusProofHash: [results.mergeEventRecord.meta.eventHash],

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
      writeBlock: ['createBlock', (results, callback) => _writeGenesisBlock({
        block: results.createBlock, eventRecord, storage}, callback)],
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

// TODO: do ACID magic
api.create = ({ledgerNode}, callback) => {
  logger.debug('Attempting to create an event.');
  const ledgerNodeId = ledgerNode.id;
  const opSetKey = _cacheKey.operationSet(ledgerNodeId);
  async.auto({
    opKeys: callback => cache.client.smembers(opSetKey, (err, result) => {
      if(err) {
        return callback(err);
      }
      if(!result || result.length === 0) {
        logger.debug('No new operations.');
        return callback(new BedrockError('Nothing to do.', 'AbortError'));
      }
      logger.debug(`New operations found: ${result.length}`);
      callback(null, result);
    }),
    ops: ['opKeys', (results, callback) => cache.client.mget(
      results.opKeys, (err, result) => {
        if(err) {
          return callback(err);
        }
        // TODO: how should operations be ordered if at all?
        // ordering by opHash now
        const operations = [];
        for(let i = 0; i < result.length; ++i) {
          operations.push(JSON.parse(result[i]));
        }
        operations.sort((a, b) => a.opHash - b.opHash);
        const operation = [];
        const opHash = [];
        // FIXME: is `opHash` useful?
        for(let i = 0; i < operations.length; ++i) {
          operation.push(operations[i].operation);
          opHash.push(operations[i].opHash);
        }
        callback(null, {operation, opHash});
      })],
    event: ['ops', (results, callback) => {
      // NOTE: must ensure that `event` document that includes operations does
      // not exceed 16MB
      const opChunks = _.chunk(results.ops.operation, 500);
      // TODO: no need to do this in series? maybe to recover from failure?
      async.each(opChunks, (operation, callback) => {
        const event = {
          '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
          type: 'WebLedgerOperationEvent',
          operation
        };
        api.add({event, ledgerNode}, callback);
      }, callback);
    }],
    cache: ['event', (results, callback) => {
      cache.client.multi()
        .srem(opSetKey, results.opKeys)
        .del(results.opKeys)
        .exec(callback);
    }]
  }, err => {
    if(err && err.name !== 'AbortError') {
      return callback(err);
    }
    callback();
  });
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
    eventHash: {$in: eventHashes.map(h => database.hash(h))}},
  {_id: 0, 'meta.eventHash': 1}
  ).forEach(e => delete headMap[e.meta.eventHash], err => {
    err ? callback(err) : callback(null, headMap);
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
          const {eventHash} = parsed;
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

// FIXME: remove
/*
api.getRecentHistory_old = (
  {creatorId, ledgerNode, excludeLocalRegularEvents = false, link = false},
  callback) => {
  logger.verbose('Start getRecentHistory');
  const startTime = Date.now();
  const eventMap = {};
  const events = [];
  const storage = ledgerNode.storage;
  async.auto({
    treeHash: callback => api._getLocalBranchHead(
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
    events: callback => {
      const query = {'meta.consensus': {$exists: false}};
      if(excludeLocalRegularEvents) {
        // simply get all merge events
        _.assign(query, {'meta.continuity2017.type': 'm'});
      } else {
        // get all events from creator and only merge events from non-creator
        query.$or = [{
          'meta.continuity2017.creator': creatorId
        }, {
          'meta.continuity2017.type': 'm',
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
      const duration = Date.now() - startTime;
      if(excludeLocalRegularEvents) {
        cache.client.set(`recentHistoryMergeOnly|${ledgerNode.id}`, duration);
      } else {
        cache.client.set(`recentHistory|${ledgerNode.id}`, duration);
      }
      logger.verbose('End getRecentHistory', {duration});
      callback(null, history);
    }],
  }, (err, results) => callback(err, results.process));
};*/

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
        cache: ['write', (results, callback) => {
          // TODO: remove items placed into parent hash from childesskey
          const {write: eventRecord} = results;
          const eventHash = results.eventHash;
          const headKey = _cacheKey.head({creatorId, ledgerNodeId});
          const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
          const outstandingMergeKey = _cacheKey.outstandingMerge(ledgerNodeId);
          const {parentHash, treeHash, type} = eventRecord.event;
          const {creator} = eventRecord.meta.continuity2017;
          // no need to set a headGeneration key here, those are only used for
          // processing peer merge events
          // TODO: `creator` is quite a long URL, can a substitution be made?

          // eventHash here is ni://
          const cacheEvent = JSON.stringify({
            eventHash,
            event: {parentHash, treeHash, type},
            meta: {continuity2017: {creator}}
          });
          cache.client.multi()
            .srem(childlessKey, parentHashes)
            .set(eventKey, cacheEvent)
            .sadd(outstandingMergeKey, eventKey)
            .hmset(headKey, 'h', eventHash, 'g', nextGeneration)
            .publish('continuity2017.event', 'merge')
            .exec(err => {
              if(err) {
                // FIXME: fail gracefully
                // failure here means head information would be corrupt which
                // cannot be allowed
                logger.error('Could not set head.', {
                  creatorId,
                  // FIXME: fix when logger.error works properly
                  err1: err,
                  headKey,
                  ledgerNodeId,
                  nextGeneration,
                });
                throw err;
              }
              callback();
            });
        }],
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
/*
api.merge_old = ({creatorId, ledgerNode}, callback) => {
  async.auto({
    history: callback => api.getRecentHistory(
      {creatorId, ledgerNode}, callback),
    // NOTE: mergeBranches mutates history
    merge: ['history', (results, callback) => api.mergeBranches(
      {history: results.history, ledgerNode}, callback)]
  }, (err, results) => err ? callback(err) : callback(err, results.merge));
};*/

// FIXME: documentation
// NOTE: mutates history
/*
api.mergeBranches_old = ({ledgerNode, history}, callback) => {
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
    creator: callback => voters.get({ledgerNodeId}, callback),
    sign: callback => voters.sign({ledgerNodeId, doc: event}, callback),
    eventHash: ['sign', (results, callback) => hasher(results.sign, callback)],
    // local merge events must be written directly to the database
    write: ['creator', 'eventHash', (results, callback) => {
      const {eventHash, creator} = results;
      const meta = {continuity2017: {
        creator: creator.id,
        generation: nextGeneration,
        type: 'm'
      }, eventHash};
      ledgerNode.storage.events.add({event: results.sign, meta}, callback);
    }],
    cache: ['write', (results, callback) => {
      const creatorId = results.creator.id;
      const headKey = _cacheKey.head({creatorId, ledgerNodeId});
      const eventHash = results.eventHash;
      // no need to set a headGeneration key here, those are only used for
      // processing peer merge events
      cache.client.hmset(headKey, 'h', eventHash, 'g', nextGeneration, err => {
        if(err) {
          // FIXME: fail gracefully
          // failure here means head information would be corrupt which
          // cannot be allowed
          logger.error('Could not set head.', {
            creatorId,
            error: err,
            headKey,
            ledgerNodeId,
            nextGeneration,
          });
          throw err;
        }
        callback();
      });
    }],
  // results.write is used in unit tests
  }, (err, results) => err ? callback(err) : callback(null, results.write));
};*/

api.getCreatorHeads = (
  {latest = false, ledgerNode, peerId}, callback) => async.auto({
  // get ids for all the creators known to the node
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
      const query = {eventHash: {$in: cacheMisses}};
      const projection = {
        _id: 0, eventHash: 1, 'meta.continuity2017.generation': 1
      };
      const mongo = new Map();
      collection.find(query, projection).forEach(r => {
        headGenerationMap.set(r.eventHash, r.meta.continuity2017.generation);
        mongo.set(r.eventHash, r.meta.continuity2017.generation);
      }, err => err ? callback(err) : callback(null, mongo));
    }],
    cache: ['mongo', (results, callback) => {
      if(!results.mongo || results.mongo.size === 0) {
        return callback();
      }
      console.log('!!!!!!!!!!!!!!!!!!', results.mongo);
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
  {creatorId, latest = false, ledgerNode}, callback) => {
  const ledgerNodeId = ledgerNode.id;
  const eventsCollection = ledgerNode.storage.events.collection;
  const headCacheKey = _cacheKey.head({creatorId, ledgerNodeId});
  async.auto({
    latest: callback => {
      if(!latest) {
        return callback();
      }
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
function _checkAncestors({eventHashes, ledgerNode}, callback) {
  if(eventHashes.length === 0) {
    return callback();
  }
  _diff({eventHashes, ledgerNode}, (err, result) => {
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
    event: callback => voters.sign({doc, ledgerNodeId}, callback),
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const {creator, event} = results;
    callback(null, {creator, event});
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

function _processLocalEvent({event, ledgerNode}, callback) {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => voters.get({ledgerNodeId}, callback),
    ancestry: ['creator', (results, callback) => _addAncestry(
      {creatorId: results.creator.id, event, ledgerNode}, callback)],
    validate: ['ancestry', (results, callback) =>
      validate('continuity.webLedgerEvents', results.ancestry, callback)],
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    const event = results.ancestry;
    const meta = {continuity2017: {creator: results.creator.id, type: 'r'}};
    callback(null, {event, meta});
  });
}

function _processPeerMergeEvent({event, ledgerNode}, callback) {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    parentGeneration: callback => _getGeneration(
      {eventHash: event.treeHash, ledgerNode}, callback),
    localCreatorId: callback => voters.get({ledgerNodeId}, (err, result) =>
      err ? callback(err) : callback(null, result.id)),
    signature: callback => signature.verify(
      {doc: event, ledgerNodeId: ledgerNode.id}, (err, result) => {
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
    const {creator} = results.signature;
    if(creator === results.localCreatorId) {
      return callback(new BedrockError(
        'Merge events created by the local node cannot be added with this API.',
        'NotSupportedError', {
          httpStatusCode: 400,
          public: true,
        }));
    }
    callback(null, {creator, parentGeneration: results.parentGeneration});
  });
}

function _processPeerEvent({event, ledgerNode}, callback) {
  if(!jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
    return callback(null, {event, meta: {continuity2017: {type: 'r'}}});
  }
  _processPeerMergeEvent({event, ledgerNode}, (err, result) => {
    if(err) {
      return callback(err);
    }
    const {creator, parentGeneration} = result;
    const meta = {continuity2017: {
      creator,
      generation: parentGeneration + 1,
      type: 'm'
    }};
    callback(null, {event, meta});
  });
}

function _queueEvent({event, ledgerNodeId, meta}, callback) {
  const eventHash = meta.eventHash;
  const eventKey = _cacheKey.event({eventHash, ledgerNodeId});
  const eventQueueKey = _cacheKey.eventQueue(ledgerNodeId);
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  let eventJson;
  try {
    eventJson = JSON.stringify({event, eventHash, meta});
  } catch(err) {
    return callback(err);
  }
  // NOTE: multi's are atomic so there is no need
  const multi = cache.client.multi();
  // add the hash to the set used to check for dups and ancestors
  multi.sadd(eventQueueSetKey, eventHash);
  // create a key that contains the event and meta
  multi.set(eventKey, eventJson);
  // push to the list that is handled in the event-writer
  multi.rpush(eventQueueKey, eventKey);
  if(meta.continuity2017.type === 'm') {
    const headGenerationKey = _cacheKey.headGeneration(
      {eventHash, ledgerNodeId});
    // this key is set to expire in the event-writer
    multi.set(headGenerationKey, meta.continuity2017.generation);
  }
  multi.exec(callback);
}

function _writeEvent(
  {continuity2017, event, genesis, ledgerNode}, callback) {
  const ledgerNodeId = ledgerNode.id;
  const storage = ledgerNode.storage;
  const fromPeer = (continuity2017 && continuity2017.peer);
  async.auto({
    // add creator to local events
    process: callback => {
      if(genesis) {
        return callback(null, {event, meta: {}});
      }
      if(fromPeer) {
        return _processPeerEvent({event, ledgerNode}, callback);
      }
      _processLocalEvent({event, ledgerNode}, callback);
    },
    // waiting here only to save cycles in event of failure in process
    config: ['process', (results, callback) => {
      if(genesis) {
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
        return _queueEvent({event, ledgerNodeId, meta}, err => {
          if(err) {
            return callback(err);
          }
          // return same data as mongo
          callback(null, {event, meta});
        });
      }
      // only local regular events pass here
      storage.events.add({event, meta}, callback);
    }],
    cacheUpdate: ['writeEvent', (results, callback) => {
      // peer events are counted in the EventWriter
      if(genesis) {
        return callback();
      }
      const eventHash = results.process.meta.eventHash;
      if(fromPeer) {
        const {creator: creatorId, generation, type} =
          results.process.meta.continuity2017;
        if(type === 'r') {
          const opCountKey = _cacheKey.opCountPeer(
            {ledgerNodeId, second: Math.round(Date.now() / 1000)});
          const {operation} = results.process.event;
          return cache.client.incrby(opCountKey, operation.length, callback);
        }
        const latestPeerHeadKey = _cacheKey.latestPeerHead(
          {creatorId, ledgerNodeId});
        return cache.client.multi()
          // expired the key in an hour, incase the peer/creator goes dark
          .hmset(latestPeerHeadKey, 'h', eventHash, 'g', generation)
          .expire(latestPeerHeadKey, 3600)
          .exec(callback);
      }
      // applies to local regular events which are commited directly to mongo
      const childlessKey = _cacheKey.childless(ledgerNodeId);
      const eventCountKey = _cacheKey.eventCountLocal(
        {ledgerNodeId, second: Math.round(Date.now() / 1000)});
      cache.client.multi()
        .sadd(childlessKey, eventHash)
        .incr(eventCountKey)
        // inform ConsensusAgent and other listeners about new regular events
        .publish('continuity2017.event', 'regular')
        .exec(callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    callback(null, results.writeEvent);
  });
}

function _writeGenesisBlock({block, storage}, callback) {
  async.auto({
    hashBlock: callback => hasher(block, callback),
    writeBlock: ['hashBlock', (results, callback) => {
      const meta = {
        blockHash: results.hashBlock,
        consensus: true,
        consensusDate: Date.now()
      };
      storage.blocks.add({block, meta}, callback);
    }]
  }, callback);
}
