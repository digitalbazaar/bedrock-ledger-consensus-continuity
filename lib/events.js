/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

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
      // add indexes specific to Continuity
      createIndexes: callback => {
        const collection = storage.events.collection.s.name;
        database.createIndexes([{
          collection,
          fields: {
            'event.type': 1,
            'meta.deleted': 1,
            'meta.continuity2017.creator': 1,
            'meta.consensus': 1,
            'meta.created': 1,
            'eventHash': 1,
          },
          options: {
            background: false,
            name: 'continuityCommon1',
            unique: true
          }
        }, {
          collection,
          fields: {
            'meta.consensus': 1,
            'meta.continuity2017.creator': 1,
            'event.type': 1,
          },
          options: {
            background: false,
            name: 'recentHistory',
            unique: false
          }
        }, {
          collection,
          fields: {
            'eventHash': 1,
            'meta.continuity2017.creator': 1,
            'event.type': 1,
            'meta.created': 1,
          },
          options: {
            background: false,
            name: 'aggregateHistory',
            unique: true
          }
        }, {
          collection,
          fields: {
            'eventHash': 1,
            'event.parentHash': 1
          },
          options: {sparse: false, background: false}
        }], callback);
      },
      mergeEvent: callback => {
        if(options.genesisBlock) {
          const event = options.genesisBlock.consensusProof[0];
          return signature.verify(
            {doc: event, ledgerNodeId: ledgerNode.id}, (err, result) => {
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
        const {eventHash, mergeEvent} = results;
        const meta = {
          consensus: true,
          consensusDate: now,
          eventHash,
          continuity2017: {
            creator: results.mergeEvent.creator.id
          },
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
        {'meta.continuity2017.creator': r.creator},
        {'meta.created': {$lte: r.created}},
        // regular events *should* be included, regardless of created
        {'event.type': 'ContinuityMergeEvent'}
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
  // do not send an empty $nor to mongo
  if(restrictSearchWithMatch.$nor.length === 0) {
    delete restrictSearchWithMatch.$nor;
  }
  // logger.debug(
  //   'Aggregate history parameters', {restrictSearchWithMatch, startHash});
  // console.log('RRRRRRR', JSON.stringify(restrictSearchWithMatch, null, 2));

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
  collection.aggregate([{
    $match: {eventHash: startHash},
  }, {
    $graphLookup: {
      from: collection.s.name,
      startWith: '$eventHash',
      connectFromField: 'event.parentHash',
      connectToField: 'eventHash',
      depthField: '_depth',
      as: '_parents',
      restrictSearchWithMatch
    }
  },
  {$unwind: '$_parents'},
  {$replaceRoot: {newRoot: '$_parents'}},
  // the order of events is unpredictable without this sort, and we
  // must ensure that events are added in chronological order
  {$sort: {'meta.created': 1}},
  ], {allowDiskUse: true}).toArray((err, result) => {
    if(err) {
      return callback(err);
    }
    callback(null, result);
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
    events: ['treeHash', (results, callback) => {
      const query = {
        'meta.consensus': {$exists: false}
      };
      if(excludeLocalRegularEvents) {
        // simply get all merge events
        query['event.type'] = 'ContinuityMergeEvent';
      } else {
        // get all events from creator and only merge events from non-creator
        query.$or = [{
          'meta.continuity2017.creator': creatorId
        }, {
          'event.type': 'ContinuityMergeEvent',
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
    }],
    process: ['events', (results, callback) => {
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
    eventsWithNoChildren[0].eventHash === history.localBranchHead) {
    // nothing to merge, return `null`
    logger.debug('mergeBranches nothing to merge.');
    return callback(null, null);
  }

  const storage = ledgerNode.storage;
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

  event.treeHash = history.localBranchHead;
  if(parentHashes.includes(event.treeHash)) {
    event.parentHash = parentHashes;
  } else {
    event.parentHash = [event.treeHash, ...parentHashes];
  }

  async.auto({
    // FIXME: maybe creator will be supplied as a parameter?
    creator: callback => voters.get(ledgerNode.id, callback),
    sign: ['creator', (results, callback) => jsigs.sign(event, {
      algorithm: 'Ed25519Signature2018',
      privateKeyBase58: results.creator.publicKey.privateKey.privateKeyBase58,
      creator: results.creator.publicKey.id
    }, callback)],
    eventHash: ['sign', (results, callback) => hasher(results.sign, callback)],
    write: ['eventHash', (results, callback) => {
      const {eventHash, creator} = results;
      const meta = {continuity2017: {creator: creator.id}, eventHash};
      storage.events.add(results.sign, meta, callback);
    }],
    // FIXME: is a race possible wrt to this?
    cache: ['write', (results, callback) => {
      const headCacheKey =
        `head|${ledgerNode.id}|${results.creator.id.substr(-43)}`;
      cache.client.set(headCacheKey, results.eventHash, callback);
    }],
  }, (err, results) => err ? callback(err) : callback(null, results.write));
};

// FIXME: make this a regular API call.  It's used in worker.js as well.
// and use an appropriate name since
// this is not just for local branch, this finds the latest merge event from
// the specified creator
api._getLocalBranchHead = (
  {eventsCollection, creatorId, ledgerNodeId}, callback) => {
  const headCacheKey = `head|${ledgerNodeId}|${creatorId.substr(-43)}`;
  const genesisCacheKey = `genesis|${ledgerNodeId}|${creatorId.substr(-43)}`;
  async.auto({
    headCache: callback => cache.client.get(headCacheKey, callback),
    localMerge: ['headCache', (results, callback) => {
      if(results.headCache) {
        return callback(null, results.headCache);
      }
      // find the latest merge event
      const query = {
        'event.type': 'ContinuityMergeEvent',
        'meta.deleted': {$exists: false},
        'meta.continuity2017.creator': creatorId
      };
      const projection = {'eventHash': 1, _id: 0};
      eventsCollection.find(query, projection, {hint: 'continuityCommon1'})
        .sort({'meta.created': -1})
        .limit(1).toArray((err, result) => {
          if(err) {
            return callback(err);
          }
          if(result.length === 0) {
            return callback();
          }
          cache.client.set(headCacheKey, result[0].eventHash, err => {
            if(err) {
              return callback(err);
            }
            callback(null, result[0].eventHash);
          });
        });
    }],
    genesisCache: ['localMerge', (results, callback) => {
      if(results.localMerge) {
        return callback();
      }
      cache.client.get(genesisCacheKey, callback);
    }],
    genesisMerge: ['genesisCache', (results, callback) => {
      if(results.localMerge) {
        return callback();
      }
      if(results.genesisCache) {
        return callback(null, results.genesisCache);
      }
      // there was no local merge event, find the genesis merge event
      const query = {
        'event.type': 'ContinuityMergeEvent',
        'meta.deleted': {$exists: false},
      };
      const projection = {'eventHash': 1, _id: 0};
      // the oldest ContinuityMergeEvent is the genesis merge event
      eventsCollection.find(query, projection).sort({'meta.created': 1})
        .limit(1).toArray((err, result) => {
          if(err) {
            return callback(err);
          }
          if(result.length === 0) {
            return callback(new BedrockError(
              'The genesis merge event was not found.',
              'InvalidStateError', {
                httpStatusCode: 400,
                public: true,
              }));
          }
          cache.client.set(genesisCacheKey, result[0].eventHash, err => {
            if(err) {
              return callback(err);
            }
            callback(null, result[0].eventHash);
          });
        });
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    results.localMerge ? callback(null, results.localMerge) :
      callback(null, results.genesisMerge);
  });
};

api.getGenesisMergeHash = ({eventsCollection}, callback) => {
  const query = {
    'event.type': 'ContinuityMergeEvent',
    'meta.deleted': {$exists: false},
  };
  const projection = {'eventHash': 1, _id: 0};
  // the oldest ContinuityMergeEvent is the genesis merge event
  // FIXME: use $natural sort
  eventsCollection.find(query, projection).sort({'meta.created': 1})
    .limit(1).toArray((err, result) => {
      if(err) {
        return callback(err);
      }
      if(result.length === 0) {
        return callback(new BedrockError(
          'The genesis merge event was not found.',
          'InvalidStateError', {
            httpStatusCode: 400,
            public: true,
          }));
      }
      const eventHash = result[0].eventHash;
      callback(null, eventHash);
    });
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

function _writeEvent(ledgerNode, event, options, callback) {
  const storage = ledgerNode.storage;
  const fromPeer = (options.continuity2017 && options.continuity2017.peer);
  const meta = {};
  async.auto({
    count: callback => {
      const cacheKey = `events-${Math.round(Date.now() / 1000)}`;
      cache.client.incr(cacheKey, err => {
        if(err) {
          return callback(err);
        }
        // expire key in 10 mins
        cache.client.expire(cacheKey, 6000, callback);
      });
    },
    // add creator to local events
    localCreator: callback => {
      if(fromPeer) {
        return callback();
      }
      voters.get(ledgerNode.id, (err, result) => {
        if(err) {
          return callback(err);
        }
        // FIXME: is setting creator needed? One side effect is that the genesis
        // WebLedgerConfiguration event is getting tagged, perhaps this is
        // acceptable?
        meta.continuity2017 = {
          creator: result.id
        };
        callback(null, result);
      });
    },
    localEvent: ['localCreator', (results, callback) => {
      // add treeHash and parentHash to local events
      if(fromPeer || options.genesis) {
        return callback();
      }
      api._getLocalBranchHead({
        eventsCollection: storage.events.collection,
        creatorId: results.localCreator.id,
        ledgerNodeId: ledgerNode.id
      }, (err, result) => {
        if(err) {
          return callback(err);
        }
        // add the hash of the local merge event as the treeHash of this event
        event.treeHash = result;
        event.parentHash = [result];
        callback();
      });
    }],
    validate: ['localEvent', (results, callback) => {
      // if the event came from a peer through the HTTP API, it's already
      // been validated
      if(fromPeer) {
        return callback();
      }
      validate('continuity.webLedgerEvents', event, callback);
    }],
    signature: ['validate', (results, callback) => {
      // only check signatures on ContinuityMergeEvent
      // ContinuityMergeEvents are guaranteed to have signatures here
      if(!(fromPeer &&
        jsonld.hasValue(event, 'type', 'ContinuityMergeEvent'))) {
        return callback();
      }
      signature.verify(
        {doc: event, ledgerNodeId: ledgerNode.id}, (err, result) => {
          if(err) {
            return callback(err);
          }
          // capture the key owner id
          meta.continuity2017 = {
            creator: result.keyOwner.id
          };
          // success
          callback();
        });
    }],
    eventHash: ['validate', (results, callback) => hasher(event, callback)],
    hasMerge: ['eventHash', (results, callback) => {
      if(!fromPeer) {
        return callback();
      }
      if(!event.parentHash.includes(event.treeHash)) {
        return callback(new BedrockError(
          'parentHash must include the value of treeHash',
          'DataError', {
            httpStatusCode: 400,
            public: true,
            event,
          }));
      }
      // ensure that all parentHash events exist
      storage.events.exists(event.parentHash, (err, result) => {
        if(err) {
          return callback(err);
        }
        if(!result) {
          return storage.events.difference(event.parentHash, (err, result) => {
            // ignore err
            callback(new BedrockError(
              'Events from `parentHash` and/or `treeHash` are missing.',
              'InvalidStateError', {
                event,
                httpStatusCode: 400,
                missing: result,
                public: true,
              }));
          });
        }
        // success
        callback();
      });
    }],
    config: ['signature', (results, callback) => {
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
    writeEvent: ['validator', 'hasMerge', 'localCreator',
      (results, callback) => {
        meta.eventHash = results.eventHash;
        storage.events.add(event, meta, callback);
      }],
    cacheHead: ['writeEvent', (results, callback) => {
      if(!jsonld.hasValue(event, 'type', 'ContinuityMergeEvent')) {
        return callback();
      }
      const headCacheKey =
        `head|${ledgerNode.id}|${meta.continuity2017.creator.substr(-43)}`;
      cache.client.set(headCacheKey, meta.eventHash, callback);
    }]
  }, (err, results) => err ?
    callback(err) : callback(null, results.writeEvent));
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
