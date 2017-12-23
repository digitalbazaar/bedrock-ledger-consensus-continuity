/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

// const _ = require('lodash');
const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const brDidClient = require('bedrock-did-client');
const brLedgerNode = require('bedrock-ledger-node');
const continuityStorage = require('./storage');
const database = require('bedrock-mongodb');
const hasher = brLedgerNode.consensus._hasher;
const jsigs = require('jsonld-signatures')();
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
          fields: {'meta.continuity2017.creator': 1},
          options: {sparse: true, background: false}
        }, {
          collection,
          fields: {'event.parentHash': 1},
          options: {sparse: true, background: false}
        }, {
          collection,
          fields: {'event.treeHash': 1},
          options: {sparse: true, background: false}
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
          conesnsusDate: now,
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
            type: 'CryptographicKey',
            owner: results.mergeEvent.creator.id,
            publicKeyPem: results.mergeEvent.creator.publicKey.publicKeyPem
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
  creatorId, eventHashFilter, eventTypeFilter, ledgerNode, startHash, treeHash
}, callback) => {
  logger.debug('Start aggregateHistory');
  const collection = ledgerNode.storage.events.collection;
  const restrictSearchWithMatch = {};
  // FIXME: optimize conditionals
  if(Array.isArray(eventHashFilter) && eventHashFilter.length !== 0
    && treeHash) {
    restrictSearchWithMatch.eventHash =
      {$nin: eventHashFilter.concat(treeHash)};
  } else if(Array.isArray(eventHashFilter) && eventHashFilter.length !== 0) {
    restrictSearchWithMatch.eventHash = {$nin: eventHashFilter};
  } else if(treeHash) {
    restrictSearchWithMatch.eventHash = {$ne: treeHash};
  }
  // const restrictSearchWithMatch = {
  //   eventHash: {$ne: treeHash},
  //   // FIXME: with meta excluded, the list includes the first
  //   // descendant in the _parents field, with it included,
  //   // nothing is in the _parents field
  //   // FIXME: would it be useful to filter based on the id of the
  //   // peer making the request?
  //   // 'meta.continuity2017.creator': {$ne: creatorId}
  // };
  // FIXME: allow creatorFilter to be an array?
  // if(creatorFilter) {
  //   restrictSearchWithMatch['meta.continuity2017.creator'] = {
  //     $ne: creatorFilter
  //   };
  // }
  if(eventTypeFilter) {
    restrictSearchWithMatch['event.type'] = eventTypeFilter;
  }
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
  {$sort: {'meta.created': 1}},
  // $push must be used here instead of $addToSet to maintain sort order
  {$group: {_id: null, mergeEventHashes: {$push: '$eventHash'}}}
  ]).toArray((err, result) => {
    if(err) {
      return callback(err);
    }
    if(result.length === 0) {
      return callback(null, []);
    }
    callback(null, result[0].mergeEventHashes);
  });
};

api.getHistory = ({
  creatorId, eventHashFilter, eventTypeFilter, ledgerNode, treeHash
}, callback) => {
  if(!(creatorId || ledgerNode)) {
    return callback(new BedrockError(
      'Either `creatorId` or `ledgerNode` must be specified.', 'DataError',
      {httpStatusCode: 400, public: true}));
  }
  async.auto({
    // voters.get API returns an error if voter is not found
    creator: callback => {
      const query = ledgerNode ? {ledgerNodeId: ledgerNode.id} :
        {voterId: creatorId};
      continuityStorage.voters.get(query, callback);
    },
    ledgerNode: ['creator', (results, callback) => {
      if(ledgerNode) {
        return callback(null, ledgerNode);
      }
      brLedgerNode.get(null, results.creator.ledgerNodeId, callback);
    }],
    firstDescendant: ['ledgerNode', (results, callback) => {
      const query = {
        'event.treeHash': treeHash,
        'event.type': 'ContinuityMergeEvent',
        'meta.continuity2017.creator': results.creator.id
      };
      results.ledgerNode.storage.events.collection.findOne(
        query, {eventHash: 1}, callback);
    }],
    aggregate: ['firstDescendant', (results, callback) => {
      if(!results.firstDescendant) {
        return callback();
      }
      api.aggregateHistory({
        creatorId,
        eventHashFilter,
        eventTypeFilter,
        ledgerNode: results.ledgerNode,
        startHash: results.firstDescendant.eventHash,
        treeHash
      }, callback);
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    if(!results.firstDescendant) {
      logger.debug('L232 getHistory did not find any events.');
      return callback(null, []);
    }
    callback(null, results.aggregate);
  });
};

// FIXME: documentation
/**
 * Get a linked summary of the history of all events that have not
 * yet reached consensus from the given ledger node. The history will be
 * indexed by the current set of electors.
 *
 * @param options the options to use
 *   ledgerNode the ledger node to get the history for.
 * @param callback(err, history) called once the operation completes.
 */
api._old_getRecentHistory = ({ledgerNode}, callback) => {
  const storage = ledgerNode.storage;
  const eventsCollection = storage.events.collection;
  async.auto({
    creator: callback => voters.get(ledgerNode.id, callback),
    head: ['creator', (results, callback) => api._getLocalBranchHead({
      creator: results.creator.id,
      eventsCollection
    }, callback)],
    headEvent: ['head', (results, callback) => {
      ledgerNode.events.get(results.head, callback);
    }],
    aggregate: ['headEvent', (results, callback) => {
      eventsCollection.aggregate([{
        $match: {eventHash: results.head},
      }, {
        $graphLookup: {
          from: eventsCollection.s.name,
          startWith: results.head,
          connectFromField: 'event.parentHash',
          connectToField: 'eventHash',
          as: '_parents',
          restrictSearchWithMatch: {
            'meta.consensusDate': {$exists: false},
            eventHash: {$ne: results.headEvent.event.treeHash}
          },
          // maxDepth: 10, // max generations to go back (need to be infinite
          // and handle out-of-memory degenerate case?)
          // depthField: "_distanceFromHead",
        }
      },
      {$unwind: '$_parents'},
      {$replaceRoot: {newRoot: '$_parents'}},
      // the order of events is unpredictable without this sort, and we
      // must ensure that events are added in chronological order
      {$sort: {'meta.created': 1}},
      // {$group: {_id: '$meta.continuity2017.creator', events: {$push: '$event'}}}
      {$group: {_id: '$meta.continuity2017.creator', events: {$push: '$eventHash'}}}
      ]).toArray(callback);
    }]
  }, (err, result) => {
    if(err) {
      return callback(err);
    }
    callback(null, result.aggregate);
  });
  // TODO: the call to _getElectorBranches described below should be performed
  // by the calling function, not here in order to keep APIs discrete
  // TODO: call _getElectorBranches({event: history, electors}) in the
  //   graphLookup callback to produce linked history and return this in
  //   `callback` for `api.getRecentHistory`
};

// called from consensus worker
api.getRecentHistory = ({creatorId, ledgerNode}, callback) => {
  const eventMap = {};
  const events = [];
  const storage = ledgerNode.storage;
  async.auto({
    creator: callback => {
      logger.debug('getRecentHistory creator');
      if(creatorId) {
        return callback(null, creatorId);
      }
      voters.get(ledgerNode.id, (err, result) =>
        err ? callback(err) : callback(null, result.id));
    },
    treeHash: ['creator', (results, callback) => api._getLocalBranchHead({
      eventsCollection: storage.events.collection,
      creator: results.creator
    }, (err, result) => {
      logger.debug('getRecentHistory after treeHash');
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
      logger.debug('getRecentHistory events');
      const query = {
        $or: [{
          // 'event.type': {$ne: 'ContinuityMergeEvent'},
          'meta.continuity2017.creator': results.creator,
          'meta.consensus': {$exists: false}
        }, {
          'event.type': 'ContinuityMergeEvent',
          // FIXME: REVIEW THIS QUERY
          'meta.continuity2017.creator': {$ne: results.creator},
          // FIXME: added check on `eventHash` because nodes that were
          // initialized with a genesisBlock did not create its genesis
          // merge event, it was matching here, there may be a proper/better way
          // eventHash: {$ne: results.treeHash},
          'meta.consensus': {$exists: false}
        }]
      };
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
      logger.debug('getRecentHistory process');
      for(const e of events) {
        for(const parentHash of e.event.parentHash) {
          if(!eventMap[parentHash]) {
            continue;
          }
          const parent = eventMap[parentHash];
          // TODO: remove `_parents` if it is unused
          e._parents.push(parent);
          parent._children.push(e);
        }
      }
      callback(null, {events, eventMap, localBranchHead: results.treeHash});
    }],
  }, (err, results) => callback(err, results.process));
};

// FIXME: documentation
api.mergeBranches = ({ledgerNode, history}, callback) => {
  if(ledgerNode === undefined || history === undefined) {
    throw new TypeError('`ledgerNode` and `history` are required.');
  }

  // determine if there are events with no children that can be merged
  const eventsWithNoChildren = history.events
    .filter(e => !e._children.length);
  if(eventsWithNoChildren.length === 1 &&
    eventsWithNoChildren[0].eventHash === history.localBranchHead) {
    // nothing to merge, return `null`
    return callback(null, null);
  }

  const storage = ledgerNode.storage;
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: ['WebLedgerEvent', 'ContinuityMergeEvent'],
  };

  // if no local events have been added, it is correct that
  // localBranchHead would be included along with events from other nodes
  const parentHashes = eventsWithNoChildren.map(e => e.eventHash);
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
      algorithm: 'LinkedDataSignature2015',
      privateKeyPem: results.creator.publicKey.privateKey.privateKeyPem,
      creator: results.creator.publicKey.id
    }, callback)],
    eventHash: ['sign', (results, callback) => hasher(results.sign, callback)],
    write: ['eventHash', (results, callback) => {
      const {eventHash, creator} = results;
      const meta = {continuity2017: {creator: creator.id}, eventHash};
      storage.events.add(results.sign, meta, callback);
    }],
    updateHistory: ['write', (results, callback) => {
      const record = {
        eventHash: results.eventHash,
        event: {
          treeHash: event.treeHash,
          parentHash: event.parentHash
        }
      };
      history.localBranchHead = record.eventHash;
      history.eventMap[record.eventHash] = record;
      history.events.push(record);
      record._treeParent = history.eventMap[event.treeHash];
      record._parents = eventsWithNoChildren;
      if(record._treeParent) {
        record._treeParent._treeChildren = [record];
        if(!parentHashes.includes(event.treeHash)) {
          record._parents.shift(record._treeParent);
        }
      }
      for(const parent of record._parents) {
        parent._children.push(record);
      }
      callback();
    }]
  }, (err, results) => err ? callback(err) : callback(null, results.write));
};

// FIXME: make this a regular API call.  It's used in worker.js as well.
// and use an appropriate name since
// this is not just for local branch, this finds the latest merge event from
// the specified creator
// exposed for testing
api._getLocalBranchHead = ({eventsCollection, creator}, callback) => {
  async.auto({
    localMerge: callback => {
      // find the latest merge event
      const query = {
        'event.type': 'ContinuityMergeEvent',
        'meta.deleted': {$exists: false},
        'meta.continuity2017.creator': creator
      };
      const projection = {'eventHash': 1, _id: 0};
      // NOTE: using natural order sort instead of meta.created to avoid
      // potential clock skew issues
      eventsCollection.find(query, projection).sort({$natural: -1})
        .limit(1).toArray((err, result) => {
          if(err) {
            return callback(err);
          }
          const eventHash = result.length === 1 ? result[0].eventHash : null;
          callback(null, eventHash);
        });
    },
    genesisMerge: ['localMerge', (results, callback) => {
      if(results.localMerge) {
        return callback();
      }
      // there was no local merge event, find the genesis merge event
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
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    if(results.localMerge) {
      return callback(null, results.localMerge);
    }
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
    type: ['WebLedgerEvent', 'ContinuityMergeEvent'],
    parentHash: [eventHash],
  };
  async.auto({
    creator: callback => voters.get(ledgerNode.id, callback),
    signedMergeEvent: ['creator', (results, callback) =>
      jsigs.sign(mergeEvent, {
        algorithm: 'LinkedDataSignature2015',
        privateKeyPem: results.creator.publicKey.privateKey.privateKeyPem,
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
  const eventType = [].concat(event.type);
  const fromPeer = (options.continuity2017 && options.continuity2017.peer);
  const meta = {};
  async.auto({
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
        creator: results.localCreator.id
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
      if(!(fromPeer && eventType.includes('ContinuityMergeEvent'))) {
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
          return callback(new BedrockError(
            'Events from `parentHash` and/or `treeHash` are missing.',
            'InvalidStateError', {
              httpStatusCode: 400,
              public: true,
              event,
            }));
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
    eventValidator: ['config', (results, callback) => {
      const config = results.config;
      const eventValidator = config.eventValidator || [];
      const requireEventValidation = config.requireEventValidation || false;
      brLedgerNode.consensus._validateEvent(
        event, eventValidator, {requireEventValidation}, callback);
    }],
    writeEvent: ['eventValidator', 'hasMerge', 'localCreator',
      (results, callback) => {
        meta.eventHash = results.eventHash;
        storage.events.add(event, meta, callback);
      }],
    // if this is a remote merge event, add a creator to regular remote events
    // FIXME: what are the consequences if writeEvent succeeds but this update
    // fails?
    // FIXME: if this is needed, the genesis WebLedgerConfiguration event is
    // getting updated in test suites to a creator that is not the originator
    // of the genesis merge event
    // updateEvents: ['writeEvent', (results, callback) => {
    //   if(!(fromPeer && eventType.includes('ContinuityMergeEvent'))) {
    //     return callback();
    //   }
    //   const query = {
    //     eventHash: {$in: event.parentHash},
    //     'event.type': {$ne: 'ContinuityMergeEvent'}
    //   };
    //   storage.events.collection.update(query, {
    //     $set: {
    //       'meta.continuity2017.creator': meta.continuity2017.creator
    //     }
    //   }, callback);
    // }]
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
