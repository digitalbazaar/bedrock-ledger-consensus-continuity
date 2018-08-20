/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cacheKey = require('./cache-key');
const _client = require('./client');
const _events = require('./events');
const _voters = require('./voters');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const {callbackify, BedrockError} = bedrock.util;
const logger = require('./logger');
const uuid = require('uuid/v4');
const JSONStream = require('JSONStream');
const PQueue = require('p-queue');

const api = {};
module.exports = api;

// communicate the very latest heads to the peer
api.gossipWith = callbackify(async ({ledgerNode, peerId}) => {
  logger.verbose('Start gossipWith', {peerId});
  const startTime = Date.now();
  try {
    const ledgerNodeId = ledgerNode.id;

    const [creator, creatorHeads] = await Promise.all([
      _voters.get({ledgerNodeId}),
      _events.getCreatorHeads({latest: true, ledgerNode, peerId})]);

    const history = await _client.getHistory({
      callerId: creator.id,
      creatorHeads: creatorHeads.heads,
      peerId
    });

    const {history: eventHashes, truncated} = history;
    if(eventHashes.length === 0) {
      // peer has nothing to share
      return {creator, creatorHeads, history, done: true};
    }

    // check to see what's needed from the peer by diffing with the cache
    // and local storage
    const needed = await _diff({eventHashes, ledgerNode});
    if(needed.length === 0) {
      // we already have what we need from other peers, so no need to gossip
      return {creator, creatorHeads, history, done: true};
    }

    try {
      // removes event hashes from `needed` as they are received
      await _getNeeded({ledgerNode, needed, peerId});
    } catch(e) {
      if(_.get(e, 'details.httpStatusCode') === 404) {
        // peer has nothing to share
        return {creator, creatorHeads, history, done: true};
      }
      // return error if all the needed events were not received
      if(needed.length !== 0) {
        throw new BedrockError(
          'Some events requested from the peer were not received.',
          'DataError', {needed, peerId}, e);
      }
      throw e;
    }
    return {creator, creatorHeads, history, done: !truncated};
  } finally {
    logger.verbose('End gossipWith', {
      duration: Date.now() - startTime
    });
  }
});

// used in server to only get current heads
api.getHeads = callbackify(async ({creatorId}) => {
  const ledgerNodeId = await _voters.getLedgerNodeId(creatorId);
  const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
  const {heads} = await _events.getCreatorHeads({ledgerNode});
  return heads;
});

// TODO: almost fully duplicated in `events.js`, move to redis/cache
// file/module for reuse
async function _diff({eventHashes, ledgerNode}) {
  if(eventHashes.length === 0) {
    return [];
  }
  const ledgerNodeId = ledgerNode.id;
  const manifestId = uuid();
  const eventQueueSetKey = _cacheKey.eventQueueSet(ledgerNodeId);
  const manifestKey = _cacheKey.manifest({ledgerNodeId, manifestId});

  // TODO: this could be implemented as smembers as well and diff the hashes
  // as an array, if the eventQueueSetKey contains a large set, then the
  // existing implementation is good

  // Note: Here we add an entry to redis just to perform a diff...
  // 1. Add `manifestKey` with `eventHashes` array as the value
  // 2. Run `sdiff` to do diff that value with what is in the event queue
  //    for the ledger node (using key `eventQueueSetKey`)
  // 3. Delete the `manifestKey` once we're done running the diff
  // ... and results of `sadd` is in result[0], `sdiff` is in result[1], so
  // we destructure result[1] into `notFound` (i.e. events not in the queue)
  const [, notFound] = await cache.client.multi()
    .sadd(manifestKey, eventHashes)
    .sdiff(manifestKey, eventQueueSetKey)
    .del(manifestKey)
    .exec();
  // ...of the events not found in the event queue (redis), return those that
  // are also not in storage (mongo), i.e. we haven't stored them at all
  const diff = await ledgerNode.storage.events.difference(notFound);
  if(diff.length === 0) {
    return [];
  }

  // the order of eventHashes must be preserved
  const needSet = new Set(diff);
  return eventHashes.filter(h => needSet.has(h));
}

async function _getNeeded({ledgerNode, needed, peerId}) {
  /* Note: Here we connect to the peer and download all needed events. The
  events currently must be added in order (TODO: handle ordering later and
  just put them into the cache as quickly as possible to optimize).

  This means that a queue of concurrency `1` is used to add the events as they
  arrive. If there is any error in the transfer of events from the peer (e.g.
  bad event, broken pipe, whatever), we should continue adding the events we
  did successfully receive and then terminate once the queue completes.

  However, if an error occurs with adding the events (since they must be added
  in order), we should terminate the connection to the peer immediately and
  bail out. */
  let addEventError;
  let peerError;
  let streamDone = false;

  const q = new PQueue({concurrency: 1});

  function _isQueueIdle() {
    // we do not use `onIdle()` as we need this to be synchronous
    return q.size === 0 && q.pending === 0;
  }

  return new Promise((resolve, reject) => {
    // connect to peer and get event stream...
    const stream = _client.getEvents({eventHash: needed, peerId})
      .on('error', err => {
        // an early error prevents anything else from happening,
        // so simply reject
        reject(err);
      })
      .on('response', res => {
        const {body, statusCode} = res;
        if(statusCode !== 200) {
          // set `peerError` because `error` handler will be called and
          // it should not do anything
          peerError = new BedrockError(
            'An error occurred contacting a peer.', 'NetworkError',
            {body, needed, peerId, statusCode});
          stream.destroy();
          // we couldn't have queued any events yet, so reject
          reject(peerError);
        }
      })
      .pipe(JSONStream.parse())
      .on('data', data => {
        // if an add event error previously occurred then we can't queue
        // another event -- and other code will handle destroying stream
        if(addEventError) {
          return;
        }

        if(!(data && data.event)) {
          stream.destroy();
          logger.debug('MALFORMED EVENT', {data});
          peerError = new BedrockError(
            'Malformed event detected during gossip.',
            'AbortError', {data, peerId});
          // if queue is idle then nothing else will trigger reject so we must
          if(_isQueueIdle()) {
            reject(peerError);
          }
          return;
        }

        q.add(async () => {
          // Note: This may execute after the stream is already shutdown
          // (and this is accounted for by checking `streamDone`).
          try {
            const record = await ledgerNode.consensus._events.add(
              {continuity2017: {peer: true},
                event: data.event, ledgerNode, needed});
            const {eventHash} = record.meta;
            _.pull(needed, eventHash);
            if(streamDone && q.size === 0 && q.pending === 1) {
              // this is the last event received
              resolve();
            }
          } catch(e) {
            addEventError = e;
            // the other events can't be processed since this one errored, so
            // clear the queue
            q.clear();
            if(!streamDone) {
              // there is no reason to continue receiving events since they
              // must be added in order and we've hit an error adding this one
              // so destroy the stream
              stream.destroy();
            }
            reject(addEventError);
          }
        });
      })
      .on('end', () => {
        // track that stream is done so we know when to `resolve()` later or
        // that we don't need to destroy it should an add event error occur
        streamDone = true;
        // if an add event error previously occurred then other code will
        // handle destroying the stream
        if(addEventError) {
          return;
        }
        // if queue is idle then we finished without error
        if(_isQueueIdle()) {
          resolve();
        }
      })
      .on('error', err => {
        if(peerError || addEventError) {
          // nothing to do, previous error already occurred
          return;
        }
        peerError = err;
        // if queue is idle then we must reject
        if(_isQueueIdle()) {
          reject(peerError);
        }
      });
  });
}
