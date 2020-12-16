/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _cache = require('../cache');
const _events = require('../events');
const _history = require('../history');
const _peers = require('../peers');
const bedrock = require('bedrock');
const cache = require('bedrock-redis');
const {config} = bedrock;
const {extend: extendBlockchain} = require('./blockchain');
const {merge} = require('./merge');
const logger = require('../logger');
const {runGossipCycle, sendNotification} = require('./gossip');
const {BedrockError} = bedrock.util;
const PeerEventWriter = require('./PeerEventWriter');
const GossipPeerSelector = require('./GossipPeerSelector');

module.exports = class Worker {
  constructor({session, halt = this._halt} = {}) {
    const {ledgerNode} = session;
    this.halt = halt;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.config = config['ledger-consensus-continuity'].writer;
    this.eventsConfig = config['ledger-consensus-continuity'].events;
    this.peerEventWriter = new PeerEventWriter({ledgerNode});
    // FIXME: move `creatorId` discovery inside peer selector
    // this.peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
    this.session = session;
    this.storage = ledgerNode.storage;
  }

  async run({targetCycles = -1} = {}) {
    const {peerEventWriter, ledgerNode, session} = this;
    let {halt} = this;
    const ledgerNodeId = ledgerNode.id;

    logger.verbose('ledger work session job running', {ledgerNodeId});

    // listen for new local operation notifications
    let resume = () => {};
    const subscriber = new cache.Client().client;
    try {
      await subscriber.subscribe(`continuity2017|needsMerge|${ledgerNodeId}`);
      subscriber.on('message', () => resume());
    } catch(e) {
      subscriber.quit();
      logger.verbose(
        'Work session failed, could not subscribe to new pub/sub messages.',
        {session: session.id});
      return;
    }

    try {
      /* This work session is designed with the assumption that any attempt to
      write to the database or the cache will be successful or an error will
      be thrown canceling the work session. This allows for the cache and
      database to be brought into sync at the start of the work session, without
      having to worry about them getting out of sync in the middle of it. It
      also provides a more simple model for reasoning about correctness and
      potential errors. */

      // ensure cache and mongo are in sync
      const creatorId = (await _peers.get({ledgerNodeId})).id;
      await _validateCache({ledgerNode, creatorId});

      // FIXME: move to state on Worker
      const peerSelector = new GossipPeerSelector({creatorId, ledgerNode});
      const {coolDownPeriod} = config['ledger-consensus-continuity'].gossip;
      let needsGossip = false;

      // commit any previously cached events to the database that could
      // not be written before because gossip timed out; this step ensures
      // valid pending events are always written
      if(!halt()) {
        await peerEventWriter.flush();
      }

      // run consensus/gossip/merge pipeline until work session expires
      // or until the pipeline is run at least once per request
      const savedState = {};
      let cycles = 0;
      if(targetCycles > 0) {
        const _halt = halt;
        halt = () => _halt() || cycles >= targetCycles;
      }
      while(!halt()) {
        // 1. extend blockchain until can't anymore
        const {blocks, priorityPeers, witnesses, blockHeight} =
          await extendBlockchain({ledgerNode, savedState, halt});

        // work session expired
        if(halt()) {
          break;
        }

        // if blocks were created, reset `needsGossip`; to be set again by
        // `merge`
        if(blocks > 0) {
          needsGossip = false;
        }

        // 2. run gossip cycle; the gossip cycle runs an internal loop against
        // selections of peers and it will loop:
        //   until >= 1 merge events received, if `needsGossip=true`;
        //   once, if `needsGossip=false`
        const {mergeEventsReceived} = await runGossipCycle({
          worker: this, ledgerNode, priorityPeers, creatorId, peerSelector,
          needsGossip, witnesses, blockHeight, halt
        });

        // work session expired
        if(halt()) {
          break;
        }

        // 3. commit all cached events to mongo
        await peerEventWriter.flush();

        // 4. merge if possible
        const {merged, status: mergeStatus} = await merge({
          ledgerNode, creatorId, priorityPeers, witnesses,
          basisBlockHeight: blockHeight - 1, halt
        });
        // keep track of whether a merge would happen if more peer events were
        // received via gossip
        needsGossip = mergeStatus.needsGossip;

        // determine if peers need to be notified of new events
        let notify;
        if(merged || mergeEventsReceived) {
          await _cache.gossip.notifyFlag({add: true, ledgerNodeId});
          notify = true;
        } else {
          notify = (await _cache.gossip.notifyFlag({ledgerNodeId})) !== null;
        }

        if(notify) {
          // notify peers of new/previous merge event(s)
          try {
            // FIXME: notify more than just the `priorityPeers`
            await sendNotification({creatorId, priorityPeers, peerSelector});
          } catch(e) {
            // just log the error, another attempt will be made on the next
            // cycle
            logger.error(
              'An error occurred while attempting to send merge notification.',
              {error: e});
          }
        }

        // work session expired
        if(halt()) {
          break;
        }

        // if there are no outstanding operations (this includes
        // configurations) need to achieve consensus, then delay for cool down
        // period or until a peer notification or a local operation
        // notification comes in
        if(!mergeStatus.hasOutstandingOperations) {
          await new Promise(resolve => {
            resume = resolve;
            setTimeout(resolve, coolDownPeriod);
          });
          // FIXME: if, after cool down, there is still nothing to do, should
          // we end the work session and let the scheduler take it from there?
        }

        // track pipeline runs
        cycles++;
      }
    } finally {
      // unsubscribe from new operation messages
      subscriber.quit();
      logger.verbose('Work session completed.', {session: session.id});
    }
  }

  // default halt function
  _halt() {
    // expire session early, with 5 second buffer for overruns
    return this.session.timeRemaining() < 5000;
  }

  // /**
  //  * Get event hashes that have no children and are candidates to
  //  * be merged by the local node.
  //  *
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<string[]>} hashes for childless events.
  //  */
  // exports.getChildlessHashes = async ({ledgerNodeId}) => {
  //   const childlessKey = _cacheKey.childless(ledgerNodeId);
  //   const childlessHashes = await cache.client.smembers(childlessKey);
  //   return {childlessHashes};
  // };

  // /**
  //  * Get local event hashes that have no children and are candidates to
  //  * be merged by the local node.
  //  *
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<string[]>} hashes for childless events.
  //  */
  // exports.getLocalChildlessHashes = async ({ledgerNodeId}) => {
  //   const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  //   const localChildlessHashes = await cache.client.smembers(localChildlessKey);
  //   return {localChildlessHashes};
  // };

  // /**
  //  * Get events.
  //  *
  //  * @param eventHash {string|string[]} - The event hash(es) to get.
  //  * @param [includeMeta=false] {Boolean} - Include event meta data.
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<Object[]>} The events.
  //  */
  // exports.getEvents = async ({eventHash, includeMeta = false, ledgerNodeId}) => {
  //   eventHash = [].concat(eventHash);
  //   const fields = ['event'];
  //   if(includeMeta) {
  //     fields.push('meta');
  //   }
  //   const eventKeys = eventHash.map(eventHash =>
  //     _cacheKey.eventGossip({eventHash, ledgerNodeId}));
  //   const txn = cache.client.multi();
  //   for(const key of eventKeys) {
  //     txn.hmget(key, ...fields);
  //   }

  //   const result = await txn.exec();
  //   return result.map(r => {
  //     if(includeMeta) {
  //       const [event, meta] = r;
  //       return {event, meta};
  //     }
  //     const [event] = r;
  //     return {event};
  //   });
  // };

  // /**
  //  * Get the current merge status information. This status information includes:
  //  * - the hashes of any peer childless events (targets for merging ... to become
  //  *   parents of the next potential merge event).
  //  * - the hashes of any local childless events (targets for merging ... to
  //  *   become parents of the next potential merge event).
  //  *
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  *
  //  * @returns {Promise<Object>} The merge status info.
  //  */
  // exports.getMergeStatus = async ({ledgerNodeId}) => {
  //   // see if there are any childless events to be merged
  //   const childlessKey = _cacheKey.childless(ledgerNodeId);
  //   const localChildlessKey = _cacheKey.localChildless(ledgerNodeId);
  //   const [
  //     peerChildlessHashes, localChildlessHashes
  //   ] = await cache.client.multi()
  //     .sdiff(childlessKey, localChildlessKey)
  //     .smembers(localChildlessKey)
  //     .exec();
  //   // FIXME: enabling the cache to report not just what is childless but
  //   // what is mergeable (especially if we pass in `witnesses`) would speed
  //   // up processing
  //   return {
  //     peerChildlessHashes,
  //     localChildlessHashes
  //   };
  // };

  // /**
  //  * Store an event and meta data for gossip purposes.
  //  *
  //  * @param event {Object} - The event.
  //  * @param eventHash {string} - The event hash.
  //  * @param [expire=600] {Number} - Expire the event after the specified ms.
  //  * @param ledgerNodeId {string} - The ID of the ledger node.
  //  * @param meta {Object} - The event meta data.
  //  *
  //  * @returns {Promise} resolves once the operation completes.
  //  */
  // exports.setEventGossip = async (
  //   {event, eventHash, expire = 600, ledgerNodeId, meta}) => {
  //   const eventKey = _cacheKey.eventGossip({eventHash, ledgerNodeId});
  //   const eventString = JSON.stringify({event});
  //   const metaString = JSON.stringify({meta});
  //   return cache.client.multi()
  //     .hmset(eventKey, 'event', eventString, 'meta', metaString)
  //     .expire(eventKey, expire)
  //     .exec();
  // };
};

// FIXME: remove me
async function _validateCache({ledgerNode, creatorId}) {
  /* Note: Ensure that childless events cache is proper, a previous work
  session may have terminated and failed to update the cache; this cache
  absolutely MUST NOT be corrupt in order for the work session operation
  to function properly, a corrupt cache here may result in loss of
  operations/regular events or invalidation as a properly operating peer.
  It should also be noted that the local regular event count in the cache
  may not be properly synced when this happens, but that key is only used
  for statistics gathering purposes and has a short expiration anyway. */
  await _cache.prime.primeChildlessEvents({ledgerNode});

  // ensure the cache head for this ledger node is in sync w/database
  const [cacheHead, mongoHead] = await Promise.all([
    _history.getHead({creatorId, ledgerNode}),
    _history.getHead({creatorId, ledgerNode, useCache: false})
  ]);
  if(_.isEqual(cacheHead, mongoHead)) {
    // success
    return;
  }
  // this should never happen and requires intervention to determine if
  // it can be repaired
  if((mongoHead.generation - cacheHead.generation) !== 1) {
    const ledgerNodeId = ledgerNode.id;
    throw new BedrockError(
      'Critical error: The cache is behind by more than one merge event.',
      'InvalidStateError',
      {cacheHead, mongoHead, ledgerNodeId});
  }
  const {eventHash} = mongoHead;
  await _events.repairCache({eventHash, ledgerNode});
}
