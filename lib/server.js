/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const _events = require('./events');
const _peerEvents = require('./peerEvents');
const _history = require('./history');
const _localPeers = require('./localPeers');
const {asyncHandler} = require('bedrock-express');
const bedrock = require('bedrock');
const bodyParser = require('body-parser');
const brLedgerNode = require('bedrock-ledger-node');
const brRest = require('bedrock-rest');
const {config} = bedrock;
const {callbackify} = require('util');
const {validate} = require('bedrock-validation');

require('bedrock-permission');

require('./config');

let _currentEventValidationJobs = 0;
let _currentAddNewPeerJobs = 0;
let MAX_ADD_NEW_PEER_JOBS;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-express.configure.bodyParser', app => {
  app.use(bodyParser.json({limit: '1mb', type: ['json', '+json']}));
});

bedrock.events.on('bedrock-express.configure.routes', app => {
  const cfg = config['ledger-consensus-continuity'];
  const {routes, gossip: {eventsValidation, notification}} = cfg;

  const MAX_EVENT_VALIDATION_JOBS = eventsValidation.concurrency;
  MAX_ADD_NEW_PEER_JOBS = notification.addNewPeerConcurrency;

  // get events
  app.post(
    routes.eventsQuery, brRest.when.prefers.ld,
    validate('continuity-server.getEvents'), asyncHandler(async (req, res) => {
      const localPeerId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const ledgerNodeId = await _localPeers.getLedgerNodeId(
        {peerId: localPeerId});
      const {eventHash} = req.body;
      const events = await _events.getEventsForGossip(
        {eventHashes: eventHash, ledgerNodeId});
      for(const event of events) {
        res.write(`${event}\n`);
      }
      res.end();
    }));

  // validate event
  app.post(
    routes.eventsValidation, brRest.when.prefers.ld,
    asyncHandler(async (req, res) => {
      if(_currentEventValidationJobs >= MAX_EVENT_VALIDATION_JOBS) {
        res.status(503).end();
        return;
      }

      _currentEventValidationJobs++;

      try {
        const {event, ledgerNodeId, session} = req.body;

        const validatedEvent = await _peerEvents.validateEvent({
          event,
          ledgerNodeId,
          session
        });

        res.json(validatedEvent);
      } finally {
        _currentEventValidationJobs--;
      }
    }));

  app.post(
    routes.gossip, brRest.when.prefers.ld,
    validate('continuity-server.gossip'), brRest.linkedDataHandler({
      // eslint-disable-next-line
      get: callbackify(async (req, res) => {
        const localPeerId = config.server.baseUri +
          '/consensus/continuity2017/voters/' + req.params.voterId;
        const {body: remoteInfo} = req;
        // `basisBlockHeight` is the last block that has reached consensus
        //   on the client
        // `localEventNumber` is optional, only present if the client knows
        //   the next `localEventNumber` to request from the server
        // `peerHeads` all of the non-consensus peer heads the client knows
        //   referenced by merge event hash
        // `peerId` the client's peer ID
        const ledgerNodeId = await _localPeers.getLedgerNodeId(
          {peerId: localPeerId});
        const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
        // clear the notifyFlag to indicate that a gossip session has occurred
        await _cache.gossip.notifyFlag({ledgerNodeId, remove: true});

        // FIXME: we do not presently authenticate the remote peer -- is there
        // any need?

        // return a partition of the DAG history appropriate for the request
        return _history.partition({ledgerNode, remoteInfo});
      })
    }));

  app.post(
    routes.notify, brRest.when.prefers.ld,
    validate('continuity-server.notification'),
    asyncHandler(async (req, res) => {
      const {peer} = req.body;
      const localPeerId = config.server.baseUri +
        '/consensus/continuity2017/voters/' + req.params.voterId;
      const ledgerNodeId = await _localPeers.getLedgerNodeId(
        {peerId: localPeerId});
      const ledgerNode = await brLedgerNode.get(null, ledgerNodeId);
      // FIXME: we need to authenticate `peerId`; perhaps http-signatures
      // should be used on notification endpoints as the simplest method;
      // however, that would not cover indirect sharing of peer URLs elsewhere
      // ... though a signature on shared URLs may end up not being a
      // requirement w/attempting to gossip with the peer may be sufficient
      // in that scenario since it has to happen anyway
      await _addNotification({ledgerNode, remotePeer: peer});
      // FIXME: remove redis usage
      await _cache.gossip.addNotification({ledgerNodeId, peerId: peer.id});
      res.status(204).end();
    }));
});

async function _addNotification({ledgerNode, remotePeer}) {
  const {id, url} = remotePeer;
  try {
    // FIXME: consider passing `backoffUntil: 0` here to clear delay --
    // but if that is done, then we need to ensure consecutive rejections
    // are tracked and *not* cleared so an attacker that clears their delay
    // will just get rejected must faster and potentially moved to a longer
    // block list
    await ledgerNode.peers.updateLastPushAt({id, url});
    return;
  } catch(e) {
    if(e.name !== 'NotFoundError') {
      throw e;
    }
  }

  /* Note: It is possible that the `peerCount` below could increase beyond the
  cap since we are not adding the peer atomically with the collection size
  check. The risk is that an attacker sends notifications for N many new
  peers at once and they all get added. This risk is mitigated by requiring
  all processes that could add a server this way to be limited by a
  configurable max concurrency factor. The total number of processes multiplied
  by the concurrency indicates the maximum overflow size. When overflowed, any
  unreliable peers will eventually cycle out -- and they should not
  significantly affect existing reliable peers. */

  // if too many other new peers are being added, do not add another one
  if(_currentAddNewPeerJobs >= MAX_ADD_NEW_PEER_JOBS) {
    return;
  }

  _currentAddNewPeerJobs++;

  try {
    // FIXME: make maximum peer count configurable
    /* Note: A maximum of 100 peers was chosen based on it being the lowest
    power of ten that is acceptable (powers of ten being human-friendly for
    analysis). There is an assumption that 10-20 will be recommended peers
    (enforced elsewhere) which means some space must be reserved for peers that
    are less trusted or reliable. Peers in this other category will be cycled
    in and out of a ledger node's peer collection based on how reliable and
    productive they are over time. Those that are not reliable and productive
    will be removed enabling space for more peers to try their hand.

    If the number of available slots is too large, then cycling through the
    peers will become challenging and enable more unreliable/unproductive peers
    to get into the collection. */
    const peerCount = await ledgerNode.peers.count();
    if(peerCount >= 100) {
      return;
    }

    // add peer since it was not found and there is space to add it
    const peer = {
      ...remotePeer,
      status: {lastPushAt: Date.now()}
    };
    try {
      await ledgerNode.peers.add({peer});
    } catch(e) {
      if(e.name !== 'DuplicateError') {
        throw e;
      }
    }

  } finally {
    _currentAddNewPeerJobs--;
  }
}
