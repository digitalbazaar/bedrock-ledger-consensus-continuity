/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const {config, util: {BedrockError}} = bedrock;
const database = require('bedrock-mongodb');
const logger = require('./logger');
const multicodec = require('multicodec');
const multibase = require('multibase');
const {Ed25519VerificationKey2020} =
  require('@digitalbazaar/ed25519-verification-key-2020');
const {default: {LruMemoize}} = require('@digitalbazaar/lru-memoize');
const URL = require('url');

require('./config');

let LOCAL_PEER_ID_CACHE;
let LOCAL_PEER_KEY_PAIR_CACHE;
const LOCAL_PEER_KEY_PAIR_CACHE_MAX_AGE = 5000;
let _pruneCacheTimer = null;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock.init', () => {
  // FIXME: make size configurable
  // create bi-directional LRU cache for mapping ledger node ID <=> peer ID
  LOCAL_PEER_ID_CACHE = new LruMemoize({max: 1000});

  // create short-lived cache for keys; it can be small as it only needs to
  // have space for a single key pair during a work session
  LOCAL_PEER_KEY_PAIR_CACHE = new LruMemoize({
    max: 1,
    maxAge: LOCAL_PEER_KEY_PAIR_CACHE_MAX_AGE,
    updateAgeOnGet: true
  });
});

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await database.openCollections(['continuity2017_local_peer']);
  await database.createIndexes([{
    collection: 'continuity2017_local_peer',
    fields: {'localPeer.peerId': 1},
    options: {unique: true, background: false}
  }, {
    collection: 'continuity2017_local_peer',
    fields: {'localPeer.ledgerNodeId': 1},
    options: {unique: true, background: false}
  }]);
});

api.generate = async ({ledgerNodeId} = {}) => {
  // try to generate the local peer...
  let peerId;
  try {
    ({localPeer: {peerId}} = await _generateLocalPeer({ledgerNodeId}));
  } catch(e) {
    if(e.name !== 'DuplicateError') {
      throw e;
    }

    // peer ID already created, return it
    peerId = await api.getPeerId({ledgerNodeId});
  }

  return {peerId};
};

/**
 * Gets the ID for the local peer for the given ledger node ID. If no such peer
 * exists, an exception will be thrown.
 *
 * @param {string} ledgerNodeId - The ID of the ledger node.
 *
 * @return a Promise that resolves to the peer ID.
 */
api.getPeerId = async ({ledgerNodeId}) => {
  return LOCAL_PEER_ID_CACHE.memoize({
    key: ledgerNodeId,
    fn: async () => _getPeerId({ledgerNodeId})
  });
};

/**
 * Gets the key pair for the local peer for the given ledger node ID. If no
 * such peer exists, an exception will be thrown.
 *
 * @param {string} ledgerNodeId - The ID of the ledger node.
 *
 * @return a Promise that resolves to the local peer information including its
 *   key pair.
 */
api.getKeyPair = async ({ledgerNodeId}) => {
  const keyPair = await LOCAL_PEER_KEY_PAIR_CACHE.memoize({
    key: ledgerNodeId,
    fn: async () => _getKeyPair({ledgerNodeId})
  });

  // schedule cache pruning if not already scheduled
  if(!_pruneCacheTimer) {
    _pruneCacheTimer = setTimeout(
      _pruneCache, LOCAL_PEER_KEY_PAIR_CACHE_MAX_AGE);
  }

  return keyPair;
};

api.getLedgerNodeId = async ({peerId}) => {
  return LOCAL_PEER_ID_CACHE.memoize({
    key: peerId,
    fn: async () => _getLedgerNodeId({peerId})
  });
};

/**
 * Gets a public key from a peer ID.
 *
 * @param peerId the ID to get the public key from.
 *
 * @return the base58-encoded public key.
 */
api.getPublicKeyFromId = ({peerId}) => {
  // FIXME: this is to be replaced by `did:key`
  const parsed = URL.parse(peerId);
  const last = parsed.pathname.split('/').pop();

  // the public key is expected to be a multibase encoded multicodec value
  const mbPubkey = last;
  const mcPubkeyBytes = multibase.decode(mbPubkey);
  const mcType = multicodec.getCodec(mcPubkeyBytes);
  if(mcType !== 'ed25519-pub') {
    throw new BedrockError(
      'Peer ID is not a multiformat-encoded ed25519 public key.',
      'EncodingError', {peerId});
  }

  return mbPubkey;
};

// direct storage interface for local peer information
api.storage = {};

/**
 * Adds a new local peer. Each individual ledger node can represent a "peer"
 * on the network associated with the ledger. It needs a peer ID, a
 * cryptographic key pair, and a ledger agent. The peer ID includes the
 * public portion of the cryptographic key pair so the public key does not
 * need to be stored separately. The ledger agent URL is not stored in the
 * database here, but computed from the configuration using the ledger
 * agent ID.
 *
 * TODO: Consider adding an option to use a capability agent and WebKMS here
 * to generate the cryptographic key pair and peer ID so that an additional
 * layer of key security is available.
 *
 * @param localPeer the ledger node's peer information:
 *   peerId - The ID of the peer.
 *   ledgerNodeId - The ID of the ledger node.
 *   privateKeyMultibase - The private key material.
 *
 * @return a Promise that resolves to the peer record.
 */
api.storage.add = async ({localPeer}) => {
  const {peerId, ledgerNodeId} = localPeer;
  logger.verbose('adding local peer', {peerId, ledgerNodeId});

  // create the record
  const now = Date.now();
  const record = {
    meta: {
      created: now,
      updated: now
    },
    localPeer
  };
  const collection = database.collections.continuity2017_local_peer;

  try {
    return (await collection.insertOne(record)).ops[0];
  } catch(e) {
    if(database.isDuplicateError(e)) {
      throw new BedrockError(
        'A local peer already exists for the given ledger node.',
        'DuplicateError', {ledgerNodeId}, e);
    }
    throw e;
  }
};

/**
 * Gets the full storage record for the local peer for the given ledger node ID
 * or peer ID.
 *
 * @param options the options to use:
 *   [ledgerNodeId] - The ID of the ledger node.
 *   [peerId] - The ID of the peer.
 *   [keyPair=false] - `true` to include the peer's key pair.
 *
 * @return a Promise that resolves to the record.
 */
api.storage.get = async ({ledgerNodeId, peerId, keyPair = false}) => {
  if(!(ledgerNodeId || peerId)) {
    throw new Error('"ledgerNodeId" or "peerId" must be given.');
  }

  const query = {'meta.deleted': {$exists: false}};
  if(ledgerNodeId) {
    query['localPeer.ledgerNodeId'] = ledgerNodeId;
  } else {
    query['localPeer.peerId'] = peerId;
  }

  const collection = database.collections.continuity2017_local_peer;
  const projection = {
    _id: 0,
    'localPeer.peerId': 1,
    'localPeer.ledgerNodeId': 1
  };
  if(keyPair) {
    projection['localPeer.privateKeyMultibase'] = 1;
  }
  const record = await collection.findOne(query, {projection});
  if(record) {
    return record;
  }
  const details = {
    httpStatusCode: 404,
    public: true
  };
  if(ledgerNodeId) {
    details.ledgerNodeId = ledgerNodeId;
  } else {
    details.peerId = peerId;
  }
  throw new BedrockError(
    'Local peer information not found.',
    'NotFoundError', details);
};

async function _generateLocalPeer({ledgerNodeId}) {
  const kp = await Ed25519VerificationKey2020.generate();
  const peerId = config.server.baseUri +
    '/consensus/continuity2017/peers/' + encodeURIComponent(kp.fingerprint());
  const localPeer = {
    peerId,
    ledgerNodeId,
    privateKeyMultibase: kp.privateKeyMultibase
  };
  return api.storage.add({localPeer});
}

async function _getPeerId({ledgerNodeId}) {
  const record = await api.storage.get({ledgerNodeId});
  return record.localPeer.peerId;
}

async function _getLedgerNodeId({peerId}) {
  const record = await api.storage.get({peerId});
  return record.localPeer.ledgerNodeId;
}

async function _getKeyPair({peerId, ledgerNodeId}) {
  const record = await api.storage.get({ledgerNodeId, peerId, keyPair: true});
  const localPeer = {
    peerId: record.localPeer.peerId,
    ledgerNodeId: record.localPeer.ledgerNodeId
  };
  localPeer.keyPair = {
    privateKeyMultibase: record.localPeer.privateKeyMultibase,
    publicKeyMultibase: api.getPublicKeyFromId({peerId: localPeer.peerId})
  };

  return localPeer;
}

function _pruneCache() {
  LOCAL_PEER_KEY_PAIR_CACHE.cache.prune();
  if(LOCAL_PEER_KEY_PAIR_CACHE.cache.length === 0) {
    // cache is empty, do not schedule pruning
    _pruneCacheTimer = null;
  } else {
    // schedule another run
    _pruneCacheTimer = setTimeout(
      _pruneCache, LOCAL_PEER_KEY_PAIR_CACHE_MAX_AGE);
  }
}
