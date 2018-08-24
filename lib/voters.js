/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const bedrock = require('bedrock');
const bs58 = require('bs58');
const cache = require('bedrock-redis');
const {callbackify, BedrockError} = bedrock.util;
const {config} = bedrock;
const chloride = require('chloride');
const jsigs = require('jsonld-signatures')();
const multicodec = require('multicodec');
const multibase = require('multibase');
const URL = require('url');

jsigs.use('jsonld', bedrock.jsonld);

const _storage = require('./storage-voters');

require('./config');

// 50 milliseconds estimated max key generation time
const KEY_GENERATION_TIME = 50;

// module API
const api = {};
module.exports = api;

/**
 * Get the voter used for the given ledger node ID. If no such voter exists,
 * it will be created.
 *
 * Creating the voter on demand as part of this `get` method is done
 * intentionally, so as to avoid a costly key generation operation in a
 * `create` call that will fail every time after the first call but not
 * until after time was spent generating the key and checking the database
 * for a duplicate.
 *
 * @param ledgerNodeId the ID of the ledger node.
 *
 * @return a Promise that resolves to the voter.
 */
api.get = callbackify(async (
  {ledgerNodeId, privateKey = false, publicKey = false}) => {
  const voterKey = _cacheKey.voter(ledgerNodeId);

  // keys are not cached
  if(!(privateKey || publicKey)) {
    const result = await cache.client.get(voterKey);
    if(result !== null) {
      // cache hit
      return {id: result};
    }
  }

  // simultaneous calls to this API can lead to multiple workers attempting
  // to create keys causing _createVoter to produce a DuplicateError
  let voter;
  let tries = 0;
  while(tries < 100) {
    try {
      voter = await _storage.get({ledgerNodeId, privateKey, publicKey});
      break;
    } catch(e) {
      if(e.name !== 'NotFoundError') {
        // error is not recoverable, abort retrying
        throw e;
      }
    }

    // voter not found, try to create it...

    // FIXME: is this lock really necessary? At worst we generate an extra
    // key that we throw out when we get a duplicate error ... we could
    // simplify this implementation by just waiting a random amount of time
    // the first time ... and then creating the key... if we really even
    // need to do that; this should not be a big deal

    // lock to do key generation
    const lockKey = `keygenerationlock|${ledgerNodeId}`;
    const result = await cache.client.setnx(lockKey, '');
    if(result === 0) {
      // failed to get lock; wait for about the length of time it takes to
      // generate a key and then throw to try again
      await _wait(KEY_GENERATION_TIME);
      tries++;
      continue;
    }

    // lock acquired, create voter
    try {
      voter = await _createVoter(ledgerNodeId);
    } catch(e) {
      if(e.name === 'DuplicateError') {
        // duplicate error indicates another process created the voter,
        // loop to get it
        continue;
      }
      throw e;
    } finally {
      // release lock
      await cache.client.del(lockKey);
    }
  }

  // update cache
  await cache.client.set(voterKey, voter.id);

  return voter;
});

api.getLedgerNodeId = callbackify(async (voterId) => {
  const ledgerNodeKey = _cacheKey.ledgerNode(voterId);
  // check cache first
  const result = await cache.client.get(ledgerNodeKey);
  if(result !== null) {
    // cache hit
    return result;
  }

  const {ledgerNodeId} = await _storage.get({voterId});

  // update cache
  await cache.client.set(ledgerNodeKey, ledgerNodeId);

  return ledgerNodeId;
});

api.sign = callbackify(async ({event, ledgerNodeId}) => {
  const creator = await api.get({ledgerNodeId, privateKey: true});
  return jsigs.sign(event, {
    algorithm: 'Ed25519Signature2018',
    privateKeyBase58: creator.publicKey.privateKey.privateKeyBase58,
    creator: creator.publicKey.id
  });
});

/**
 * Gets a public key from a voter ID.
 *
 * @param voterId the ID to get the public key from.
 *
 * @return the base58-encoded public key.
 */
api.getPublicKeyFromId = ({voterId}) => {
  // TODO: should we do this check (or an even more comprehensive one
  //   or be strict like we are now?)
  // remove trailing slash
  // if(id.endsWith('/')) {
  //   id = id.substr(0, id.length - 1);
  // }
  const parsed = URL.parse(voterId);
  const last = parsed.pathname.split('/').pop();

  // the public key is expected to be a multibase encoded multicodec value
  const mbPubkey = last;
  const mcPubkeyBytes = multibase.decode(mbPubkey);
  const mcType = multicodec.getCodec(mcPubkeyBytes);
  if(mcType !== 'ed25519-pub') {
    throw new BedrockError(
      'Voter ID is not a multiformats encoded ed25519 public key.',
      'EncodingError', {voterId});
  }
  const pubkeyBytes = multicodec.rmPrefix(mcPubkeyBytes);
  const b58Pubkey = bs58.encode(pubkeyBytes);

  return b58Pubkey;
};

api.getPeerIds = callbackify(async ({creatorId, ledgerNode, limit}) => {
  const {collection} = ledgerNode.storage.events;
  // FIXME: ideally only peers that have events that have achieved
  // consensus would be included but this requires updating gossip-agent
  const query = {
    $or: [{
      'meta.continuity2017.type': 'm',
      'meta.consensus': {$exists: true},
      // exclude creatorId
      'meta.creator': {$ne: creatorId}
    }, {
      'meta.continuity2017.type': 'm',
      'meta.consensus': {$exists: false},
      // exclude creatorId
      'meta.creator': {$ne: creatorId}
    }]
  };
  // FIXME: implement `limit` (and would need to sort by most recent events
  // as well)
  // covered query under `continuity2` index
  return collection.distinct('meta.continuity2017.creator', query);
});

async function _createVoter(ledgerNodeId) {
  const kp = await _generateEd25519Key();

  // use multicodec to encode the voter ID
  const publicKeyBytes = bs58.decode(kp.publicKeyBase58);
  const mcPubkey = multicodec.addPrefix('ed25519-pub', publicKeyBytes);
  const mbPubkey = multibase.encode('base58btc', mcPubkey).toString();
  const voterId = config.server.baseUri +
    '/consensus/continuity2017/voters/' + mbPubkey;
  const voter = {
    id: voterId,
    ledgerNodeId: ledgerNodeId,
    publicKey: {
      // same ID as voter (i.e. voter *is* a key)
      id: voterId,
      publicKeyBase58: kp.publicKeyBase58,
      privateKey: {
        privateKeyBase58: kp.privateKeyBase58
      }
    }
  };
  const result = await _storage.add(voter);
  return result.voter;
}

async function _generateEd25519Key() {
  // generate keypair for ledger node
  const keypair = chloride.crypto_sign_keypair();
  return {
    publicKeyBase58: bs58.encode(keypair.publicKey),
    privateKeyBase58: bs58.encode(keypair.secretKey)
  };
}

function _wait(millis) {
  return new Promise(resolve => {
    setTimeout(() => resolve(), millis);
  });
}
