/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cacheKey = require('./cache-key');
const bedrock = require('bedrock');
const brDidClient = require('bedrock-did-client');
const bs58 = require('bs58');
const cache = require('bedrock-redis');
const {callbackify, BedrockError} = bedrock.util;
const {config} = bedrock;
const chloride = require('chloride');
const jsigs = require('jsonld-signatures')();
const multicodec = require('multicodec');
const multibase = require('multibase');
const pRetry = require('p-retry');

jsigs.use('jsonld', brDidClient.jsonld);

const storage = require('./storage');

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
  await pRetry(async () => {
    try {
      voter = await storage.voters.get({ledgerNodeId, privateKey, publicKey});
    } catch(e) {
      if(_createKeyErrorFilter(e)) {
        // error indicates a retry may pass, throw it to try again
        throw e;
      }

      if(e.name !== 'NotFoundError') {
        // error is not recoverable, abort retrying
        throw new pRetry.AbortError(e.message);
      }

      // voter not found, try to create it...

      // lock to do key generation
      const lockKey = `keygenerationlock|${ledgerNodeId}`;
      const result = await cache.client.setnx(lockKey, '');
      if(result === 0) {
        // wait for about the length of time it takes to generate a key
        // and then throw to try again
        await _wait(KEY_GENERATION_TIME);
        throw new BedrockError(
          'Key generation in process. Try again.', 'LockError');
      }

      // lock acquired, create voter
      voter = await _createVoter(ledgerNodeId);

      // release lock
      await cache.client.del(lockKey);
    }
  }, {retries: 100});

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

  const {ledgerNodeId} = await storage.voters.get({voterId});

  // update cache
  await cache.client.set(ledgerNodeKey, ledgerNodeId);

  return ledgerNodeId;
});

api.sign = callbackify(async ({doc, ledgerNodeId}) => {
  const creator = await api.get({ledgerNodeId, privateKey: true});
  return jsigs.sign(doc, {
    algorithm: 'Ed25519Signature2018',
    privateKeyBase58: creator.publicKey.privateKey.privateKeyBase58,
    creator: creator.publicKey.id
  });
});

// TODO: need a method for voter signature verification whereby the public
// key for a voter will be retrieved from the blockchain and verified against
// its voter ID (and cached for future use?)

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
  const result = await storage.voters.add(voter);
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

function _createKeyErrorFilter(err) {
  return ['DuplicateError', 'LockError'].includes(err.name);
}

function _wait(millis) {
  return new Promise(resolve => {
    setTimeout(() => resolve(), millis);
  });
}
