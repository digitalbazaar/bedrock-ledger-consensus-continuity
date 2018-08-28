/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _cache = require('./cache');
const bedrock = require('bedrock');
const bs58 = require('bs58');
const {callbackify, BedrockError} = bedrock.util;
const {config} = bedrock;
const chloride = require('chloride');
const database = require('bedrock-mongodb');
const jsigs = require('jsonld-signatures')();
const logger = require('./logger');
const multicodec = require('multicodec');
const multibase = require('multibase');
const {promisify} = require('util');
const URL = require('url');

jsigs.use('jsonld', bedrock.jsonld);

require('./config');

// 50 milliseconds estimated max key generation time
const KEY_GENERATION_TIME = 50;

// module API
const api = {};
module.exports = api;

bedrock.events.on('bedrock-mongodb.ready', async () => {
  await promisify(database.openCollections)(['continuity2017_voter']);
  await promisify(database.createIndexes)([{
    collection: 'continuity2017_voter',
    fields: {'voter.id': 1},
    options: {unique: true, background: false}
  }, {
    collection: 'continuity2017_voter',
    fields: {'voter.ledgerNodeId': 1},
    options: {unique: true, background: false}
  }]);
});

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
  // keys are not cached
  if(!(privateKey || publicKey)) {
    const voterId = await _cache.voters.get({ledgerNodeId});
    if(voterId !== null) {
      // cache hit
      return {id: voterId};
    }
  }

  // simultaneous calls to this API can lead to multiple workers attempting
  // to create keys causing _createVoter to produce a DuplicateError
  let voter;
  let first = true;
  while(true) {
    try {
      voter = await api.storage.get({ledgerNodeId, privateKey, publicKey});
      break;
    } catch(e) {
      if(e.name !== 'NotFoundError') {
        // error is not recoverable, abort retrying
        throw e;
      }
    }

    // voter not found...

    // if this is the first attempt, wait a random period of time anywhere
    // from `0 - KEY_GEN_TIME * 2` and then loop; waiting this period time
    // is an attempt to reduce unnecessary duplicate key generations by
    // multiple processes, hopefully fewer processes waste effort, but either
    // way it's not a big deal -- and only happens on ledger node init
    if(first) {
      first = false;
      await _wait(Math.floor(Math.random() * ((KEY_GENERATION_TIME * 2) + 1)));
      continue;
    }

    // try to create the voter...
    try {
      await _createVoter(ledgerNodeId);
    } catch(e) {
      if(e.name === 'DuplicateError') {
        // duplicate error indicates another process created the voter,
        // loop to get it
        continue;
      }
      throw e;
    }
  }

  // update cache
  await _cache.voters.add({voterId: voter.id, ledgerNodeId});

  return voter;
});

api.getLedgerNodeId = callbackify(async (voterId) => {
  // check cache first
  const result = await _cache.voters.getLedgerNodeId({voterId});
  if(result !== null) {
    // cache hit
    return result;
  }

  // get from storage then update cache
  const {ledgerNodeId} = await api.storage.get({voterId});
  await _cache.voters.setLedgerNodeId({voterId, ledgerNodeId});

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

/**
 * Get the IDs of all peers known to a ledger node that have participated in a
 * ledger by submitting merge events that have achieved consensus. If a limit
 * is specified, the most recent peers will be returned until the limit is
 * reached.
 *
 * @param creatorId the ID of the node to get the peers for.
 * @param ledgerNode the ledger node to check for peers with.
 * @param [limit] a limit on the number of peers to get.
 *
 * @return a Promise that resolves to the IDs of the peers.
 */
api.getPeerIds = callbackify(async ({creatorId, ledgerNode, limit}) => {
  const {collection} = ledgerNode.storage.events;
  // FIXME: ideally only peers that have events that have achieved
  // consensus would be included but this requires updating gossip-agent
  const query = {
    'meta.continuity2017.type': 'm',
    // FIXME: seems like this should be excluded but it causes gossip to
    // break... why?
    // exclude creatorId
    //'meta.continuity2017.creator': {$ne: creatorId},
  };
  // FIXME: implement `limit` (and would need to sort by most recent events
  // as well)
  // covered query under `continuity2` index
  return collection.distinct('meta.continuity2017.creator', query);
});

api.storage = {};

/**
 * Adds a new voter. Each individual ledger node can be a "voter" and
 * needs voter information including an identifier, a cryptographic key,
 * and a ledger agent. The ledger agent URL is not stored in the database,
 * but computed from the configuration using the ledger agent ID.
 *
 * FIXME: move private key storage to HSM/SSM.
 *
 * @param voter the ledger node's voter information:
 *          id the ID of the voter.
 *          ledgerNodeId the ID of the ledger node.
 *          publicKey the voter's public key.
 *          privateKey the voter's private key.
 *
 * @return a Promise that resolves to the voter record.
 */
api.storage.add = callbackify(async (voter) => {
  logger.verbose('adding voter', voter.id);

  // create the record
  const now = Date.now();
  const record = {
    meta: {
      created: now,
      updated: now
    },
    voter
  };
  const collection = database.collections.continuity2017_voter;

  try {
    return (await collection.insert(record, database.writeOptions)).ops[0];
  } catch(e) {
    if(database.isDuplicateError(e)) {
      // TODO: how to handle key rotation?
      throw new BedrockError(
        'The voter information for the given ledger node already exists.',
        'DuplicateError', {ledgerNodeId: voter.ledgerNodeId}, e);
    }
    throw e;
  }

  return record;
});

/**
 * Gets the voter information for the given ledger node ID or voter ID.
 *
 * @param options the options to use:
 *          [ledgerNodeId] the ID of the ledger node.
 *          [voterId] the ID of the voter.
 *
 * @return a Promise that resolves to the voter.
 */
api.storage.get = callbackify(async (
  {ledgerNodeId, privateKey = false, publicKey = false, voterId}) => {
  if(!(ledgerNodeId || voterId)) {
    throw new Error('"ledgerNodeId" or "voterId" must be set.');
  }

  const query = {'meta.deleted': {$exists: false}};
  if(ledgerNodeId) {
    query['voter.ledgerNodeId'] = ledgerNodeId;
  } else {
    query['voter.id'] = voterId;
  }

  const collection = database.collections.continuity2017_voter;
  const projection = {
    _id: 0,
    'voter.id': 1,
    'voter.ledgerNodeId': 1,
  };
  if(privateKey) {
    projection['voter.privateKeyBase58'] = 1;
  }
  const record = await collection.findOne(query, projection);
  if(!record) {
    const details = {
      httpStatusCode: 404,
      public: true
    };
    if(ledgerNodeId) {
      details.ledgerNodeId = ledgerNodeId;
    } else {
      details.voterId = voterId;
    }

    throw new BedrockError(
      'Voter information not found for the given ledger node.',
      'NotFoundError', details);
  }
  const voter = {
    id: record.voter.id,
    ledgerNodeId: record.voter.ledgerNodeId
  };
  if(publicKey || privateKey) {
    voter.publicKey = {id: voter.id};
    if(publicKey) {
      voter.publicKey.publicKeyBase58 = api.getPublicKeyFromId(
        {voterId: voter.id});
    }
    if(privateKey) {
      voter.publicKey.privateKey = {
        privateKeyBase58: record.voter.privateKeyBase58
      };
    }
  }
  return voter;
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
    ledgerNodeId,
    privateKeyBase58: kp.privateKeyBase58
  };
  const result = await api.storage.add(voter);
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
