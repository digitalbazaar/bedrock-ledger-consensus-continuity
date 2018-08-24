/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const {callbackify, BedrockError} = bedrock.util;
const database = require('bedrock-mongodb');
const {promisify} = require('util');

// module API
const api = {};
module.exports = api;

// get logger
const logger = bedrock.loggers.get('app');

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
api.add = callbackify(async (voter) => {
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
api.get = callbackify(async (
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
  if(publicKey) {
    // FIXME: get `publicKeyBase58` from voter ID, not database
    _.assign(projection, {
      'voter.publicKey.id': 1,
      'voter.publicKey.publicKeyBase58': 1
    });
  }
  if(privateKey) {
    // the private key and the associated `id` resides in `publicKey`
    _.assign(projection, {
      'voter.publicKey.id': 1,
      'voter.publicKey.privateKey.privateKeyBase58': 1
    });
  }
  const record = await collection.findOne(query, projection);
  if(!record) {
    const details = {
      httpStatusCode: 400,
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

  return record.voter;
});
