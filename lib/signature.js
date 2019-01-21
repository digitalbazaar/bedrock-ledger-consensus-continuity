/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _voters = require('./voters');
const bedrock = require('bedrock');
const {config: {constants}, util: {BedrockError}} = bedrock;
const jsigs = require('jsonld-signatures')();
const logger = require('./logger');

jsigs.use('jsonld', bedrock.jsonld);

const api = {};
module.exports = api;

api.sign = async ({event, ledgerNodeId}) => {
  const creator = await _voters.get({ledgerNodeId, privateKey: true});
  return jsigs.sign(event, {
    algorithm: 'Ed25519Signature2018',
    privateKeyBase58: creator.publicKey.privateKey.privateKeyBase58,
    creator: creator.publicKey.id
  });
};

/**
 * Verify the proof signature on a merge event using the public key derived
 * from its voter ID. This method assumes that the merge event has a single
 * authentication proof (i.e. that validation has been performed to confirm
 * this).
 *
 * @param event the merge event to verify.
 *
 * @return a Promise that resolves when there is no verification failure and
 *         that rejects otherwise.
 */
api.verify = async ({event}) => {
  let publicKeyBase58;
  let keyOwner;
  const result = await jsigs.verify(event, {
    getPublicKey: _getPublicKey,
    getPublicKeyOwner: _getPublicKeyOwner,
    checkTimestamp: false
  });
  if(!result.verified) {
    const {verified} = result;
    const keyResultError = _.get(result, 'keyResults.error', 'none');
    logger.debug('Signature Verification Failure', {verified, keyResultError});
    throw new BedrockError(
      'Merge event signature verification failed.',
      'AuthenticationError', {
        event,
        // FIXME: enable when bedrock.logger can properly log `error`
        // keyResults: result.keyResults
      });
  }

  return {keyOwner};

  async function _getPublicKey(publicKeyId) {
    publicKeyBase58 = _voters.getPublicKeyFromId({voterId: publicKeyId});
    return {
      '@context': constants.SECURITY_CONTEXT_V2_URL,
      id: publicKeyId,
      type: 'Ed25519VerificationKey2018',
      owner: publicKeyId,
      publicKey: publicKeyId,
      publicKeyBase58
    };
  }

  async function _getPublicKeyOwner(owner) {
    return keyOwner = {
      '@context': constants.SECURITY_CONTEXT_V2_URL,
      id: owner,
      publicKey: {
        id: owner,
        type: 'Ed25519VerificationKey2018',
        owner,
        publicKeyBase58
      }
    };
  }
};
