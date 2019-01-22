/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _voters = require('./voters');
const bedrock = require('bedrock');
const {config: {constants}, util: {BedrockError}} = bedrock;
const jsigs = require('jsonld-signatures');
const logger = require('./logger');
const {Ed25519KeyPair} = require('crypto-ld');
const {Ed25519Signature2018} = jsigs.suites;
const {AssertionProofPurpose} = jsigs.purposes;

const api = {};
module.exports = api;

/**
 * Sign a merge event.
 *
 * @param event the merge event to sign.
 * @param ledgerNodeId the ID of the ledger node associated with the event.
 *
 * @return {Promise} the provided event with a proof attached.
 */
api.sign = async ({event, ledgerNodeId}) => {
  const creator = await _voters.get(
    {ledgerNodeId, privateKey: true, publicKey: true});
  const key = new Ed25519KeyPair({
    id: creator.publicKey.id,
    privateKeyBase58: creator.publicKey.privateKey.privateKeyBase58,
    publicKeyBase58: creator.publicKey.publicKeyBase58
  });
  return jsigs.sign(event, {
    compactProof: false,
    documentLoader: bedrock.jsonld.documentLoader,
    suite: new Ed25519Signature2018({key}),
    purpose: new AssertionProofPurpose()
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
 * @return {Promise} resolves when there is no verification failure and
 *         rejects otherwise.
 */
api.verify = async ({event}) => {
  let publicKeyBase58;
  const {proof: {verificationMethod: publicKeyId}} = event;
  const controller = {
    '@context': constants.SECURITY_CONTEXT_URL,
    id: publicKeyId,
    assertionMethod: publicKeyId,
  };
  const key = new Ed25519KeyPair(await _getPublicKey(publicKeyId));
  const result = await jsigs.verify(event, {
    documentLoader: bedrock.jsonld.documentLoader,
    purpose: new AssertionProofPurpose({controller}),
    suite: new Ed25519Signature2018({key}),
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

  return {keyOwner: {id: publicKeyId}};

  async function _getPublicKey(publicKeyId) {
    publicKeyBase58 = _voters.getPublicKeyFromId({voterId: publicKeyId});
    return {
      id: publicKeyId,
      type: 'Ed25519VerificationKey2018',
      controller: publicKeyId,
      publicKey: publicKeyId,
      publicKeyBase58
    };
  }
};
