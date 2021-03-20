/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _localPeers = require('./localPeers');
const bedrock = require('bedrock');
const {config: {constants}, util: {BedrockError}} = bedrock;
const jsigs = require('jsonld-signatures');
const {documentLoader} = require('bedrock-jsonld-document-loader');
const logger = require('./logger');
const {Ed25519VerificationKey2018} =
  require('@digitalbazaar/ed25519-verification-key-2018');
const MergeEventEd25519Signature2018 = require(
  './MergeEventEd25519Signature2018');

const {
  // TODO: remove once tests are written
  //suites: {Ed25519Signature2018},
  purposes: {AssertionProofPurpose}
} = jsigs;

/**
 * Sign a merge event.
 *
 * @param event the merge event to sign.
 * @param ledgerNodeId the ID of the ledger node associated with the event.
 *
 * @return {Promise} the provided event with a proof attached.
 */
exports.sign = async ({event, ledgerNodeId}) => {
  const localPeer = await _localPeers.getKeyPair({ledgerNodeId});
  const key = new Ed25519VerificationKey2018({
    id: localPeer.peerId,
    privateKeyBase58: localPeer.keyPair.privateKeyBase58,
    publicKeyBase58: localPeer.keyPair.publicKeyBase58
  });
  const signed = await jsigs.sign(event, {
    compactProof: false,
    documentLoader,
    // TODO: add tests that compare output of this function against using
    //  `Ed25519Signature2018`
    //suite: new Ed25519Signature2018({key}),
    //suite: new MergeEventEd25519Signature2018({key}),
    suite: new MergeEventEd25519Signature2018({key}),
    purpose: new AssertionProofPurpose()
  });
  return {signed, peerId: localPeer.peerId};
};

/**
 * Verify the proof signature on a merge event using the public key derived
 * from its peer ID. This method assumes that the merge event has a single
 * assertion proof (i.e. that validation has been performed to confirm
 * this).
 *
 * @param event the merge event to verify.
 *
 * @return {Promise} resolves when there is no verification failure and
 *   rejects otherwise.
 */
exports.verify = async ({event}) => {
  const {proof: {verificationMethod: publicKeyId}} = event;
  const controller = {
    '@context': constants.SECURITY_CONTEXT_URL,
    id: publicKeyId,
    assertionMethod: publicKeyId,
  };
  const key = new Ed25519VerificationKey2018(_getPublicKey(publicKeyId));
  const result = await jsigs.verify(event, {
    compactProof: false,
    documentLoader,
    purpose: new AssertionProofPurpose({controller}),
    // TODO: add tests that compare output of this function against using
    //  `Ed25519Signature2018`
    //suite: new Ed25519Signature2018({key}),
    suite: new MergeEventEd25519Signature2018({key})
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

  return {controller: publicKeyId};
};

function _getPublicKey(publicKeyId) {
  const publicKeyBase58 = _localPeers.getPublicKeyFromId({peerId: publicKeyId});
  return {
    id: publicKeyId,
    type: 'Ed25519VerificationKey2018',
    controller: publicKeyId,
    publicKeyBase58
  };
}
