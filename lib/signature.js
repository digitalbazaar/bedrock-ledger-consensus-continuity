/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const _localPeers = require('./localPeers');
const bedrock = require('bedrock');
const {config: {constants}, util: {BedrockError}} = bedrock;
const jsigs = require('jsonld-signatures');
const {
  parseRequest,
  createAuthzHeader,
  createSignatureString
} = require('http-signature-header');
const {createHeaderValue} = require('@digitalbazaar/http-digest-header');
const {documentLoader} = require('bedrock-jsonld-document-loader');
const logger = require('./logger');
const {Ed25519VerificationKey2020} =
  require('@digitalbazaar/ed25519-verification-key-2020');
const MergeEventEd25519Signature2020 = require(
  './MergeEventEd25519Signature2020');

const {
  // TODO: remove once tests are written
  //suites: {Ed25519Signature2020},
  purposes: {AssertionProofPurpose}
} = jsigs;

exports.getKeyPair = async ({ledgerNodeId}) => {
  const localPeer = await _localPeers.getKeyPair({ledgerNodeId});

  return new Ed25519VerificationKey2020({
    id: localPeer.peerId,
    publicKeyMultibase: localPeer.keyPair.publicKeyMultibase,
    privateKeyMultibase: localPeer.keyPair.privateKeyMultibase
  });
};

/**
 * Sign a merge event.
 *
 * @param event the merge event to sign.
 * @param ledgerNodeId the ID of the ledger node associated with the event.
 *
 * @return {Promise} the provided event with a proof attached.
 */
exports.sign = async ({event, ledgerNodeId}) => {
  const key = await exports.getKeyPair({ledgerNodeId});
  const signed = await jsigs.sign(event, {
    compactProof: false,
    documentLoader,
    // TODO: add tests that compare output of this function against using
    //  `Ed25519Signature2020`
    //suite: new Ed25519Signature2020({key}),
    //suite: new MergeEventEd25519Signature2020({key}),
    suite: new MergeEventEd25519Signature2020({key}),
    purpose: new AssertionProofPurpose()
  });
  return {signed, peerId: key.id};
};

const COVERED_CONTENT = [
  '(created)',
  '(expires)',
  // (key-id) will include the peerId in the signature itself
  '(key-id)',
  '(request-target)',
  'digest',
  'content-type',
  'host'
];

/**
 * Signs a request with a valid HTTP Signature header.
 *
 * @param {object} options - Options to use.
 * @param {URL} options.url - The request URL.
 * @param {object} options.json - The request body.
 * @param {object} options.signer - A signer for the signature.
 * @param {Array<string>} [options.includeHeaders = COVERED_CONTENT] - Which
 *   headers to include in the signature.
 *
 * @returns {Promise<object>} Returns the headers for the request.
 */
exports.signRequest = async ({
  url,
  json,
  signer,
  includeHeaders = COVERED_CONTENT
}) => {
  const created = Math.floor(Date.now() / 1000);
  const expires = created + 5 * 60;
  const headers = {
    digest: await createHeaderValue({data: json, useMultihash: true}),
    'content-type': 'application/json'
  };
  const plainText = createSignatureString({
    includeHeaders,
    headers,
    requestOptions: {
      created,
      expires,
      url,
      method: 'POST',
      headers,
      keyId: signer.id
    }
  });
  const data = new TextEncoder().encode(plainText);
  const sigBuffer = await signer.sign({data});
  const signature = Buffer.from(
    sigBuffer, sigBuffer.offset, sigBuffer.length).toString('base64');
  const authorization = createAuthzHeader({
    created,
    expires,
    includeHeaders,
    keyId: signer.id,
    signature
  });
  return {authorization, ...headers};
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
  const key = new Ed25519VerificationKey2020(_getPublicKey(publicKeyId));
  const result = await jsigs.verify(event, {
    compactProof: false,
    documentLoader,
    purpose: new AssertionProofPurpose({controller}),
    // TODO: add tests that compare output of this function against using
    //  `Ed25519Signature2020`
    //suite: new Ed25519Signature2020({key}),
    suite: new MergeEventEd25519Signature2020({key})
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

/**
 * Verifies a request's HTTP signature header.
 *
 * @param {object} options to use.
 * @param {object} options.req - An express Request object.
 * @param {Array<string>} [options.expectedHeaders = COVERED_CONTENT] - Which
 *   headers are expected to be in the request.
 *
 * @returns {Promise<object>} The result of verification and the keyId.
 */
exports.verifyRequest = async ({req, expectedHeaders = COVERED_CONTENT}) => {
  try {
    const {url, method, headers} = req;
    const parsed = parseRequest(
      {url, method, headers}, {headers: expectedHeaders});
    // get parsed parameters from from HTTP header and generate signing string
    const {keyId, signingString, params: {signature: b64Signature}} = parsed;
    // returns the key bytes
    const key = new Ed25519VerificationKey2020(_getPublicKey(keyId));
    const verifier = key.verifier();
    const encoder = new TextEncoder();
    const data = encoder.encode(signingString);
    const signature = Buffer.from(b64Signature, 'base64');
    // verify HTTP signature
    const verified = await verifier.verify({data, signature});
    return {verified, keyId};
  } catch(e) {
    throw new BedrockError(
      'Signature verification failed.', 'DataError',
      {public: true, httpStatusCode: 403}, e);
  }
};

function _getPublicKey(publicKeyId) {
  const publicKeyMultibase = _localPeers.getPublicKeyFromId(
    {peerId: publicKeyId});
  return {
    id: publicKeyId,
    type: 'Ed25519VerificationKey2020',
    controller: publicKeyId,
    publicKeyMultibase
  };
}
