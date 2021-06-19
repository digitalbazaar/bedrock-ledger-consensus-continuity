/*!
 * Copyright (c) 2019-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {Ed25519Signature2020} =
  require('@digitalbazaar/ed25519-signature-2020');

// MergeEventEd25519Signature2020 is just an Ed25519Signature2020 that is
// optimized for merge events
module.exports = class MergeEventEd25519Signature2020
  extends Ed25519Signature2020 {
  constructor(...args) {
    super(...args);
  }

  async canonize(input) {
    let optimized =
      '_:c14n0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ' +
      '<https://w3id.org/webledger#ContinuityMergeEvent> .\n' +
      '_:c14n0 <https://w3id.org/webledger#basisBlockHeight> ' +
      `"${input.basisBlockHeight}"` +
      '^^<http://www.w3.org/2001/XMLSchema#integer> .\n' +
      '_:c14n0 <https://w3id.org/webledger#mergeHeight> ' +
      `"${input.mergeHeight}"` +
      '^^<http://www.w3.org/2001/XMLSchema#integer> .\n';

    let parentHashes;
    if(Array.isArray(input.parentHash)) {
      parentHashes = input.parentHash.slice();
      parentHashes.sort();
    } else {
      parentHashes = [input.parentHash];
    }
    const parentQuads = parentHashes.map(h =>
      `_:c14n0 <https://w3id.org/webledger#parentHash> "${h}" .\n`);
    optimized += parentQuads.join('');

    if(input.parentHashCommitment) {
      let parentHashCommitments;
      if(Array.isArray(input.parentHashCommitment)) {
        parentHashCommitments = input.parentHashCommitment.slice();
        parentHashCommitments.sort();
      } else {
        parentHashCommitments = [input.parentHashCommitment];
      }
      const parentCommitmentQuads = parentHashCommitments.map(h =>
        `_:c14n0 <https://w3id.org/webledger#parentHashCommitment> "${h}" .\n`);
      optimized += parentCommitmentQuads.join('');
    }

    if(input.peerCommitment) {
      let peerCommitments;
      if(Array.isArray(input.peerCommitment)) {
        peerCommitments = input.peerCommitment.slice();
        peerCommitments.sort();
      } else {
        peerCommitments = [input.peerCommitment];
      }
      const peerCommitmentQuads = peerCommitments.map(h =>
        `_:c14n0 <https://w3id.org/webledger#peerCommitment> "${h}" .\n`);
      optimized += peerCommitmentQuads.join('');
    }

    if(input.treeHash) {
      optimized +=
        `_:c14n0 <https://w3id.org/webledger#treeHash> "${input.treeHash}" .\n`;
    }

    return optimized;
  }

  async canonizeProof(proof) {
    // Note: Code assumes `proof` has been validated.
    const optimized =
      '_:c14n0 <http://purl.org/dc/terms/created> ' +
        `"${proof.created}"^^<http://www.w3.org/2001/XMLSchema#dateTime> .\n` +
      '_:c14n0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ' +
        '<https://w3id.org/security#Ed25519Signature2020> .\n' +
      '_:c14n0 <https://w3id.org/security#proofPurpose> ' +
        '<https://w3id.org/security#assertionMethod> .\n' +
      '_:c14n0 <https://w3id.org/security#verificationMethod> ' +
        `<${proof.verificationMethod}> .\n`;
    return optimized;
  }
};
