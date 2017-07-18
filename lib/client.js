/*
 * Client for Continuity2017 consensus method.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const config = bedrock.config;

require('./config');

// module API
const api = {};
module.exports = api;

// define request pool for all gossip requests
const requestPool = {};
Object.defineProperty(requestPool, 'maxSockets', {
  configurable: true,
  enumerable: true,
  get: () => config['ledger-continuity'].gossip.requestPool.maxSockets
});

// FIXME: remove me
const mockUuid = '6b6c9cbd-75db-434d-9892-152e2063f1f3';
const mockStatus = {
  statusCount: 0,
  status: [{
    '@context': 'https://w3id.org/webledger/v1',
    blockHeight: 2,
    consensusPhase: 'gossip',
    eventHash: ['ni:///sha-256;cGBSKHn2cBJ563oSt3SAf4OxZXXfwtSxj1xFO5LtkGkW']
  }, {
    '@context': 'https://w3id.org/webledger/v1',
    blockHeight: 2,
    consensusPhase: 'decideEvents',
    election: [{
      topic: 'Events',
      electionResults: [{
        manifestHash: 'ni:///sha-256;Jd7DiClbgmaivhLPoImJjrcr8GgnANE-rVYXf02EHdA',
        round: 1,
        voter: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
        signature: {
          type: 'LinkedDataSignature2015',
          creator: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
          signatureValue: 'dsafhb2bf72hfb7823h...f8732hf238f23'
        }
      }]
    }]
  }, {
    '@context': 'https://w3id.org/webledger/v1',
    blockHeight: 2,
    consensusPhase: 'decideRollCall',
    election: [{
      topic: 'Events',
      electionResults: [{
        manifestHash: 'ni:///sha-256;Jd7DiClbgmaivhLPoImJjrcr8GgnANE-rVYXf02EHdA',
        round: 1,
        voter: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
        signature: {
          type: 'LinkedDataSignature2015',
          creator: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
          signatureValue: 'dsafhb2bf72hfb7823h...f8732hf238f23'
        }
      }]
    }, {
      topic: 'RollCall',
      electionResults: [{
        manifestHash: 'ni:///sha-256;Jd7DiClbgmaivhLPoImJjrcr8GgnANE-rVYXf02EHdA',
        round: 2,
        voter: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
        signature: {
          type: 'LinkedDataSignature2015',
          creator: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
          signatureValue: 'dsafhb2bf72hfb7823h...f8732hf238f23'
        }
      }]
    }]
  }, {
    '@context': 'https://w3id.org/webledger/v1',
    blockHeight: 2,
    consensusPhase: 'consensus',
    election: [{
      topic: 'Events',
      electionResults: [{
        manifestHash: 'ni:///sha-256;Jd7DiClbgmaivhLPoImJjrcr8GgnANE-rVYXf02EHdA',
        round: 1,
        voter: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
        signature: {
          type: 'LinkedDataSignature2015',
          creator: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
          signatureValue: 'dsafhb2bf72hfb7823h...f8732hf238f23'
        }
      }]
    }, {
      topic: 'RollCall',
      electionResults: [{
        manifestHash: 'ni:///sha-256;Jd7DiClbgmaivhLPoImJjrcr8GgnANE-rVYXf02EHdA',
        round: 2,
        voter: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
        signature: {
          type: 'LinkedDataSignature2015',
          creator: 'urn:rsa-pubkey-sha256:d436fbd6a3ba6cb4a3c6d6b34363a6bc3b6b3c',
          signatureValue: 'dsafhb2bf72hfb7823h...f8732hf238f23'
        }
      }]
    }]
  }]
};

// TODO: add method for getting block status
api.getBlockStatus = (blockHeight, peer, callback) => {
  // TODO: implement me, currently mocked
  const status = mockStatus.status[mockStatus.statusCount];
  mockStatus.statusCount = Math.min(
    mockStatus.statusCount + 1, mockStatus.status.length);
  callback(null, status);
};

// TODO: add method for getting gossip given block status
api.getManifest = (manifestHash, peer, callback) => {
  // TODO: get manifest (event hashes) from peer and store via manifestStorage
};

// TODO: add method for sending gossip given block status
api.sendEvents = (blockStatus, peer, callback) => {
  // TODO: get all non-consensus, non-deleted events from storage modulo the
  // ones with hashes in blockStatus.eventHash... and send them to the peer.
  // TODO: potentially send one at a time instead of in bulk
  callback(new Error('Not implemented'));
}

// TODO: add method for getting event
api.getEvent = (eventHash, peer, callback) => {
  // TODO: get event from peer and pass to consensus.add.events
  callback(new Error('Not implemented'));
};
