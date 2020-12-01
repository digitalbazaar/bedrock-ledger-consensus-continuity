/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const config = bedrock.config;
const constants = config.constants;

const mock = {};
module.exports = mock;

const ledgerConfiguration = mock.ledgerConfiguration = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'WebLedgerConfiguration',
  ledger: 'did:v1:eb8c22dc-bde6-4315-92e2-59bd3f3c7d59',
  consensusMethod: 'Continuity2017',
  electorSelectionMethod: {
    type: 'MostRecentParticipants',
  },
  sequence: 0,
};

const operations = mock.operations = {};
operations.alpha = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'CreateWebLedgerRecord',
  record: {
    '@context': constants.TEST_CONTEXT_V1_URL,
    id: 'https://example.com/events/123456',
    type: 'Concert',
    name: 'Big Band Concert in New York City',
    startDate: '2017-07-14T21:30',
    location: 'https://example.org/the-venue',
    offers: {
      type: 'Offer',
      price: '13.00',
      priceCurrency: 'USD',
      url: 'https://www.ticketfly.com/purchase/309433'
    }
  }
};
operations.beta = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'CreateWebLedgerRecord',
  record: {
    '@context': constants.TEST_CONTEXT_V1_URL,
    id: 'https://example.com/events/4444',
    type: 'Concert',
    name: 'Big Band Concert in Atlanta',
    startDate: '2017-07-14T21:30',
    location: 'https://example.org/the-other-venue',
    offers: {
      type: 'Offer',
      price: '13.00',
      priceCurrency: 'USD',
      url: 'https://www.ticketfly.com/purchase/309433'
    }
  }
};

const events = mock.events = {};
events.alpha = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'WebLedgerOperationEvent',
  // operation: [operations.alpha, operations.beta]
};

events.config = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'WebLedgerConfigurationEvent',
  ledgerConfiguration
};

const mergeEvents = mock.mergeEvents = {};
mergeEvents.alpha = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'ContinuityMergeEvent',
  treeHash: 'ni:///sha-256;1rj73NTf8Nx3fhGrwHo7elDCF7dfdUqPoK2tzpf-XXX',
  parentHash: [
    'ni:///sha-256;1rj73NTf8Nx3fhGrwHo7elDCF7dfdUqPoK2tzpf-AAA',
    'ni:///sha-256;1rj73NTf8Nx3fhGrwHo7elDCF7dfdUqPoK2tzpf-BBB',
    'ni:///sha-256;1rj73NTf8Nx3fhGrwHo7elDCF7dfdUqPoK2tzpf-CCC'
  ]
};

// constants
mock.authorizedSignerUrl = 'https://example.com/keys/authorized-key-1';

// all mock keys for all groups
mock.groups = {
  authorized: {
    publicKey: 'GycSSui454dpYRKiFdsQ5uaE8Gy3ac6dSMPcAoQsk8yq',
    privateKey: '3Mmk4UzTRJTEtxaKk61LxtgUxAa2Dg36jF6VogPtRiKvfpsQWKPCLesK' +
      'SV182RMmvMJKk6QErH3wgdHp8itkSSiF'
  },
  unauthorized: { // unauthorized group
    publicKey: 'AAD3mt6xZqbJBmMp643irCG7yqCQwVUk4UUK4XGm6ZpW',
    privateKey: '5Y57oBSw5ykt21N3cbHPVDhRPL84xjgfQXN6wnqzWNQbGp5WHhy3XieA' +
      'jzwY9J26Whg1DBv31ktgUnnYuDkWXMTQ'
  }
};

mock.exampleIdentity =
  `https://example.com/i/${mock.groups.authorized.publicKey}`;
mock.ldDocuments = {
  [mock.exampleIdentity]: {
    '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
    id: mock.exampleIdentity,
    publicKey: [{
      id: mock.authorizedSignerUrl,
      type: 'Ed25519VerificationKey2018',
      controller: mock.exampleIdentity,
      publicKeyBase58: mock.groups.authorized.publicKey
    }]
  }
};
mock.ldDocuments[mock.authorizedSignerUrl] = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'Ed25519VerificationKey2018',
  controller: mock.exampleIdentity,
  label: 'Signing Key 2',
  id: mock.authorizedSignerUrl,
  publicKeyBase58: mock.groups.authorized.publicKey
};

mock.consensusInput = require('./continuity-test-vectors');
