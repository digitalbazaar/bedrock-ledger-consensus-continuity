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
  '@context': [
    constants.WEB_LEDGER_CONTEXT_V1_URL,
    constants.ED25519_2020_CONTEXT_V1_URL
  ],
  type: 'WebLedgerConfiguration',
  ledger: 'did:v1:uuid:eb8c22dc-bde6-4315-92e2-59bd3f3c7d59',
  consensusMethod: 'Continuity2017',
  witnessSelectionMethod: {
    type: 'WitnessPoolWitnessSelection',
    witnessPool: 'did:v1:uuid:2f3c9466-ddc9-11eb-92f2-f31707920b3b'
  },
  sequence: 0
};

mock.witnessPool = {
  '@context': [
    constants.WEB_LEDGER_CONTEXT_V1_URL,
    constants.ED25519_2020_CONTEXT_V1_URL
  ],
  id: ledgerConfiguration.witnessSelectionMethod.witnessPool,
  type: 'WitnessPool',
  // the rest of these fields are test-run specific and set at runtime
  controller: 'did:v1:nym:z6MkkykQv27u9XnqTD4gjpyKzxseiU41TLzZSJQeoms6Gnb4',
  maximumWitnessCount: 0,
  primaryWitnessCandidate: [],
  secondaryWitnessCandidate: []
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
    publicKey: 'z6MkhjYywBWYTS5U2XfWdTzB8M4eaQhufiTLdrJz2uGL8FPZ',
    privateKey: 'zrv4kd5YoRYTFTore36rtwmFCs2sWHsJFTF63fA44RcySGJBofUo78MPSDd' +
      'LC9JHtELReaG1U3Smb25hnTsP38vmNrX'
  },
  unauthorized: { // unauthorized group
    publicKey: 'z6MkkykQv27u9XnqTD4gjpyKzxseiU41TLzZSJQeoms6Gnb4',
    privateKey: 'zrv4GDfChcAd6fsdMkVJ3agthxa5GQgHA2B3bGgCiMfCeheXioPEbXihdxd' +
      '3gTPP2kHQXnc3anc8Xeo8U6k2F926tgS'
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
      type: 'Ed25519VerificationKey2020',
      controller: mock.exampleIdentity,
      publicKeyMultibase: mock.groups.authorized.publicKey
    }]
  }
};
mock.ldDocuments[mock.authorizedSignerUrl] = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'Ed25519VerificationKey2020',
  controller: mock.exampleIdentity,
  label: 'Signing Key 2',
  id: mock.authorizedSignerUrl,
  publicKeyMultibase: mock.groups.authorized.publicKey
};

mock.consensusInput = require('./continuity-test-vectors');
