/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const config = bedrock.config;
const constants = config.constants;
const helpers = require('./helpers');

const mock = {};
module.exports = mock;

const identities = mock.identities = {};

const userName = 'regularUser';
identities[userName] = {};
identities[userName].identity = helpers.createIdentity(userName);
identities[userName].meta = {
  sysResourceRole: [{
    sysRole: 'bedrock-ledger.test',
    generateResource: 'id'
  }]
};

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

mock.ledgerConfigurationRecovery = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'WebLedgerConfiguration',
  ledger: 'did:v1:eb8c22dc-bde6-4315-92e2-59bd3f3c7d59',
  consensusMethod: 'Continuity2017',
  electorSelectionMethod: {
    type: 'MostRecentParticipantsWithRecovery',
  },
  sequence: 0
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
      owner: mock.exampleIdentity,
      publicKeyBase58: mock.groups.authorized.publicKey
    }]
  }
};
mock.ldDocuments[mock.authorizedSignerUrl] = {
  '@context': constants.WEB_LEDGER_CONTEXT_V1_URL,
  type: 'Ed25519VerificationKey2018',
  owner: mock.exampleIdentity,
  label: 'Signing Key 2',
  id: mock.authorizedSignerUrl,
  publicKeyBase58: mock.groups.authorized.publicKey
};

/*
const jsonld = bedrock.jsonld;
const oldLoader = jsonld.documentLoader;
jsonld.documentLoader = function(url, callback) {
  if(Object.keys(mock.ldDocuments).includes(url)) {
    return callback(null, {
      contextUrl: null,
      document: mock.ldDocuments[url],
      documentUrl: url
    });
  }
  // const regex = new RegExp(
  //   'http://authorization.dev/dids' + '/(.*?)$');
  // const didMatch = url.match(regex);
  // if(didMatch && didMatch.length === 2 && didMatch[1] in mock.didDocuments) {
  //   return callback(null, {
  //     contextUrl: null,
  //     document: mock.didDocuments[didMatch[1]],
  //     documentUrl: url
  //   });
  // }
  oldLoader(url, callback);
};
*/

const consensusInput = {
  'fig-1-2': require('./consensus-input-fig-1-2'),
  'fig-1-4': require('./consensus-input-fig-1-4'),
  'fig-1-5': require('./consensus-input-fig-1-5'),
  'fig-1-6': require('./consensus-input-fig-1-6'),
  'fig-1-7': require('./consensus-input-fig-1-7'),
  'fig-1-8': require('./consensus-input-fig-1-8'),
  'fig-1-9': require('./consensus-input-fig-1-9'),
  'fig-1-10': require('./consensus-input-fig-1-10'),
  'fig-1-11': require('./consensus-input-fig-1-11'),
  'fig-1-12': require('./consensus-input-fig-1-12')
};

mock.consensusInput = consensusInput;
