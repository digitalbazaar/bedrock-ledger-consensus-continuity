/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const {jsonLdDocumentLoader} = require('bedrock-jsonld-document-loader');
require('bedrock-ledger-consensus-continuity');
require('bedrock-ledger-consensus-continuity-ws-witness-pool');
require('bedrock-ledger-storage-mongodb');

// initialize a mock alternate consensus method used in testing
require('./mocha/mock.alternate-consensus');

bedrock.events.on('bedrock.init', () => {
  const mockData = require('./mocha/mock.data');
  for(const url in mockData.ldDocuments) {
    jsonLdDocumentLoader.addStatic(url, mockData.ldDocuments[url]);
  }
});

require('bedrock-test');
bedrock.start();
