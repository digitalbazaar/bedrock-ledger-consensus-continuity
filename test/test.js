/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
const brDidClient = require('bedrock-did-client');
require('bedrock-ledger-consensus-continuity');
require('bedrock-ledger-storage-mongodb');

bedrock.events.on('bedrock.init', () => {
  const jsonld = bedrock.jsonld;
  const mockData = require('./mocha/mock.data');

  const oldLoader = jsonld.documentLoader;

  jsonld.documentLoader = function(url, callback) {
    if(Object.keys(mockData.ldDocuments).includes(url)) {
      return callback(null, {
        contextUrl: null,
        document: mockData.ldDocuments[url],
        documentUrl: url
      });
    }
    oldLoader(url, callback);
  };
  // override jsonld.documentLoader in brDidClient so this document loader
  // can be used for did: and https: URLs
  brDidClient.jsonld.documentLoader = jsonld.documentLoader;
});

require('bedrock-test');
bedrock.start();
