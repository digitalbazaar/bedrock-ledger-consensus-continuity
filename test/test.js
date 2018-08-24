/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
require('bedrock-ledger-consensus-continuity');
require('bedrock-ledger-storage-mongodb');

bedrock.events.on('bedrock.init', () => {
  const jsonld = bedrock.jsonld;
  const mockData = require('./mocha/mock.data');

  const oldLoader = jsonld.documentLoader;

  // load mock documents
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
});

require('bedrock-test');
bedrock.start();
