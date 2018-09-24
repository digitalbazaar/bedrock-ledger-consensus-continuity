/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
const bedrock = require('bedrock');
require('bedrock-ledger-consensus-continuity');
require('bedrock-ledger-consensus-continuity-es-most-recent-participants');
require('bedrock-ledger-consensus-continuity-es-most-recent-participants-' +
  'with-recovery');
require('bedrock-ledger-storage-mongodb');

// initialize a mock alternate consensus method used in testing
require('./mocha/mock.alternate-consensus');

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
