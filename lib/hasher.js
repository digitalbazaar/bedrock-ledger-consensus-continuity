/*
 * Web Ledger Continuity2017 hasher.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const bedrock = require('bedrock');
const jsonld = bedrock.jsonld;
const niUri = require('ni-uri');

module.exports = function hasher(data, callback) {
  async.auto({
    // normalize ledger event to nquads
    normalize: callback => jsonld.normalize(data, {
      algorithm: 'URDNA2015',
      format: 'application/nquads'
    }, callback),
    hash: ['normalize', (results, callback) => {
      const hash = niUri.digest('sha-256', results.normalize, true);
      callback(null, hash);
    }]
  }, (err, results) => callback(err, results.hash));
}
