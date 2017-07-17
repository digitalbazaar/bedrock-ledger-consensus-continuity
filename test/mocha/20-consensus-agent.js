/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedger = require('bedrock-ledger');
const config = bedrock.config;
const expect = global.chai.expect;
const helpers = require('./helpers');
const mockData = require('./mock.data');
const multihash = require('multihashes')
let request = require('request');
request = request.defaults({json: true, strictSSL: false});

const urlObj = {
  protocol: 'https',
  host: config.server.host,
  pathname: ''
};

describe.only('Consensus Agent API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  let blockHeight;
  beforeEach(done => {
    const configEvent = mockData.events.config;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedger.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedger.add(
        null, configEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          expect(result).to.be.ok;
          ledgerNode = result;
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          voterId = multihash.toB58String(
            multihash.encode(new Buffer(result), 'sha2-256'));
          callback();
        });
      }]
    }, done);
  });

  it('get block status', done => {
    urlObj.pathName = '/consensus/continuity2017/' + voterId +
      '/blocks/' + blockHeight + '/status';
    done();
  });
});
