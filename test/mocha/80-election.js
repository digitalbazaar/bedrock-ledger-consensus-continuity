/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
/* globals should */

'use strict';

const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedger = require('bedrock-ledger');
const brTest = require('bedrock-test');
const async = require('async');
const expect = global.chai.expect;
const request = brTest.require('request');
const sinon = require('sinon');
const uuid = require('uuid/v4');

const helpers = require('./helpers');
const mockData = require('./mock.data');

describe.only('Election API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  let consensusApi;
  let ledgerNode;
  let voterId;
  let eventHash;
  let testEventId;
  beforeEach(done => {
    const configEvent = mockData.events.config;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
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
          ledgerNode = result;
          callback();
        })],
      getVoter: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          voterId = result.id;
          callback();
        });
      }],
      addEvent: ['ledgerNode', (results, callback) => ledgerNode.events.add(
        testEvent, (err, result) => {
          eventHash = result.meta.eventHash;
          callback();
        })]
    }, done);
  });
  describe('_getManifest', () => {
    before(() => {
      sinon.stub(request, 'get').callsFake((options, callback) => {
        // console.log('AAAAAA', arguments);
        if(!mockData.sinon.manifests[options.url]) {
          callback(null, {
            statusCode: 404,
            body: {error: 'Manifest not found.'}
          });
        }
        callback(null, {
          statusCode: 200,
          body: {yo: 'ho'}
        });
      });

    });
    after(() => {
      request.get.restore();
    });
    it('gets a manifest out of local storage', done => {
      async.auto({
        createManifest: callback => consensusApi._worker._election
          ._createEventManifest(ledgerNode, 1, callback),
        getManifest: ['createManifest', (results, callback) => {
          const manifestHash = results.createManifest.id;
          consensusApi._election._getManifest(
            voterId, manifestHash, (err, result) => {
              should.not.exist(err);
              result.should.deep.equal(results.createManifest);
              callback();
            });
        }]
      }, done);
    });
    it('returns NotFound on unknown manifestHash', done => {
      async.auto({
        getManifest: callback => {
          const manifestHash =
            'ni:///sha-256;-D0-PH-X_NVlNPeTwY9jjtlaH-4HOhQHVmzH-CT6rYI';
          consensusApi._election._getManifest(
            voterId, manifestHash, (err, result) => {
              should.exist(err);
              err.name.should.equal('NotFound');
              callback();
            });
        }
      }, done);
    });
  });
});
