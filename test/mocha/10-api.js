/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedger = require('bedrock-ledger');
const async = require('async');
const expect = global.chai.expect;
const events = bedrock.events;
const should = global.should;
require('bedrock-ledger-continuity');

const helpers = require('./helpers');
const mockData = require('./mock.data');

describe('Continuity2017', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  describe('Event Consensus', () => {
    // get consensus plugin and create ledger node for use in each test
    let consensusApi;
    let ledgerNode;
    beforeEach(done => {
      const mockIdentity = mockData.identities.regularUser;
      const configEvent = mockData.events.config;
      async.auto({
        clean: callback =>
          helpers.removeCollections(['ledger', 'ledgerNode'], callback),
        actor: ['clean', (results, callback) => brIdentity.get(
          null, mockIdentity.identity.id, (err, identity) => {
            callback(err, identity);
          })],
        consensusPlugin: callback => brLedger.use('Continuity2017', callback),
        ledgerNode: ['actor', (results, callback) => brLedger.add(
          results.actor, configEvent, (err, ledgerNode) => {
            if(err) {
              return callback(err);
            }
            expect(ledgerNode).to.be.ok;
            callback(null, ledgerNode);
          })]
      }, (err, results) => {
        if(err) {
          return done(err);
        }
        ledgerNode = results.ledgerNode;
        consensusApi = results.consensusPlugin.api;
        done();
      });
    });

    it.skip('should add an event and achieve consensus', done => {
      // // FIXME: remove `done`
      // done();
      // events.onceAsync('bedrock-ledger-continuity.consensus', (e, callback) => {
      //   // TODO: assert things about ledger event
      //   done();
      // });

      // TODO: add event
      consensusApi._worker._run(null, err => {
        done(err);
      });
    });
  });
});
