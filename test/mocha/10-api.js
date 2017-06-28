/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const async = require('async');
const expect = global.chai.expect;
const events = bedrock.events;
require('bedrock-ledger-continuity');

const helpers = require('./helpers');
const mockData = require('./mock.data');

describe('Continuity2017', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  describe('Event Consensus', () => {
    // create ledger node for use in each test
    let ledgerNode;
    beforeEach(done => {
      const mockIdentity = mockData.identities.regularUser;
      async.auto({
        clean: callback =>
          helpers.removeCollections(['ledger', 'ledgerNode'], callback),
        actor: ['clean', (results, callback) => brIdentity.get(
          null, mockIdentity.identity.id, (err, result) => {
          actor = result;
          const configBlock = mockData.configBlocks.alpha;
          callback(err);
        })],
        ledgerNode: ['getActor', (results, callback) => brLedger.add(
          results.actor, configBlock, (err, ledgerNode) => {
            should.not.exist(err);
            expect(ledgerNode).to.be.ok;
            callback(null, ledgerNode);
          })]
      }, (err, results) => {
        if(err) {
          return done(err);
        }
        ledgerNode = results.ledgerNode;
        done();
      });
    });

    it.only('should add an event and achieve consensus', done => {
      done();
      events.onceAsync('bedrock-ledger-continuity.consensus', (e, callback) => {
        // TODO: assert things about ledger event
        done();
      });

      // TODO: add event
    });
  });
});
