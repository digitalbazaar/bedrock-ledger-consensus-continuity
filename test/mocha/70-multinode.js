/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
/* globals should */

'use strict';

const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedger = require('bedrock-ledger');
const async = require('async');
const expect = global.chai.expect;
const uuid = require('uuid/v4');

const helpers = require('./helpers');
const mockData = require('./mock.data');

describe('Multinode', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  describe('Consensus with 4 Nodes', () => {
    const nodes = 4;
    const maxConsensusTime = 1000;

    // get consensus plugin and create genesis ledger node
    let consensusApi;
    let genesisLedgerNode;
    const mockIdentity = mockData.identities.regularUser;
    const configEvent = mockData.events.config;
    before(done => {
      async.auto({
        clean: callback =>
          helpers.removeCollections(['ledger', 'ledgerNode'], callback),
        actor: ['clean', (results, callback) => brIdentity.get(
          null, mockIdentity.identity.id, (err, identity) => {
            callback(err, identity);
          })],
        consensusPlugin: callback => brLedger.use('Continuity2017', callback),
        ledgerNode: ['actor', (results, callback) => brLedger.add(
          null, {configEvent}, (err, ledgerNode) => {
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
        genesisLedgerNode = results.ledgerNode;
        consensusApi = results.consensusPlugin.api;
        done();
      });
    });

    // get genesis record (block + meta)
    let genesisRecord;
    before(done => {
      genesisLedgerNode.blocks.getGenesis((err, result) => {
        if(err) {
          return done(err);
        }
        genesisRecord = result.genesisBlock;
        done();
      });
    });

    // add N - 1 more private nodes
    const peers = [genesisLedgerNode];
    before(done => {
      let count = 0;
      async.whilst(() => count++ < (nodes - 1), loop => {
        brLedger.add(null, {
          genesisBlock: genesisRecord.block,
          owner: mockIdentity.identity.id,
        }, (err, ledgerNode) => {
          if(err) {
            return loop(err);
          }
          peers.push(ledgerNode);
          loop();
        });
      }, done);
    });

    it('should add an event and achieve consensus', done => {
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = 'https://example.com/events/' + uuid();
      async.auto({
        addEvent: callback => genesisLedgerNode.events.add(
          testEvent, callback),
        runWorkers: ['addEvent', (results, callback) => async.each(
          peers,
          (ledgerNode, callback) => consensusApi._worker._run(null, callback),
          callback)],
        wait: ['runWorkers', (results, callback) => {
          setTimeout(callback, maxConsensusTime);
        }],
        getLatest: ['runWorkers', (results, callback) =>
          async.each(peers, (ledgerNode, callback) =>
            ledgerNode.storage.blocks.getLatest((err, result) => {
              should.not.exist(err);
              const eventBlock = result.eventBlock;
              should.exist(eventBlock.block);
              eventBlock.block.event.should.be.an('array');
              eventBlock.block.event.should.have.length(1);
              const event = eventBlock.block.event[0];
              event.input.should.be.an('array');
              event.input.should.have.length(1);
              event.should.deep.equal(testEvent);
              should.exist(eventBlock.meta);
              callback();
          }, callback))]
      }, done);
    });
  });
});
