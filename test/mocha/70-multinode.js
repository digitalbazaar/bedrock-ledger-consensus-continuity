/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
/* globals should */

'use strict';

const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedger = require('bedrock-ledger-node');
const async = require('async');
const uuid = require('uuid/v4');

const helpers = require('./helpers');
const mockData = require('./mock.data');

// NOTE: the tests in this file are designed to run in series
// DO NOT use `it.only`

describe('Multinode', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });

  describe('Consensus with 10 Nodes', () => {
    const nodes = 10;

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
        ledgerNode: ['actor', (results, callback) => {
          brLedger.add(null, {configEvent}, (err, ledgerNode) => {
            if(err) {
              return callback(err);
            }
            callback(null, ledgerNode);
          });
        }]
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
    const peers = [];
    before(function(done) {
      this.timeout(120000);
      peers.push(genesisLedgerNode);
      async.times(nodes - 1, (i, callback) => {
        brLedger.add(null, {
          genesisBlock: genesisRecord.block,
          owner: mockIdentity.identity.id
        }, (err, ledgerNode) => {
          if(err) {
            return callback(err);
          }
          peers.push(ledgerNode);
          callback();
        });
      }, done);
    });

    describe('Block 1', () => {
      let recommendedElectorsBlock1;
      before(done => {
        async.map(peers, (ledgerNode, callback) => {
          consensusApi._voters.get(ledgerNode.id, (err, result) => {
            if(err) {
              return callback(err);
            }
            callback(null, {id: result.id});
          });
        }, (err, result) => {
          if(err) {
            return done(err);
          }
          recommendedElectorsBlock1 = result;
          done();
        });
      });

      it('should add an event and achieve consensus', function(done) {
        this.timeout(120000);
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEvent.input[0].id = 'https://example.com/events/' + uuid();
        // instruct consenses on which electors to use for Block 2
        // these recommended electors will be included in Block 1
        consensusApi._election._recommendElectors =
          (ledgerNode, voter, electors, manifest, callback) => {
            callback(null, recommendedElectorsBlock1);
          };

        async.auto({
          addEvent: callback => genesisLedgerNode.events.add(
            testEvent, callback),
          runWorkers: ['addEvent', (results, callback) => async.each(
            peers, (ledgerNode, callback) =>
              consensusApi._worker._run(ledgerNode, callback), callback)],
          getLatest: ['runWorkers', (results, callback) =>
            async.each(peers, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                if(err) {
                  return callback(err);
                }
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResults);
                eventBlock.block.electionResults.should.be.an('array');
                eventBlock.block.electionResults.should.have.length(1);
                const electionResults = eventBlock.block.electionResults[0];
                should.exist(electionResults.recommendedElector);
                electionResults.recommendedElector.map(e => e.id)
                  .should.have.same.members(recommendedElectorsBlock1.map(
                    e => e.id));
                callback();
              }), callback)]
        }, done);
      });
    }); // end block 1
    describe('Block 2', () => {
      it('should add an event and achieve consensus', function(done) {
        this.timeout(120000);
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEvent.input[0].id = 'https://example.com/events/' + uuid();
        async.auto({
          addEvent: callback => genesisLedgerNode.events.add(
            testEvent, callback),
          runWorkers: ['addEvent', (results, callback) => async.each(
            peers, (ledgerNode, callback) =>
              consensusApi._worker._run(ledgerNode, callback), callback)],
          getLatest: ['runWorkers', (results, callback) =>
            async.each(peers, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                if(err) {
                  return callback(err);
                }
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResults);
                eventBlock.block.electionResults.should.be.an('array');
                eventBlock.block.electionResults.should.have.length.at.least(
                  _twoThirdsMajority(nodes));
                callback();
              }), callback)]
        }, done);
      });
    });
    describe('Block 3', () => {
      it('should achieve consensus with only 7 nodes', function(done) {
        this.timeout(120000);
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEvent.input[0].id = 'https://example.com/events/' + uuid();
        const twoThirdsMajority = peers.slice(
          0, _twoThirdsMajority(peers.length));
        twoThirdsMajority.length.should.equal(_twoThirdsMajority(peers.length));
        async.auto({
          addEvent: callback => genesisLedgerNode.events.add(
            testEvent, callback),
          runWorkers: ['addEvent', (results, callback) => async.each(
            twoThirdsMajority, (ledgerNode, callback) =>
              consensusApi._worker._run(ledgerNode, callback), callback)],
          getLatest: ['runWorkers', (results, callback) =>
            async.each(twoThirdsMajority, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                if(err) {
                  return callback(err);
                }
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResults);
                eventBlock.block.electionResults.should.be.an('array');
                eventBlock.block.electionResults.should.have.length.at.least(
                  _twoThirdsMajority(nodes));
                const electionResults = eventBlock.block.electionResults[0];
                should.exist(electionResults.recommendedElector);
                // electionResults.recommendedElector.map(e => e.id)
                //   .should.have.same.members(recommendedElectorsBlock1.map(
                //     e => e.id));
                callback();
              }), callback)]
        }, done);
      });
    });
    describe('Block 4', () => {
      it('should achieve consensus with 10 nodes again', function(done) {
        this.timeout(120000);
        const testEvent = bedrock.util.clone(mockData.events.alpha);
        testEvent.input[0].id = 'https://example.com/events/' + uuid();
        const trailingPeers = peers.slice(
          _twoThirdsMajority(peers.length));
        trailingPeers.length.should.equal(
          peers.length - _twoThirdsMajority(peers.length));
        async.auto({
          addEvent: callback => genesisLedgerNode.events.add(
            testEvent, callback),
          syncTrailingPeers: callback => async.each(
            trailingPeers, (ledgerNode, callback) =>
              consensusApi._worker._run(ledgerNode, callback), callback),
          runWorkers: ['addEvent', 'syncTrailingPeers', (results, callback) =>
            async.each(peers, (ledgerNode, callback) =>
              consensusApi._worker._run(ledgerNode, callback), callback)],
          getLatest: ['runWorkers', (results, callback) =>
            async.each(peers, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                if(err) {
                  return callback(err);
                }
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResults);
                eventBlock.block.electionResults.should.be.an('array');
                eventBlock.block.electionResults.should.have.length.at.least(
                  _twoThirdsMajority(nodes));
                const electionResults = eventBlock.block.electionResults[0];
                should.exist(electionResults.recommendedElector);
                // electionResults.recommendedElector.map(e => e.id)
                //   .should.have.same.members(recommendedElectorsBlock1.map(
                //     e => e.id));
                callback();
              }), callback)]
        }, done);
      });
    });
  });
});

function _twoThirdsMajority(count) {
  // special case when electors < 3 -- every elector must agree.
  return (count < 3) ? count : Math.floor(count / 3) * 2 + 1;
}
