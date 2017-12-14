/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
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

describe.only('Multinode', () => {
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

    describe('Check Genesis Block', () => {
      it('should have the proper information', done => async.auto({
        getLatest: callback => async.map(peers, (ledgerNode, callback) =>
          ledgerNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            const eventBlock = result.eventBlock;
            should.exist(eventBlock.block);
            eventBlock.block.blockHeight.should.equal(0);
            eventBlock.block.event.should.be.an('array');
            eventBlock.block.event.should.have.length(1);
            const event = eventBlock.block.event[0];
            // TODO: signature is dynamic... needs a better check
            delete event.signature;
            event.should.deep.equal(configEvent);
            should.exist(eventBlock.meta);
            should.exist(eventBlock.block.consensusProof);
            const consensusProof = eventBlock.block.consensusProof;
            consensusProof.should.be.an('array');
            consensusProof.should.have.length(1);
            // FIXME: make assertions about the contents of consensusProof
            // console.log('8888888', JSON.stringify(eventBlock, null, 2));
            callback(null, eventBlock.meta.blockHash);
          }), callback),
        testHash: ['getLatest', (results, callback) => {
          const blockHashes = results.getLatest;
          blockHashes.every(h => h === blockHashes[0]).should.be.true;
          callback();
        }]
      }, done));
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

      it.only('should add an event and achieve consensus', function(done) {
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
            async.map(peers, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                assertNoError(err);
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.blockHeight.should.equal(1);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResult);
                eventBlock.block.electionResult.should.be.an('array');
                eventBlock.block.electionResult.should.have.length(1);
                const electionResult = eventBlock.block.electionResult[0];
                should.exist(electionResult.recommendedElector);
                electionResult.recommendedElector.map(e => e.id)
                  .should.have.same.members(recommendedElectorsBlock1.map(
                    e => e.id));
                callback(null, eventBlock.meta.blockHash);
              }), callback)],
          testHash: ['getLatest', (results, callback) => {
            const blockHashes = results.getLatest;
            blockHashes.every(h => h === blockHashes[0]).should.be.true;
            callback();
          }]
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
            async.map(peers, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                assertNoError(err);
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.blockHeight.should.equal(2);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResult);
                eventBlock.block.electionResult.should.be.an('array');
                eventBlock.block.electionResult.should.have.length.at.least(
                  _twoThirdsMajority(nodes));
                callback(null, eventBlock.meta.blockHash);
              }), callback)],
          testHash: ['getLatest', (results, callback) => {
            const blockHashes = results.getLatest;
            blockHashes.every(h => h === blockHashes[0]).should.be.true;
            callback();
          }]
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
            async.map(twoThirdsMajority, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                assertNoError(err);
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.blockHeight.should.equal(3);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResult);
                eventBlock.block.electionResult.should.be.an('array');
                eventBlock.block.electionResult.should.have.length.at.least(
                  _twoThirdsMajority(nodes));
                const electionResult = eventBlock.block.electionResult[0];
                should.exist(electionResult.recommendedElector);
                // electionResult.recommendedElector.map(e => e.id)
                //   .should.have.same.members(recommendedElectorsBlock1.map(
                //     e => e.id));
                callback(null, eventBlock.meta.blockHash);
              }), callback)],
          testHash: ['getLatest', (results, callback) => {
            const blockHashes = results.getLatest;
            blockHashes.every(h => h === blockHashes[0]).should.be.true;
            callback();
          }]
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
            async.map(peers, (ledgerNode, callback) =>
              ledgerNode.storage.blocks.getLatest((err, result) => {
                assertNoError(err);
                const eventBlock = result.eventBlock;
                should.exist(eventBlock.block);
                eventBlock.block.blockHeight.should.equal(4);
                eventBlock.block.event.should.be.an('array');
                eventBlock.block.event.should.have.length(1);
                const event = eventBlock.block.event[0];
                event.input.should.be.an('array');
                event.input.should.have.length(1);
                // TODO: signature is dynamic... needs a better check
                delete event.signature;
                event.should.deep.equal(testEvent);
                should.exist(eventBlock.meta);
                should.exist(eventBlock.block.electionResult);
                eventBlock.block.electionResult.should.be.an('array');
                eventBlock.block.electionResult.should.have.length.at.least(
                  _twoThirdsMajority(nodes));
                const electionResult = eventBlock.block.electionResult[0];
                should.exist(electionResult.recommendedElector);
                // electionResult.recommendedElector.map(e => e.id)
                //   .should.have.same.members(recommendedElectorsBlock1.map(
                //     e => e.id));
                callback(null, eventBlock.meta.blockHash);
              }), callback)],
          testHash: ['getLatest', (results, callback) => {
            const blockHashes = results.getLatest;
            blockHashes.every(h => h === blockHashes[0]).should.be.true;
            callback();
          }]
        }, done);
      });
    });
    describe('Block 5 - staggered worker kick-off', () => {
      it('achieves consensus when an event is added at each node',
        function(done) {
          this.timeout(120000);
          const testEvents = [];
          for(let i = 0; i < peers.length; ++i) {
            const testEvent = bedrock.util.clone(mockData.events.alpha);
            testEvent.input[0].id = 'https://example.com/events/' + uuid();
            testEvents.push(testEvent);
          }
          let delay = 0;
          async.auto({
            addEvents: callback =>
              async.eachOf(peers, (ledgerNode, index, callback) =>
                ledgerNode.events.add(testEvents[index], callback), callback),
            runWorkers: ['addEvents', (results, callback) =>
              async.each(peers, (ledgerNode, callback) => {
                setTimeout(() => consensusApi._worker._run(
                  ledgerNode, callback), delay += 250);
              }, callback)],
            getLatest: ['runWorkers', (results, callback) =>
              async.map(peers, (ledgerNode, callback) =>
                ledgerNode.storage.blocks.getLatest((err, result) => {
                  assertNoError(err);
                  const eventBlock = result.eventBlock;
                  should.exist(eventBlock.block);
                  eventBlock.block.blockHeight.should.equal(5);
                  eventBlock.block.event.should.be.an('array');
                  eventBlock.block.event.should.have.length.at.least(1);
                  eventBlock.block.electionResult.should.have.length.at.least(
                    _twoThirdsMajority(nodes));
                  callback(null, eventBlock.meta.blockHash);
                }), callback)],
            testHash: ['getLatest', (results, callback) => {
              const blockHashes = results.getLatest;
              blockHashes.every(h => h === blockHashes[0]).should.be.true;
              callback();
            }]
          }, done);
        });
    });
    describe('Catch-up', () => {
      let catchUpNode;
      before(done => brLedger.add(null, {
        genesisBlock: genesisRecord.block,
        owner: mockIdentity.identity.id
      }, (err, ledgerNode) => {
        if(err) {
          return done(err);
        }
        catchUpNode = ledgerNode;
        done();
      }));

      it('a new node is able to catch up', function(done) {
        this.timeout(120000);
        async.series([
          callback => consensusApi._worker._run(catchUpNode, callback),
          callback => catchUpNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            result.eventBlock.block.blockHeight.should.equal(1);
            result.eventBlock.block.event.should.be.an('array');
            result.eventBlock.block.event.should.have.length(1);
            callback();
          }),
          callback => consensusApi._worker._run(catchUpNode, callback),
          callback => catchUpNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            result.eventBlock.block.blockHeight.should.equal(2);
            result.eventBlock.block.event.should.be.an('array');
            result.eventBlock.block.event.should.have.length(1);
            callback();
          }),
          callback => consensusApi._worker._run(catchUpNode, callback),
          callback => catchUpNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            result.eventBlock.block.blockHeight.should.equal(3);
            result.eventBlock.block.event.should.be.an('array');
            result.eventBlock.block.event.should.have.length(1);
            callback();
          }),
          callback => consensusApi._worker._run(catchUpNode, callback),
          callback => catchUpNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            result.eventBlock.block.blockHeight.should.equal(4);
            result.eventBlock.block.event.should.be.an('array');
            result.eventBlock.block.event.should.have.length(1);
            callback();
          }),
          callback => consensusApi._worker._run(catchUpNode, callback),
          callback => catchUpNode.storage.blocks.getLatest((err, result) => {
            assertNoError(err);
            result.eventBlock.block.blockHeight.should.equal(5);
            result.eventBlock.block.event.should.be.an('array');
            result.eventBlock.block.event.should.have.length.at.least(1);
            callback();
          }),
        ], done);
      });
    });
  });
});

function _twoThirdsMajority(count) {
  // special case when electors < 3 -- every elector must agree.
  return (count < 3) ? count : Math.floor(count / 3) * 2 + 1;
}
