/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const expect = global.chai.expect;
const helpers = require('./helpers');
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

describe('Continuity2017', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  // get consensus plugin and create ledger node for use in each test
  let consensusApi;
  let genesisMergeHash;
  let creator;
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
      consensusPlugin: callback => brLedgerNode.use('Continuity2017', callback),
      ledgerNode: ['actor', (results, callback) => brLedgerNode.add(
        results.actor, {configEvent}, (err, ledgerNode) => {
          if(err) {
            return callback(err);
          }
          expect(ledgerNode).to.be.ok;
          callback(null, ledgerNode);
        })],
      creator: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi = results.consensusPlugin.api;
        ledgerNode = results.ledgerNode;
        consensusApi._voters.get(ledgerNode.id, (err, result) => {
          if(err) {
            return callback(err);
          }
          creator = result;
          callback(null, result);
        });
      }],
      genesisMerge: ['creator', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          eventsCollection: ledgerNode.storage.events.collection,
          creator: results.creator.id
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMergeHash = result;
          callback();
        });
      }],
    }, done);
  });

  describe('add event API', () => {
    it('should add a regular local event', done => {
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = `https://example.com/event/${uuid()}`;
      async.auto({
        addEvent: callback => ledgerNode.events.add(
          testEvent, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            should.exist(event.type);
            event.type.should.equal('WebLedgerEvent');
            should.exist(event.operation);
            event.operation.should.equal('Create');
            should.exist(event.input);
            event.input.should.be.an('array');
            event.input.length.should.equal(1);
            event.input.should.deep.equal(testEvent.input);
            should.exist(event.treeHash);
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(result.meta);
            const meta = result.meta;
            should.exist(meta.continuity2017);
            should.exist(meta.continuity2017.creator);
            const eventCreator = meta.continuity2017.creator;
            eventCreator.should.be.a('string');
            eventCreator.should.equal(creator.id);
            should.exist(meta.eventHash);
            meta.eventHash.should.be.a('string');
            should.exist(meta.created);
            meta.created.should.be.a('number');
            should.exist(meta.updated);
            meta.updated.should.be.a('number');
            callback();
          }),
      }, done);
    });
  }); // end add event API

  describe('mergeBranches API', () => {
    it('collects one local event', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = `https://example.com/event/${uuid()}`;
      async.auto({
        addEvent: callback => ledgerNode.events.add(testEvent, callback),
        mergeBranches: ['addEvent', (results, callback) => {
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            const eventHash = results.addEvent.meta.eventHash;
            should.exist(result.event);
            const event = result.event;
            should.exist(event.type);
            event.type.should.be.an('array');
            event.type.should.have.length(2);
            event.type.should.have.same.members(
              ['WebLedgerEvent', 'ContinuityMergeEvent']);
            should.exist(event.treeHash);
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(2);
            parentHash.should.have.same.members([eventHash, event.treeHash]);
            should.exist(result.meta);
            const meta = result.meta;
            should.exist(meta.continuity2017);
            should.exist(meta.continuity2017.creator);
            const eventCreator = meta.continuity2017.creator;
            eventCreator.should.be.a('string');
            eventCreator.should.equal(creator.id);
            should.exist(meta.eventHash);
            meta.eventHash.should.be.a('string');
            should.exist(meta.created);
            meta.created.should.be.a('number');
            should.exist(meta.updated);
            meta.updated.should.be.a('number');
            callback();
          });
        }]
      }, done);
    });
    it('collects five local events', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      const eventTemplate = mockData.events.alpha;
      async.auto({
        events: callback => helpers.createEvent(
          {eventTemplate, eventNum: 5, consensus: false, hash: false},
          callback),
        addEvent: ['events', (results, callback) => async.map(
          results.events, (e, callback) => ledgerNode.events.add(
            e.event, (err, result) => callback(err, result.meta.eventHash)),
          callback)],
        mergeBranches: ['addEvent', (results, callback) => {
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(6);
            parentHash.should.have.same.members(
              results.addEvent.concat(event.treeHash));
            callback();
          });
        }]
      }, done);
    });
    it('collects one remote merge event', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      async.auto({
        remoteEvents: callback => helpers.addRemoteEvents(
          {consensusApi, ledgerNode, mockData}, callback),
        mergeBranches: ['remoteEvents', (results, callback) => {
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            should.exist(event.treeHash);
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(2);
            parentHash.should.have.same.members([
              results.remoteEvents.merge, event.treeHash]);
            callback();
          });
        }]
      }, done);
    });
    it('collects five remote merge events', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      async.auto({
        remoteEvents: callback => async.times(
          5, (i, callback) => helpers.addRemoteEvents(
            {consensusApi, ledgerNode, mockData}, callback), callback),
        mergeBranches: ['remoteEvents', (results, callback) => {
          const remoteMergeHashes = results.remoteEvents.map(e => e.merge);
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(6);
            parentHash.should.have.same.members(
              remoteMergeHashes.concat(event.treeHash));
            callback();
          });
        }]
      }, done);
    });
    it('collects five remote merge events and eight local events', done => {
      const eventTemplate = mockData.events.alpha;
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      async.auto({
        events: callback => helpers.createEvent(
          {eventTemplate, eventNum: 8, consensus: false, hash: false},
          callback),
        localEvents: ['events', (results, callback) => async.map(
          results.events, (e, callback) => ledgerNode.events.add(
            e.event, (err, result) => callback(err, result.meta.eventHash)),
          callback)],
        remoteEvents: callback => async.times(
          5, (i, callback) => helpers.addRemoteEvents(
            {consensusApi, ledgerNode, mockData}, callback), callback),
        mergeBranches: ['localEvents', 'remoteEvents', (results, callback) => {
          const remoteMergeHashes = results.remoteEvents.map(e => e.merge);
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            event.treeHash.should.equal(genesisMergeHash);
            const allHashes = results.localEvents.concat(
              remoteMergeHashes, event.treeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(14);
            parentHash.should.have.same.members(allHashes);
            callback();
          });
        }]
      }, done);
    });
    // NOTE: no assertions can be made about the events refrenced in the merge
    // events because there is no intervening consensus work occuring
    it('Second merge event has the proper treeHash', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      const testEvent2 = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = `https://example.com/event/${uuid()}`;
      testEvent2.input[0].id = `https://example.com/event/${uuid()}`;
      async.auto({
        addEvent: callback => ledgerNode.events.add(testEvent, callback),
        mergeBranches: ['addEvent', (results, callback) => {
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            const event = result.event;
            event.treeHash.should.equal(genesisMergeHash);
            callback(null, event);
          });
        }],
        addEvent2: ['mergeBranches', (results, callback) =>
          ledgerNode.events.add(testEvent2, callback)],
        // hashing the merge event here because the storage API does not return
        // meta.eventHash
        mergeEventHash: ['mergeBranches', (results, callback) =>
          helpers.testHasher(results.mergeBranches, callback)],
        mergeBranches2: ['mergeEventHash', 'addEvent2', (results, callback) => {
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            const event = result.event;
            event.treeHash.should.equal(results.mergeEventHash);

            callback();
          });
        }],
      }, done);
    });
  }); // end mergeBranches event API

  describe('getRecentHistory API', () => {
    it('history includes one local event', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      const getRecentHistory =
        ledgerNode.consensus._worker._events.getRecentHistory;
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = `https://example.com/event/${uuid()}`;
      async.auto({
        addEvent: callback => ledgerNode.events.add(testEvent, callback),
        mergeBranches: ['addEvent', (results, callback) => {
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            const eventHash = results.addEvent.meta.eventHash;
            should.exist(result.event);
            const event = result.event;
            callback();
          });
        }],
        history: ['mergeBranches', (results, callback) => {
          getRecentHistory({ledgerNode}, (err, result) => {
            assertNoError(err);
            console.log('TTTTTTT', JSON.stringify(result, null, 2));
            callback();
          });
        }]
      }, done);
    });
    it('contains five remote merge events and eight local events', done => {
      const eventTemplate = mockData.events.alpha;
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      const getRecentHistory =
        ledgerNode.consensus._worker._events.getRecentHistory;
      async.auto({
        events: callback => helpers.createEvent(
          {eventTemplate, eventNum: 8, consensus: false, hash: false},
          callback),
        localEvents: ['events', (results, callback) => async.map(
          results.events, (e, callback) => ledgerNode.events.add(
            e.event, (err, result) => callback(err, result.meta.eventHash)),
          callback)],
        remoteEvents: callback => async.times(
          5, (i, callback) => helpers.addRemoteEvents(
            {consensusApi, ledgerNode, mockData}, callback), callback),
        mergeBranches: ['localEvents', 'remoteEvents', (results, callback) => {
          const remoteMergeHashes = results.remoteEvents.map(e => e.merge);
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            callback();
          });
        }],
        history: ['mergeBranches', (results, callback) => {
          getRecentHistory({ledgerNode}, (err, result) => {
            assertNoError(err);
            console.log('YYYYYY', JSON.stringify(result, null, 2));
            callback();
          });
        }]
      }, done);
    });
  }); // end getRecentHistory API

  describe.skip('Event Consensus', () => {
    it('should add an event and achieve consensus', done => {
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = `https://example.com/event/${uuid()}`;
      async.auto({
        addEvent: callback => ledgerNode.events.add(testEvent, callback),
        runWorker: ['addEvent', (results, callback) =>
          consensusApi._worker._run(ledgerNode, err => {
            callback(err);
          })],
        getLatest: ['runWorker', (results, callback) =>
          ledgerNode.storage.blocks.getLatest((err, result) => {
            should.not.exist(err);
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
            callback();
          })]
      }, done);
    });

    it('should ensure the blocks round-trip expand/compact properly', done => {
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = 'https://example.com/events/EXAMPLE';
      async.auto({
        getConfigBlock: callback =>
          ledgerNode.storage.blocks.getLatest((err, result) => {
            should.not.exist(err);
            const eventBlock = result.eventBlock;
            should.exist(eventBlock.block);
            const block = eventBlock.block;
            bedrock.jsonld.compact(
              block, block['@context'], (err, compacted) => {
                should.not.exist(err);
                delete block.event[0]['@context'];
                delete block.electionResult[0]['@context'];
                block.should.deep.equal(compacted);
                callback();
              });
          }),
        addEvent: ['getConfigBlock', (results, callback) =>
          ledgerNode.events.add(testEvent, callback)],
        runWorker: ['addEvent', (results, callback) =>
          consensusApi._worker._run(ledgerNode, err => {
            callback(err);
          })],
        getEventBlock: ['runWorker', (results, callback) =>
          ledgerNode.storage.blocks.getLatest((err, result) => {
            should.not.exist(err);
            const eventBlock = result.eventBlock;
            should.exist(eventBlock.block);
            const block = eventBlock.block;
            async.auto({
              compactInput: callback => bedrock.jsonld.compact(
                block.event[0].input[0], block.event[0].input[0]['@context'],
                (err, compacted) => callback(err, compacted)),
              compactBlock: ['compactInput', (results, callback) =>
                bedrock.jsonld.compact(
                  block, block['@context'], (err, compacted) => {
                    should.not.exist(err);
                    // use input compacted with its own context
                    compacted.event[0].input[0] = results.compactInput;
                    // remove extra @context entries
                    delete block.event[0]['@context'];
                    delete block.electionResult[0]['@context'];
                    block.should.deep.equal(compacted);
                    callback();
                  })]
            }, callback);
          })]
      }, done);
    });
  });
});
