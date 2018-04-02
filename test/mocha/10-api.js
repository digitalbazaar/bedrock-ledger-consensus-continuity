/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const expect = global.chai.expect;
const hasher = brLedgerNode.consensus._hasher;
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
    const ledgerConfiguration = mockData.ledgerConfiguration;
    async.auto({
      cleanCache: callback => cache.client.flushall(callback),
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      actor: ['clean', (results, callback) => brIdentity.get(
        null, mockIdentity.identity.id, (err, identity) => {
          callback(err, identity);
        })],
      consensusPlugin: callback => brLedgerNode.use('Continuity2017', callback),
      ledgerNode: ['actor', (results, callback) => brLedgerNode.add(
        results.actor, {ledgerConfiguration}, (err, ledgerNode) => {
          if(err) {
            return callback(err);
          }
          expect(ledgerNode).to.be.ok;
          callback(null, ledgerNode);
        })],
      creator: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi = results.consensusPlugin.api;
        ledgerNode = results.ledgerNode;
        consensusApi._voters.get(
          {ledgerNodeId: ledgerNode.id}, (err, result) => {
            if(err) {
              return callback(err);
            }
            creator = result;
            ledgerNode.creatorId = result.id;
            callback(null, result);
          });
      }],
      genesisMerge: ['creator', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          ledgerNode,
          creatorId: results.creator.id
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMergeHash = result.eventHash;
          callback();
        });
      }],
    }, done);
  });

  describe('add operation API', () => {
    it('should add an operation', done => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      async.auto({
        addOp: callback => ledgerNode.operations.add(
          {operation}, err => {
            assertNoError(err);
            callback();
          }),
      }, done);
    });
  }); // end add operation API

  describe('private add event API', () => {
    it('should add a regular local event', done => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      const {creatorId} = ledgerNode;
      async.auto({
        head: callback => ledgerNode.consensus._events._getLocalBranchHead(
          {creatorId, ledgerNode}, (err, result) => {
            if(err) {
              return callback(err);
            }
            const {eventHash: headHash} = result;
            testEvent.parentHash = [headHash];
            testEvent.treeHash = headHash;
            callback();
          }),
        operationHash: callback => hasher(operation, (err, opHash) => {
          if(err) {
            return callback(err);
          }
          testEvent.operationHash = [opHash];
          callback(null, opHash);
        }),
        eventHash: ['head', 'operationHash', (results, callback) => hasher(
          testEvent, callback)],
        operation: ['eventHash', (results, callback) => {
          const {eventHash, operationHash} = results;
          const operations = [{
            meta: {operationHash},
            operation,
          }];
          ledgerNode.consensus.operations.write(
            {eventHash, ledgerNode, operations}, callback);
        }],
        addEvent: ['operation', (results, callback) =>
          ledgerNode.consensus._events.add(
            {event: testEvent, eventHash: results.eventHash, ledgerNode},
            (err, result) => {
              const {eventHash} = results;
              assertNoError(err);
              should.exist(result.event);
              const event = result.event;
              should.exist(event.type);
              event.type.should.equal('WebLedgerOperationEvent');
              should.exist(event.treeHash);
              event.treeHash.should.equal(genesisMergeHash);
              should.exist(result.meta);
              const meta = result.meta;
              should.exist(meta.continuity2017);
              should.exist(meta.continuity2017.creator);
              should.exist(meta.continuity2017.type);
              const {creator: eventCreator, type} = meta.continuity2017;
              eventCreator.should.be.a('string');
              eventCreator.should.equal(creator.id);
              type.should.be.a('string');
              type.should.equal('r');
              should.exist(meta.eventHash);
              meta.eventHash.should.be.a('string');
              meta.eventHash.should.equal(eventHash);
              should.exist(meta.created);
              meta.created.should.be.a('number');
              should.exist(meta.updated);
              meta.updated.should.be.a('number');
              callback();
            })],
        test: ['addEvent', (results, callback) => {
          // get event and test operation
          const {eventHash} = results;
          ledgerNode.storage.events.get(eventHash, (err, result) => {
            assertNoError(err);
            // test event
            should.exist(result.event);
            const {event} = result;
            should.exist(event.type);
            event.type.should.equal('WebLedgerOperationEvent');
            should.exist(event.treeHash);
            event.treeHash.should.equal(genesisMergeHash);
            // test operation
            should.exist(event.operation);
            const {operation: op} = event;
            op.should.be.an('array');
            op.should.have.length(1);
            op[0].should.eql(operation);
            // test meta
            should.exist(result.meta);
            const meta = result.meta;
            should.exist(meta.continuity2017);
            should.exist(meta.continuity2017.creator);
            should.exist(meta.continuity2017.type);
            const {creator: eventCreator, type} = meta.continuity2017;
            eventCreator.should.be.a('string');
            eventCreator.should.equal(creator.id);
            type.should.be.a('string');
            type.should.equal('r');
            should.exist(meta.eventHash);
            meta.eventHash.should.be.a('string');
            meta.eventHash.should.equal(eventHash);
            should.exist(meta.created);
            meta.created.should.be.a('number');
            should.exist(meta.updated);
            meta.updated.should.be.a('number');
            callback();
          });
        }]
      }, done);
    });
  }); // end add event API

  // FIXME: this API chaanged and tests need to be updated accordingly
  describe('getRecentHistory API', () => {
    it('history includes one local event and one local merge event', done => {
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      const eventTemplate = mockData.events.alpha;
      const opTemplate = mockData.operations.alpha;
      async.auto({
        addEvent: callback => helpers.addEventAndMerge(
          {consensusApi, eventTemplate, ledgerNode, opTemplate}, callback),
        history: ['addEvent', (results, callback) => {
          getRecentHistory({
            creatorId: ledgerNode.creatorId,
            ledgerNode
          }, (err, result) => {
            assertNoError(err);
            const mergeEventHash = results.addEvent.mergeHash;
            // inspect eventMap
            // it should include keys for the regular event and merge event
            should.exist(result.eventMap);
            result.eventMap.should.be.an('object');
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(1);
            hashes.should.have.same.members([mergeEventHash]);
            // inspect the regular event
            // it should have the merge event as its only child
            const mergeEvent = result.eventMap[mergeEventHash];
            should.exist(mergeEvent._children);
            mergeEvent._children.should.be.an('array');
            mergeEvent._children.should.have.length(0);
            should.exist(mergeEvent._parents);
            mergeEvent._parents.should.be.an('array');
            mergeEvent._parents.should.have.length(0);
            callback();
          });
        }]
      }, done);
    });
    it('history includes 4 local events and one local merge event', done => {
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      const eventTemplate = mockData.events.alpha;
      const opTemplate = mockData.operations.alpha;
      async.auto({
        addEvent: callback => helpers.addEventAndMerge({
          consensusApi, count: 4, eventTemplate, ledgerNode, opTemplate
        }, callback),
        history: ['addEvent', (results, callback) => {
          getRecentHistory({
            creatorId: ledgerNode.creatorId, ledgerNode
          }, (err, result) => {
            assertNoError(err);
            const mergeEventHash = results.addEvent.merge.meta.eventHash;
            const regularEventHash = Object.keys(results.addEvent.regular);
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(1);
            hashes.should.have.same.members([mergeEventHash]);
            // inspect the merge event
            const mergeEvent = result.eventMap[mergeEventHash];
            should.exist(mergeEvent._children);
            const children = mergeEvent._children;
            children.should.be.an('array');
            children.should.have.length(0);
            should.exist(mergeEvent._parents);
            const parents = mergeEvent._parents;
            parents.should.be.an('array');
            parents.should.have.length(0);
            const {parentHash} = mergeEvent.event;
            parentHash.should.have.same.members(
              [genesisMergeHash, ...regularEventHash]);
            callback();
          });
        }]
      }, done);
    });
    it.skip('history includes 1 remote merge and one local merge event', done => {
      const mergeBranches = consensusApi._worker._events.mergeBranches;
      const getRecentHistory =
        consensusApi._worker._events.getRecentHistory;
      async.auto({
        remoteEvent: callback => helpers.addRemoteEvents(
          {consensusApi, ledgerNode, mockData}, callback),
        history1: ['remoteEvent', (results, callback) =>
          getRecentHistory({ledgerNode, link: true}, callback)],
        mergeBranches: ['history1', (results, callback) =>
          mergeBranches({history: results.history1, ledgerNode}, callback)],
        history2: ['mergeBranches', (results, callback) => {
          getRecentHistory({ledgerNode, link: true}, (err, result) => {
            assertNoError(err);
            const mergeEventHash = results.mergeBranches.meta.eventHash;
            const remoteMergeHash = results.remoteEvent.merge;
            // inspect eventMap
            // it should include keys for the regular event and merge event
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(2);
            hashes.should.have.same.members(
              [mergeEventHash, remoteMergeHash]);
            // inspect the merge event
            // it should have one parent, the remote merge event
            const mergeEvent = result.eventMap[mergeEventHash];
            should.exist(mergeEvent._children);
            let children = mergeEvent._children;
            children.should.be.an('array');
            children.should.have.length(0);
            should.exist(mergeEvent._parents);
            let parents = mergeEvent._parents;
            parents.should.be.an('array');
            parents.should.have.length(1);
            const parentHashes = parents.map(e => e.eventHash);
            parentHashes.should.have.same.members([remoteMergeHash]);
            // inspect remote merge event
            const remoteMergeEvent = result.eventMap[remoteMergeHash];
            should.exist(remoteMergeEvent._children);
            children = remoteMergeEvent._children;
            children.should.be.an('array');
            children.should.have.length(1);
            const child0 = children[0];
            child0.should.be.an('object');
            should.exist(child0.eventHash);
            should.exist(child0.event);
            should.exist(child0.meta.continuity2017);
            child0.eventHash.should.equal(mergeEventHash);
            should.exist(remoteMergeEvent._parents);
            parents = remoteMergeEvent._parents;
            parents.should.be.an('array');
            parents.should.have.length(0);
            callback();
          });
        }]
      }, done);
    });
    it.skip('contains two remote merge and one local merge event', done => {
      const mergeBranches = consensusApi._worker._events.mergeBranches;
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      async.auto({
        // 2 remote merge events from the same creator chained together
        remoteEvent: callback => helpers.addRemoteEvents(
          {consensusApi, count: 2, ledgerNode, mockData}, callback),
        history1: ['remoteEvent', (results, callback) =>
          getRecentHistory({ledgerNode, link: true}, callback)],
        mergeBranches: ['history1', (results, callback) =>
          mergeBranches({history: results.history1, ledgerNode}, callback)],
        history2: ['mergeBranches', (results, callback) => {
          getRecentHistory({ledgerNode, link: true}, (err, result) => {
            assertNoError(err);
            const mergeEventHash = results.mergeBranches.meta.eventHash;
            const remoteMergeHash = results.remoteEvent.map(e => e.merge);
            // inspect eventMap
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(3);
            hashes.should.have.same.members(
              [mergeEventHash, ...remoteMergeHash]);
            // inspect local merge
            const mergeEvent = result.eventMap[mergeEventHash];
            let children = mergeEvent._children;
            children.should.have.length(0);
            let parents = mergeEvent._parents;
            parents.should.have.length(1);
            const parentHashes = parents.map(e => e.eventHash);
            parentHashes.should.have.same.members([remoteMergeHash[1]]);
            // inspect remote merge event 0
            const rme0 = result.eventMap[remoteMergeHash[0]];
            children = rme0._children;
            children.should.have.length(1);
            children[0].eventHash.should.equal(remoteMergeHash[1]);
            rme0._parents.should.have.length(0);
            // inspect remote merge event 1
            const rme1 = result.eventMap[remoteMergeHash[1]];
            children = rme1._children;
            children.should.have.length(1);
            children[0].eventHash.should.equal(mergeEventHash);
            parents = rme1._parents;
            parents.should.have.length(1);
            parents[0].eventHash.should.equal(remoteMergeHash[0]);
            callback();
          });
        }]
      }, done);
    });
    it.skip('contains two remote merge events before a local merge', done => {
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      async.auto({
        // 2 remote merge events from the same creator chained together
        remoteEvent: callback => helpers.addRemoteEvents(
          {consensusApi, count: 2, ledgerNode, mockData}, callback),
        history: ['remoteEvent', (results, callback) => {
          getRecentHistory({ledgerNode, link: true}, (err, result) => {
            assertNoError(err);
            const remoteMergeHash = results.remoteEvent.map(e => e.merge);
            // inspect eventMap
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(2);
            hashes.should.have.same.members(remoteMergeHash);
            // inspect remote merge event 0
            const rme0 = result.eventMap[remoteMergeHash[0]];
            let children = rme0._children;
            children.should.have.length(1);
            children[0].eventHash.should.equal(remoteMergeHash[1]);
            rme0._parents.should.have.length(0);
            // inspect remote merge event 1
            const rme1 = result.eventMap[remoteMergeHash[1]];
            children = rme1._children;
            children.should.have.length(0);
            const parents = rme1._parents;
            parents.should.have.length(1);
            parents[0].eventHash.should.equal(remoteMergeHash[0]);
            callback();
          });
        }]
      }, done);
    });
    it.skip('history includes one local event before local merge', done => {
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.operation[0].record.id = `https://example.com/event/${uuid()}`;
      async.auto({
        addEvent: callback => ledgerNode.consensus._events.add(
          testEvent, ledgerNode, callback),
        history: ['addEvent', (results, callback) => {
          getRecentHistory({ledgerNode, link: true}, (err, result) => {
            assertNoError(err);
            const regularEventHash = results.addEvent.meta.eventHash;
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(1);
            hashes.should.have.same.members([regularEventHash]);
            const regularEvent = result.eventMap[regularEventHash];
            const children = regularEvent._children;
            children.should.have.length(0);
            const parents = regularEvent._parents;
            parents.should.have.length(0);
            callback();
          });
        }]
      }, done);
    });
    it.skip('history includes 4 local events before a local merge', done => {
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      const eventTemplate = mockData.events.alpha;
      async.auto({
        addEvent: callback => helpers.addEvent(
          {ledgerNode, count: 4, eventTemplate}, callback),
        history: ['addEvent', (results, callback) => {
          getRecentHistory({ledgerNode, link: true}, (err, result) => {
            assertNoError(err);
            const regularEventHash = Object.keys(results.addEvent);
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(4);
            hashes.should.have.same.members(regularEventHash);
            // inspect the regular events
            // it should have no parents or children
            regularEventHash.forEach(h => {
              const regularEvent = result.eventMap[h];
              const children = regularEvent._children;
              children.should.have.length(0);
              const parents = regularEvent._parents;
              parents.should.have.length(0);
            });
            callback();
          });
        }]
      }, done);
    });
    it.skip('includes 4 LEs and 2 RMEs before local merge', done => {
      const getRecentHistory = consensusApi._worker._events.getRecentHistory;
      const eventTemplate = mockData.events.alpha;
      async.auto({
        addEvent: callback => helpers.addEvent(
          {ledgerNode, count: 4, eventTemplate}, callback),
        // 2 remote merge events from the same creator chained together
        remoteEvent: callback => helpers.addRemoteEvents(
          {consensusApi, count: 2, ledgerNode, mockData}, callback),
        history: ['addEvent', 'remoteEvent', (results, callback) => {
          getRecentHistory({ledgerNode, link: true}, (err, result) => {
            assertNoError(err);
            const regularEventHash = Object.keys(results.addEvent);
            const remoteMergeHash = results.remoteEvent.map(e => e.merge);
            const hashes = Object.keys(result.eventMap);
            hashes.should.have.length(6);
            hashes.should.have.same.members(
              [...regularEventHash, ...remoteMergeHash]);
            // inspect the regular events
            // it should have no parents or children
            regularEventHash.forEach(h => {
              const regularEvent = result.eventMap[h];
              const children = regularEvent._children;
              children.should.have.length(0);
              const parents = regularEvent._parents;
              parents.should.have.length(0);
            });
            // inspect remote merge event 0
            const rme0 = result.eventMap[remoteMergeHash[0]];
            let children = rme0._children;
            children.should.have.length(1);
            children[0].eventHash.should.equal(remoteMergeHash[1]);
            rme0._parents.should.have.length(0);
            // inspect remote merge event 1
            const rme1 = result.eventMap[remoteMergeHash[1]];
            children = rme1._children;
            children.should.have.length(0);
            const parents = rme1._parents;
            parents.should.have.length(1);
            parents[0].eventHash.should.equal(remoteMergeHash[0]);
            callback();
          });
        }]
      }, done);
    });
  }); // end getRecentHistory API

  describe.skip('Event Consensus', () => {
    it('should add an event and achieve consensus', done => {
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.operation[0].record.id = `https://example.com/event/${uuid()}`;
      async.auto({
        addEvent: callback => ledgerNode.consensus._events.add(
          testEvent, ledgerNode, callback),
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
            event.operation.should.be.an('array');
            event.operation.should.have.length(1);
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
      testEvent.operation[0].record.id = 'https://example.com/events/EXAMPLE';
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
          ledgerNode.conseneus._events.add(testEvent, ledgerNode, callback)],
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
              compactRecord: callback => bedrock.jsonld.compact(
                block.event[0].operation[0],
                block.event[0].operation[0].record['@context'],
                (err, compacted) => callback(err, compacted)),
              compactBlock: ['compactInput', (results, callback) =>
                bedrock.jsonld.compact(
                  block, block['@context'], (err, compacted) => {
                    should.not.exist(err);
                    // use record compacted with its own context
                    compacted.event[0].operation[0].record =
                      results.compactRecord;
                    // remove extra @context entries
                    delete block.event[0]['@context'];
                    block.should.deep.equal(compacted);
                    callback();
                  })]
            }, callback);
          })]
      }, done);
    });
  });
});
