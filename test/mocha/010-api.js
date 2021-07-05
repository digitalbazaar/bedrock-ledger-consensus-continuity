/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const {callbackify} = require('util');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const cache = require('bedrock-redis');
const expect = global.chai.expect;
const hasher = brLedgerNode.consensus._hasher;
const helpers = require('./helpers');
const jsonld = require('jsonld');
const mockData = require('./mock.data');
const {util: {uuid}} = bedrock;

describe('Continuity2017', () => {
  before(async () => {
    await helpers.prepareDatabase();
  });
  // get consensus plugin and create ledger node for use in each test
  let consensusApi;
  let genesisMergeHash;
  let creator;
  let ledgerNode;
  let Worker;
  beforeEach(async function() {
    const ledgerConfiguration = mockData.ledgerConfiguration;
    // start by flushing redis
    await cache.client.flushall();
    // remove the collections so we have fresh test data
    await helpers.removeCollections(['ledger', 'ledgerNode']);
    // get the consensusPlugin that registered by this library
    const consensusPlugin = brLedgerNode.use('Continuity2017');
    ledgerNode = await brLedgerNode.add(null, {ledgerConfiguration});
    expect(ledgerNode, 'Expected ledgerNode to be ok').to.be.ok;
    // the consensusApi is defined by this library
    consensusApi = consensusPlugin.api;
    Worker = consensusApi._worker.Worker;
    // attach worker to the node to emulate a work session used by `helpers`
    ledgerNode.worker = new Worker({session: {ledgerNode}});

    await ledgerNode.worker.init();
    ledgerNode.peerId = await consensusApi._localPeers.getPeerId(
      {ledgerNodeId: ledgerNode.id});
    creator = {id: ledgerNode.peerId};
    genesisMergeHash = ledgerNode.worker.head.eventHash;
  });

  describe('add operation API', () => {
    it('should add an operation', async () => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      operation.creator = ledgerNode.peerId;
      let error;
      try {
        await ledgerNode.operations.add({operation});
      } catch(e) {
        error = e;
      }
      assertNoError(error);
    });
    it('DuplicateError on a duplicate operation in cache', async () => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      operation.creator = ledgerNode.peerId;
      let error;
      try {
        await ledgerNode.operations.add({operation});
      } catch(e) {
        error = e;
      }
      // first operation succeeds
      assertNoError(error);
      try {
        await ledgerNode.operations.add({operation});
      } catch(e) {
        error = e;
      }
      // duplicate operation should fail
      should.exist(error);
      error.name.should.equal('DuplicateError');
      should.exist(error.details.duplicateLocation);
      error.details.duplicateLocation.should.equal('cache');
    });
    it('DuplicateError on a duplicate operation in db', async () => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      operation.creator = ledgerNode.peerId;
      let error;
      try {
        await ledgerNode.operations.add({operation});
      } catch(e) {
        error = e;
      }
      // first operation succeeds
      await ledgerNode.consensus._events.create(
        {ledgerNode, worker: ledgerNode.worker});
      assertNoError(error);
      try {
        await ledgerNode.operations.add({operation});
      } catch(e) {
        error = e;
      }
      // duplicate operation should fail
      should.exist(error);
      error.name.should.equal('DuplicateError');
      should.exist(error.details.duplicateLocation);
      error.details.duplicateLocation.should.equal('db');
    });
    it('ValidationError on operation without creator', async () => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      // missing operation.creator
      let error;
      try {
        await ledgerNode.operations.add({operation});
      } catch(e) {
        error = e;
      }
      should.exist(error);
      error.name.should.equal('ValidationError');
      error.details.errors[0].message.should.equal(
        `should have required property 'creator'`);
    });
    it('ValidationError an operation with incorrect creator', async () => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      operation.creator = 'SomeInvalidPeerId';
      let error;
      try {
        await ledgerNode.operations.add({operation});
      } catch(e) {
        error = e;
      }
      should.exist(error);
      error.name.should.equal('ValidationError');
      error.details.errors[0].message.should.equal(
        'should be equal to one of the allowed values');
      error.details.errors[0].details.path.should.equal('.creator');
    });
  }); // end add operation API

  describe('private add event API', () => {
    it('should add a regular local event', async () => {
      const operation = bedrock.util.clone(mockData.operations.alpha);
      operation.record.id = `https://example.com/event/${uuid()}`;
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      const {eventHash: headHash} = ledgerNode.worker.head;
      testEvent.parentHash = [headHash];
      testEvent.treeHash = headHash;

      const operationHash = await hasher(operation);
      testEvent.operationHash = [operationHash];

      const eventHash = await hasher(testEvent);
      const operations = [{
        meta: {operationHash},
        operation,
      }];
      await ledgerNode.consensus.operations.write(
        {eventHash, ledgerNode, operations});

      let result = await ledgerNode.consensus._events.add(
        {event: testEvent, eventHash, ledgerNode, worker: ledgerNode.worker});
      should.exist(result.event);
      let event = result.event;
      should.exist(event.type);
      event.type.should.equal('WebLedgerOperationEvent');
      should.exist(event.treeHash);
      event.treeHash.should.equal(genesisMergeHash);
      should.exist(result.meta);
      let meta = result.meta;
      should.exist(meta.continuity2017);
      should.exist(meta.continuity2017.creator);
      should.exist(meta.continuity2017.type);
      let {creator: eventCreator, type} = meta.continuity2017;
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

      // get event and test operation
      result = await ledgerNode.storage.events.get(eventHash);
      // test event
      should.exist(result.event);
      event = result.event;
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
      meta = result.meta;
      should.exist(meta.continuity2017);
      should.exist(meta.continuity2017.creator);
      should.exist(meta.continuity2017.type);
      ({creator: eventCreator, type} = meta.continuity2017);
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
    });
  }); // end add event API

  // FIXME: this API changed and tests need to be updated accordingly
  describe('getRecentHistory API', () => {
    it('history includes one local event and one local merge event', done => {
      const eventTemplate = mockData.events.alpha;
      const opTemplate = mockData.operations.alpha;
      async.auto({
        addEvent: callback => callbackify(helpers.addEventAndMerge)(
          {eventTemplate, ledgerNode, opTemplate}, callback),
        history: ['addEvent', (results, callback) => {
          const result = ledgerNode.worker.getRecentHistory();
          const mergeEventHash = results.addEvent.mergeHash;
          // inspect events
          // it should include keys for the merge event
          should.exist(result.events);
          result.events.should.be.an('array');
          result.events.should.have.length(1);
          should.exist(result.events[0].eventHash);
          result.events[0].eventHash.should.equal(mergeEventHash);
          callback();
        }]
      }, done);
    });
    it('history includes 4 local events and one local merge event', done => {
      const eventTemplate = mockData.events.alpha;
      const opTemplate = mockData.operations.alpha;
      async.auto({
        addEvent: callback => callbackify(helpers.addEventAndMerge)({
          count: 4, eventTemplate, ledgerNode, opTemplate
        }, callback),
        history: ['addEvent', (results, callback) => {
          const result = ledgerNode.worker.getRecentHistory();
          const mergeEventHash = results.addEvent.merge.meta.eventHash;
          const regularEventHash = Object.keys(results.addEvent.regular);
          const hashes = result.events.map(e => e.eventHash);
          hashes.should.have.length(1);
          hashes.should.have.same.members([mergeEventHash]);
          // inspect the merge event
          const mergeEvent = result.events[0];
          should.exist(mergeEvent);
          const {parentHash} = mergeEvent.event;
          parentHash.should.have.same.members(
            [genesisMergeHash, ...regularEventHash]);
          callback();
        }]
      }, done);
    });
    it.skip('history includes 1 remote merge and 1 local merge event', done => {
      const mergeBranches = callbackify(consensusApi._events.mergeBranches);
      const getRecentHistory = callbackify(consensusApi._history.getRecent);
      async.auto({
        remoteEvent: callback => callbackify(helpers.addRemoteEvents)(
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
            should.exist(mergeEvent._c.parents);
            let parents = mergeEvent._c.parents;
            parents.should.be.an('array');
            parents.should.have.length(1);
            const parentHashes = parents.map(e => e.eventHash);
            parentHashes.should.have.same.members([remoteMergeHash]);
            // inspect remote merge event
            const remoteMergeEvent = result.eventMap[remoteMergeHash];
            should.exist(remoteMergeEvent._c.parents);
            parents = remoteMergeEvent._c.parents;
            parents.should.be.an('array');
            parents.should.have.length(0);
            callback();
          });
        }]
      }, done);
    });
    it.skip('contains two remote merge and one local merge event', done => {
      const mergeBranches = callbackify(consensusApi._events.mergeBranches);
      const getRecentHistory = callbackify(consensusApi._history.getRecent);
      async.auto({
        // 2 remote merge events from the same creator chained together
        remoteEvent: callback => callbackify(helpers.addRemoteEvents)(
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
            let parents = mergeEvent._c.parents;
            parents.should.have.length(1);
            const parentHashes = parents.map(e => e.eventHash);
            parentHashes.should.have.same.members([remoteMergeHash[1]]);
            // inspect remote merge event 0
            const rme0 = result.eventMap[remoteMergeHash[0]];
            rme0._c.parents.should.have.length(0);
            // inspect remote merge event 1
            const rme1 = result.eventMap[remoteMergeHash[1]];
            parents = rme1._c.parents;
            parents.should.have.length(1);
            parents[0].eventHash.should.equal(remoteMergeHash[0]);
            callback();
          });
        }]
      }, done);
    });
    it.skip('contains two remote merge events before a local merge', done => {
      const getRecentHistory = callbackify(consensusApi._history.getRecent);
      async.auto({
        // 2 remote merge events from the same creator chained together
        remoteEvent: callback => callbackify(helpers.addRemoteEvents)(
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
            rme0._c.parents.should.have.length(0);
            // inspect remote merge event 1
            const rme1 = result.eventMap[remoteMergeHash[1]];
            const parents = rme1._c.parents;
            parents.should.have.length(1);
            parents[0].eventHash.should.equal(remoteMergeHash[0]);
            callback();
          });
        }]
      }, done);
    });
    it.skip('history includes one local event before local merge', done => {
      const getRecentHistory = callbackify(consensusApi._history.getRecent);
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
            const parents = regularEvent._c.parents;
            parents.should.have.length(0);
            callback();
          });
        }]
      }, done);
    });
    it.skip('history includes 4 local events before a local merge', done => {
      const getRecentHistory = callbackify(consensusApi._history.getRecent);
      const eventTemplate = mockData.events.alpha;
      async.auto({
        addEvent: callback => callbackify(helpers.addEvent)(
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
              const parents = regularEvent._c.parents;
              parents.should.have.length(0);
            });
            callback();
          });
        }]
      }, done);
    });
    it.skip('includes 4 LEs and 2 RMEs before local merge', done => {
      const getRecentHistory = callbackify(consensusApi._history.getRecent);
      const eventTemplate = mockData.events.alpha;
      async.auto({
        addEvent: callback => callbackify(helpers.addEvent)(
          {ledgerNode, count: 4, eventTemplate}, callback),
        // 2 remote merge events from the same creator chained together
        remoteEvent: callback => callbackify(helpers.addRemoteEvents)(
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
              const parents = regularEvent._c.parents;
              parents.should.have.length(0);
            });
            // inspect remote merge event 0
            const rme0 = result.eventMap[remoteMergeHash[0]];
            rme0._c.parents.should.have.length(0);
            // inspect remote merge event 1
            const rme1 = result.eventMap[remoteMergeHash[1]];
            const parents = rme1._c.parents;
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
          callbackify(consensusApi._worker._run)({ledgerNode}, err => {
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
            jsonld.compact(
              block, block['@context'], (err, compacted) => {
                should.not.exist(err);
                delete block.event[0]['@context'];
                delete block.electionResult[0]['@context'];
                block.should.deep.equal(compacted);
                callback();
              });
          }),
        addEvent: ['getConfigBlock', (results, callback) =>
          ledgerNode.consensus._events.add(testEvent, ledgerNode, callback)],
        runWorker: ['addEvent', (results, callback) =>
          callbackify(consensusApi._worker._run)({ledgerNode}, err => {
            callback(err);
          })],
        getEventBlock: ['runWorker', (results, callback) =>
          callbackify(ledgerNode.storage.blocks.getLatest)((err, result) => {
            should.not.exist(err);
            const eventBlock = result.eventBlock;
            should.exist(eventBlock.block);
            const block = eventBlock.block;
            async.auto({
              compactRecord: callback => callbackify(bedrock.jsonld.compact)(
                block.event[0].operation[0],
                block.event[0].operation[0].record['@context'],
                (err, compacted) => callback(err, compacted)),
              compactBlock: ['compactInput', (results, callback) =>
                callbackify(jsonld.compact)(
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
