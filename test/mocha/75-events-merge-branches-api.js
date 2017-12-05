/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const helpers = require('./helpers');
const mockData = require('./mock.data');
const util = require('util');
const uuid = require('uuid/v4');

let consensusApi;

describe.only('Continuity2017 events.mergeBranches API', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let genesisMergeHash;
  let eventHash;
  let testEventId;
  const nodes = {};
  const peers = {};
  beforeEach(done => {
    const configEvent = mockData.events.config;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEventId = 'https://example.com/events/' + uuid();
    testEvent.input[0].id = testEventId;
    async.auto({
      clean: callback =>
        helpers.removeCollections(['ledger', 'ledgerNode'], callback),
      consensusPlugin: callback =>
        brLedgerNode.use('Continuity2017', (err, result) => {
          if(err) {
            return callback(err);
          }
          consensusApi = result.api;
          callback();
        }),
      ledgerNode: ['clean', (results, callback) => brLedgerNode.add(
        null, {configEvent}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.alpha = result;
          callback(null, result);
        })],
      genesisMerge: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          eventsCollection: nodes.alpha.storage.events.collection
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMergeHash = result;
          callback();
        });
      }],
      genesisBlock: ['ledgerNode', (results, callback) =>
        nodes.alpha.blocks.getGenesis((err, result) => {
          if(err) {
            return callback(err);
          }
          callback(null, result.genesisBlock.block);
        })],
      nodeBeta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.beta = result;
          callback(null, result);
        })],
      nodeGamma: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.gamma = result;
          callback(null, result);
        })],
      nodeDelta: ['genesisBlock', (results, callback) => brLedgerNode.add(
        null, {genesisBlock: results.genesisBlock}, (err, result) => {
          if(err) {
            return callback(err);
          }
          nodes.delta = result;
          callback(null, result);
        })],
      // nodeEpsilon: ['genesisBlock', (results, callback) => brLedgerNode.add(
      //   null, {genesisBlock: results.genesisBlock}, (err, result) => {
      //     if(err) {
      //       return callback(err);
      //     }
      //     nodes.epsilon = result;
      //     callback(null, result);
      //   })],
      creator: ['nodeDelta', (results, callback) =>
        async.eachOf(nodes, (n, i, callback) =>
          consensusApi._worker._voters.get(n.id, (err, result) => {
            if(err) {
              return callback(err);
            }
            peers[i] = result.id;
            callback();
          }), callback)]
    }, done);
  });

  it.only('collects one local event', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEvent.input[0].id = `https://example.com/event/${uuid()}`;
    const ledgerNode = nodes.alpha;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      mergeBranches: ['addEvent', (results, callback) => {
        mergeBranches({ledgerNode}, (err, result) => {
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
          eventCreator.should.equal(peers.alpha);
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
  it.only('parentHash includes only prior merge event hash', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    testEvent.input[0].id = `https://example.com/event/${uuid()}`;
    const ledgerNode = nodes.alpha;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      mergeBranches1: ['addEvent', (results, callback) =>
        mergeBranches({ledgerNode}, callback)],
      mergeBranches2: ['mergeBranches1', (results, callback) => {
        mergeBranches({ledgerNode}, (err, result) => {
          assertNoError(err);
          const firstMergeHash = results.mergeBranches1.meta.eventHash;
          should.exist(result.event);
          const event = result.event;
          should.exist(event.treeHash);
          event.treeHash.should.equal(firstMergeHash);
          event.parentHash.should.have.same.members([firstMergeHash]);
          callback();
        });
      }]
    }, done);
  });
  it.only('collects five local events', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const eventTemplate = mockData.events.alpha;
    const ledgerNode = nodes.alpha;
    async.auto({
      addEvent: callback => helpers.addEvent(
        {eventTemplate, count: 5, ledgerNode}, callback),
      mergeBranches: ['addEvent', (results, callback) => {
        mergeBranches({ledgerNode}, (err, result) => {
          assertNoError(err);
          should.exist(result.event);
          const event = result.event;
          event.treeHash.should.equal(genesisMergeHash);
          should.exist(event.parentHash);
          const parentHash = event.parentHash;
          parentHash.should.be.an('array');
          parentHash.should.have.length(6);
          const regularEventHash = Object.keys(results.addEvent);
          parentHash.should.have.same.members(
            [event.treeHash, ...regularEventHash]);
          callback();
        });
      }]
    }, done);
  });
  it.only('collects one remote merge event', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    async.auto({
      eventsBeta: callback => helpers.addRemoteEvents(
        {consensusApi, ledgerNode: nodes.beta, mockData}, callback),
      mergeBeta: ['eventsBeta', (results, callback) => {
        mergeBranches({ledgerNode: nodes.beta}, callback);
      }],
      mergeBranches: ['mergeBeta', (results, callback) => {
        helpers.copyAndMerge(
          {consensusApi, from: nodes.beta, to: nodes.alpha}, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            should.exist(event.treeHash);
            event.treeHash.should.equal(genesisMergeHash);
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(2);
            const betaMergeHash = results.mergeBeta.meta.eventHash;
            parentHash.should.have.same.members(
              [betaMergeHash, event.treeHash]);
            callback();
          });
      }]
    }, done);
  });
  it('collects one remote merge events', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    async.auto({
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, count: 5, ledgerNode, mockData}, callback),
      mergeBranches: ['remoteEvents', (results, callback) => {
        const remoteMergeHashes = results.remoteEvents.map(e => e.merge);
        mergeBranches({ledgerNode}, (err, result) => {
          assertNoError(err);
          should.exist(result.event);
          const event = result.event;
          event.treeHash.should.equal(genesisMergeHash);
          should.exist(event.parentHash);
          const parentHash = event.parentHash;
          parentHash.should.be.an('array');
          parentHash.should.have.length(2);
          parentHash.should.have.same.members(
            [event.treeHash, remoteMergeHashes.pop()]);
          callback();
        });
      }]
    }, done);
  });
  it('collects one remote merge events and eight local events', done => {
    const eventTemplate = mockData.events.alpha;
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    async.auto({
      events: callback => helpers.createEvent(
        {eventTemplate, eventNum: 8, consensus: false, hash: false},
        callback),
      localEvents: ['events', (results, callback) => async.map(
        results.events, (e, callback) => ledgerNode.events.add(
          e.event, (err, result) => callback(err, result.meta.eventHash)),
        callback)],
      remoteEvents: callback => helpers.addRemoteEvents(
        {consensusApi, count: 5, ledgerNode, mockData}, callback),
      mergeBranches: ['localEvents', 'remoteEvents', (results, callback) => {
        const remoteMergeHashes = results.remoteEvents.map(e => e.merge);
        mergeBranches({ledgerNode}, (err, result) => {
          assertNoError(err);
          should.exist(result.event);
          const event = result.event;
          event.treeHash.should.equal(genesisMergeHash);
          const allHashes = results.localEvents.concat(
            remoteMergeHashes.pop(), event.treeHash);
          should.exist(event.parentHash);
          const parentHash = event.parentHash;
          parentHash.should.be.an('array');
          parentHash.should.have.length(10);
          parentHash.should.have.same.members(allHashes);
          callback();
        });
      }]
    }, done);
  });
  // NOTE: no assertions can be made about the events refrenced in the merge
  // events because there is no intervening consensus work occuring
  it('Second merge event has the proper treeHash', done => {
    const mergeBranches = consensusApi._worker._events.mergeBranches;
    const testEvent = bedrock.util.clone(mockData.events.alpha);
    const testEvent2 = bedrock.util.clone(mockData.events.alpha);
    testEvent.input[0].id = `https://example.com/event/${uuid()}`;
    testEvent2.input[0].id = `https://example.com/event/${uuid()}`;
    async.auto({
      addEvent: callback => ledgerNode.events.add(testEvent, callback),
      mergeBranches: ['addEvent', (results, callback) => {
        mergeBranches({ledgerNode}, (err, result) => {
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
        mergeBranches({ledgerNode}, (err, result) => {
          assertNoError(err);
          const event = result.event;
          event.treeHash.should.equal(results.mergeEventHash);

          callback();
        });
      }],
    }, done);
  });
});
