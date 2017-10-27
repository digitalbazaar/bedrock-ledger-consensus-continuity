/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedgerNode = require('bedrock-ledger-node');
const expect = global.chai.expect;
const hasher = brLedgerNode.consensus._hasher;
const helpers = require('./helpers');
const jsigs = require('jsonld-signatures')();
const jsonld = bedrock.jsonld;
const mockData = require('./mock.data');
const uuid = require('uuid/v4');

jsigs.use('jsonld', jsonld);

describe('Continuity2017', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  // get consensus plugin and create ledger node for use in each test
  let consensusApi;
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
          callback();
        });
      }]
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
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(1);
            parentHash.should.have.same.members([eventHash]);
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
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(5);
            parentHash.should.have.same.members(results.addEvent);
            callback();
          });
        }]
      }, done);
    });
    it('collects one remote merge event', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      async.auto({
        remoteEvents: callback => _addRemoteEvents(ledgerNode, callback),
        mergeBranches: ['remoteEvents', (results, callback) => {
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(1);
            parentHash.should.have.same.members([results.remoteEvents.merge]);
            callback();
          });
        }]
      }, done);
    });
    it('collects five remote merge events', done => {
      const mergeBranches = ledgerNode.consensus._worker._events.mergeBranches;
      async.auto({
        remoteEvents: callback => async.times(
          5, (i, callback) => _addRemoteEvents(ledgerNode, callback), callback),
        mergeBranches: ['remoteEvents', (results, callback) => {
          const remoteMergeHashes = results.remoteEvents.map(e => e.merge);
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            should.exist(result.event);
            const event = result.event;
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(5);
            parentHash.should.have.same.members(remoteMergeHashes);
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
          5, (i, callback) => _addRemoteEvents(ledgerNode, callback), callback),
        mergeBranches: ['localEvents', 'remoteEvents', (results, callback) => {
          const remoteMergeHashes = results.remoteEvents.map(e => e.merge);
          mergeBranches(ledgerNode, (err, result) => {
            assertNoError(err);
            const allHashes = results.localEvents.concat(remoteMergeHashes);
            should.exist(result.event);
            const event = result.event;
            should.exist(event.parentHash);
            const parentHash = event.parentHash;
            parentHash.should.be.an('array');
            parentHash.should.have.length(13);
            parentHash.should.have.same.members(allHashes);
            callback();
          });
        }]
      }, done);
    });
  }); // end mergeBranches event API

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

  // add a merge event and regular event as if it came in through gossip
  function _addRemoteEvents(ledgerNode, callback) {
    const testRegularEvent = bedrock.util.clone(mockData.events.alpha);
    testRegularEvent.input[0].id = `https://example.com/event/${uuid()}`;
    const testMergeEvent = bedrock.util.clone(mockData.mergeEvents.alpha);
    // use a valid keypair from mocks
    const keyPair = mockData.groups.authorized;
    // NOTE: using the loal branch head for treeHash of the remote merge event
    const getHead = consensusApi._worker._events._getLocalBranchHead;
    async.auto({
      head: callback => getHead(
        ledgerNode.storage.events.collection, (err, result) => {
          if(err) {
            return callback(err);
          }
          // in this example the merge event and the regular event
          // have a common ancestor which is the genesis merge event
          testMergeEvent.treeHash = result;
          testRegularEvent.treeHash = result;
          callback(null, result);
        }),
      regularEventHash: ['head', (results, callback) =>
        hasher(testRegularEvent, (err, result) => {
          if(err) {
            return callback(err);
          }
          testMergeEvent.parentHash = [result];
          callback(null, result);
        })],
      sign: ['regularEventHash', (results, callback) => jsigs.sign(
        testMergeEvent, {
          algorithm: 'LinkedDataSignature2015',
          privateKeyPem: keyPair.privateKey,
          creator: mockData.authorizedSignerUrl
        }, callback)],
      addMerge: ['sign', (results, callback) => ledgerNode.events.add(
        results.sign, {continuity2017: {peer: true}}, callback)],
      addRegular: ['addMerge', (results, callback) => ledgerNode.events.add(
        testRegularEvent, {continuity2017: {peer: true}}, callback)],
    }, (err, results) => {
      if(err) {
        return callback(err);
      }
      const hashes = {
        merge: results.addMerge.meta.eventHash,
        regular: results.addRegular.meta.eventHash
      };
      callback(null, hashes);
    });
  }
});
