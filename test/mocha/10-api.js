/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brIdentity = require('bedrock-identity');
const brLedger = require('bedrock-ledger-node');
const async = require('async');
const expect = global.chai.expect;
const uuid = require('uuid/v4');

const helpers = require('./helpers');
const mockData = require('./mock.data');

describe('Continuity2017', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
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
        results.actor, {configEvent}, (err, ledgerNode) => {
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

  describe('add event API', () => {
    it('should add a regular local event', done => {
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = uuid();
      async.auto({
        addEvent: callback => ledgerNode.events.add(testEvent, callback),
      }, done);
    });
  }); // end add event API

  describe.skip('Event Consensus', () => {
    it('should add an event and achieve consensus', done => {
      const testEvent = bedrock.util.clone(mockData.events.alpha);
      testEvent.input[0].id = uuid();
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
