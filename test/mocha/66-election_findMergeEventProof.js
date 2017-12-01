/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const bedrock = require('bedrock');
const brLedgerNode = require('bedrock-ledger-node');
const brTest = require('bedrock-test');
const async = require('async');
const request = brTest.require('request');
const sinon = require('sinon');
const uuid = require('uuid/v4');
const util = require('util');

const helpers = require('./helpers');
const mockData = require('./mock.data');

describe('Election API _findMergeEventProof', () => {
  before(done => {
    helpers.prepareDatabase(mockData, done);
  });
  let consensusApi;
  let genesisMerge;
  let ledgerNode;
  let ledgerNodeBeta;
  let peerId;
  let eventHash;
  let testEventId;
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
          ledgerNode = result;
          callback(null, result);
        })],
      creator: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._voters.get(ledgerNode.id, (err, result) => {
          if(err) {
            return callback(err);
          }
          peerId = result.id;
          callback(null, result);
        });
      }],
      genesisMerge: ['consensusPlugin', 'ledgerNode', (results, callback) => {
        consensusApi._worker._events._getLocalBranchHead({
          eventsCollection: ledgerNode.storage.events.collection,
          creator: peerId
        }, (err, result) => {
          if(err) {
            return callback(err);
          }
          genesisMerge = result;
          callback();
        });
      }],
      addEvent: ['ledgerNode', (results, callback) => ledgerNode.events.add(
        testEvent, (err, result) => {
          eventHash = result.meta.eventHash;
          callback();
        })],
      genesisBlock: ['ledgerNode', (results, callback) =>
        ledgerNode.blocks.getGenesis((err, result) => {
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
          ledgerNodeBeta = result;
          callback(null, result);
        })]
    }, done);
  });
  it('does something', done => {
    done();
  });
});
