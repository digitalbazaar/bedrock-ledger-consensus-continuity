/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const async = require('async');
const bedrock = require('bedrock');
const config = bedrock.config;
const _events = require('./events');
const _util = require('./util');
const _voters = require('./voters');

const api = {};
module.exports = api;

/**
 * Adds a new event.
 *
 * @param event the event to add.
 * @param ledgerNode the node that is tracking this event.
 * @param options the options to use.
 * @param callback(err, record) called once the operation completes.
 */
api.change = (
  {genesis, genesisBlock, ledgerConfiguration, ledgerNode}, callback) => {
  const event = {
    '@context': config.constants.WEB_LEDGER_CONTEXT_V1_URL,
    type: 'WebLedgerConfigurationEvent',
    ledgerConfiguration
  };
  async.auto({
    // mutates event
    head: callback => {
      if(genesis) {
        return callback();
      }
      _getHead({ledgerNode}, (err, result) => {
        if(err) {
          return callback(err);
        }
        event.parentHash = [result.eventHash];
        event.treeHash = result.eventHash;
        callback();
      });
    },
    eventHash: ['head', (results, callback) => _util.hasher(event, callback)],
    add: ['eventHash', (results, callback) => {
      const {eventHash} = results;
      _events.add(
        {event, eventHash, genesis, genesisBlock, ledgerNode}, callback);
    }]
  }, err => callback(err));
};

function _getHead({ledgerNode}, callback) {
  const ledgerNodeId = ledgerNode.id;
  async.auto({
    creator: callback => _voters.get({ledgerNodeId}, callback),
    head: ['creator', (results, callback) => {
      const creatorId = results.creator.id;
      _events._getLocalBranchHead({creatorId, ledgerNode}, callback);
    }],
  }, (err, results) => err ? callback(err) : callback(null, results.head));
}
