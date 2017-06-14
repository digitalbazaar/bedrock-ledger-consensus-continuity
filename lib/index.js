/*
 * Web Ledger Continuity Consensus module.
 *
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
require('bedrock');

// load config defaults
require('./config');

// module API
const api = {};
module.exports = api;

// TODO: move this to bedrock-ledger.consensus:
// TODO: note that this likely needs to be implemented at the
// storage layer, so it would be exposed on the storage API
brLedgerStorage.consensus.process = (method, session, fn, callback) => {
  // loop until all eligible ledgers have been addressed
  let done = false;
  async.until(() => done, loopCallback => {
    async.auto({
      reserve: (results, callback) =>
        // reserve a ledger to perform work on...
        //   this will add a consensus session to a single eligible ledger,
        //   where eligibility depends on a matching consensus method, non-
        //   deleted status, and no existing matching session or session
        //   of the same type
        consensus.reserve(method, session, callback),
      reconcile: ['reserve', (results, callback) => {
        const ledgerId = results.reserve;
        if(!ledgerId) {
          // no ledgers left or session expired
          done = true;
          return loopCallback();
        }
        fn(ledgerId, session, callback);
      }],
      release: ['reconcile', (results, callback) => {
        // releasing allows other work sessions of the same type on the ledger,
        // but not an exact match; if release is not called for any reason,
        // then other sessions of the same type will be prohibited from working
        // on the ledger until after its expiry period
        consensus.release(results.reserve, session, callback);
      }]
    }, err => loopCallback(err));
  }, err => callback(err));
};
