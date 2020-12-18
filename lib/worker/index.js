/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const bedrock = require('bedrock');
const logger = require('../logger');
const Worker = require('./Worker');

// load config defaults
require('../config');

// module API
const api = {};
module.exports = api;

// exposed for testing
api.Worker = Worker;

// temporary hack to access/update ledger node meta
const _ledgerNodeMeta = require('../temporaryLedgerNodeMeta');

api.scheduleWork = async ({session}) => {
  // start a consensus session for ledgers
  const maxAge =
    bedrock.config['ledger-consensus-continuity'].worker.session.maxTime;
  return session.start({fn: _guardedRun, maxAge});
};

// Note: exposed for testing
api._run = async ({ledgerNode, targetCycles = 1}) => {
  const worker = new Worker({
    session: {ledgerNode},
    halt() {
      // don't halt before pipeline is run once; this is safe when
      // `runPipelineOnlyOnce` is set and there is no ledger work session
      // scheduler being used that might concurrently schedule another session,
      // which is true in tests
      return false;
    }
  });
  // force pipeline to run a certain number of times
  return worker.run({targetCycles});
};

async function _guardedRun(session) {
  // do not allow worker to run until `waitUntil` time
  const meta = await _ledgerNodeMeta.get(session.ledgerNode.id);
  // FIXME: remove `_.get` to eliminate need for lodash
  const waitUntil = _.get(meta, 'consensus-continuity.waitUntil');
  if(waitUntil && waitUntil > Date.now()) {
    // do not run consensus yet
    logger.verbose('consensus job delaying until ' + new Date(waitUntil),
      {ledgerNodeId: session.ledgerNode.id, waitUntil});
    return;
  }
  // ready to run pipeline
  const worker = new Worker({session});
  return worker.run();
}
