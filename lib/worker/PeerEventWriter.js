/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const logger = require('../logger');

// PeerEventWriter writes peer events, in bulk, to the database
module.exports = class PeerEventWriter {
  constructor({worker}) {
    const {ledgerNode} = worker;
    this.ledgerNode = ledgerNode;
    this.ledgerNodeId = ledgerNode.id;
    this.eventMap = new Map();
    this.operationMap = new Map();
    this.storage = ledgerNode.storage;
    this.worker = worker;
  }

  /**
   * Adds an event received from a peer to be written to persistent storage
   * when this writer is flushed.
   *
   * @param event {Object} - The event to cache.
   * @param meta {Object} - The meta data for the event.
   *
   * @returns {Promise} resolves once the operation completes.
   */
  async add({event, meta} = {}) {
    const {operationMap, eventMap} = this;

    // shallow copy data to enable modification
    const record = {
      event: {...event},
      meta: {...meta}
    };

    // add operation records to in-memory operation map
    if(record.event.type === 'WebLedgerOperationEvent') {
      for(const opRecord of record.event.operationRecords) {
        operationMap.set(opRecord.meta.operationHash, opRecord);
      }
      delete record.event.operationRecords;
    }

    // add to in-memory event map
    eventMap.set(meta.eventHash, {event, meta});
  }

  async flush() {
    const {eventMap, operationMap} = this;
    if(eventMap.size === 0) {
      // nothing to flush
      return;
    }

    try {
      // build unique operations and events to be written
      const operations = [...operationMap.values()];
      const events = [...eventMap.values()];
      const now = Date.now();
      for(const {meta} of events) {
        meta.created = meta.updated = now;
      }
      operationMap.clear();
      eventMap.clear();

      // write operations
      if(operations.length !== 0) {
        logger.debug(`Attempting to store ${operations.length} operations.`);
        await this.storage.operations.addMany({operations});
      }
      // write events
      logger.debug(`Attempting to store ${events.length} events.`);
      // this API will automatically retry on duplicate events until all events
      // have been processed
      // FIXME: use ACID transaction to ensure all events are written or
      // none are
      await this.storage.events.addMany({events});

      // notify worker of new peer events
      await this.worker._addPeerEvents({events});
    } catch(e) {
      logger.error(`Error in event writer: ${this.ledgerNodeId}`, {error: e});
      throw e;
    }
  }
};
