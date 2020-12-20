/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {_client} = require('bedrock-ledger-consensus-continuity');
const {config, util: {BedrockError}} = require('bedrock');

describe('Client API', () => {
  describe('notifyPeer', () => {
    it('throws a NetworkError on connection refused', async () => {
      const callerId = 'foo';
      const peerId = 'https://127.0.0.1';
      let err;
      try {
        await _client.notifyPeer({callerId, peerId});
      } catch(e) {
        err = e;
      }
      should.exist(err);
      err.name.should.equal('NetworkError');
      err.details.should.have.property('callerId');
      err.details.should.have.property('peerId');
      err.should.have.property('cause');
      err.cause.should.have.property('details');
      err.cause.details.should.have.property('address');
      err.cause.details.should.have.property('code');
      err.cause.details.should.have.property('errno');
      err.cause.details.should.have.property('port');
      err.cause.details.code.should.equal('ECONNREFUSED');
    });
  });

  describe('getEvents', () => {
    it('properly handles ECONNREFUSED', async () => {
      let error;
      try {
        await _client.getEvents({
          eventHashes: ['abc'],
          peerId: 'https://127.0.0.1:3333'
        });
      } catch(e) {
        error = e;
      }
      should.exist(error);
      error.should.be.instanceOf(BedrockError);
      error.name.should.equal('NetworkError');
      error.message.should.contain('ECONNREFUSED');
    });
    it('properly handles a 404 error', async () => {
      let error;
      try {
        await _client.getEvents({
          eventHashes: ['abc'],
          peerId: config.server.baseUri,
        });
      } catch(e) {
        error = e;
      }
      should.exist(error);
      error.should.be.instanceOf(BedrockError);
      error.name.should.equal('NetworkError');
      error.details.httpStatusCode.should.equal(404);
    });
  });
});
