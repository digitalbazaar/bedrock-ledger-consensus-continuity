/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {_client} = require('bedrock-ledger-consensus-continuity');
const {config, util: {BedrockError}} = require('bedrock');

describe('Client API', () => {
  describe('notifyPeer', () => {
    it('throws a NetworkError on connection refused', async () => {
      const localPeerId = 'foo';
      const remotePeer = {
        id: 'https://127.0.0.1',
        url: 'https://127.0.0.1'
      };
      let err;
      try {
        await _client.notifyPeer({localPeerId, remotePeer});
      } catch(e) {
        err = e;
      }
      should.exist(err);
      err.name.should.equal('NetworkError');
      err.details.should.have.property('localPeerId');
      err.details.should.have.property('remotePeerId');
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
        const remotePeer = {
          id: 'https://127.0.0.1:3333',
          url: 'https://127.0.0.1:3333'
        };
        await _client.getEvents({eventHashes: ['abc'], remotePeer});
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
        const remotePeer = {
          id: config.server.baseUri,
          url: config.server.baseUri
        };
        await _client.getEvents({eventHashes: ['abc'], remotePeer});
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
