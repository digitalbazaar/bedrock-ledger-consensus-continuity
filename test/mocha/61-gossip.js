/*!
 * Copyright (c) 2017-2019 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {config, util: {BedrockError}} = require('bedrock');
const {_client} = require('bedrock-ledger-consensus-continuity');

describe('client getEvents API', () => {
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
