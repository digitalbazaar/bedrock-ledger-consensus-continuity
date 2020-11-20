/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {_client} = require('bedrock-ledger-consensus-continuity');

describe('Client notifyPeer API', () => {
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
