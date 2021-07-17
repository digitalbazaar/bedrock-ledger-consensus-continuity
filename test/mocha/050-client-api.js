/*!
 * Copyright (c) 2017-2021 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {_client, _localPeers} = require('bedrock-ledger-consensus-continuity');
const {config, util: {BedrockError}} = require('bedrock');

describe('Client API', () => {
  describe('notifyPeer', () => {
    let peerId = null;
    const ledgerNodeId = 'foo';
    const remotePeerId =
      'did:key:z6MkikAFvQGvzvunQcSma5jvUY41FkLJxrBomQPKPSWWX7kU';
    before(async () => {
      ({peerId} = await _localPeers.generate({ledgerNodeId}));
    });
    it('throws a NotFoundError if ledgerNodeId is not found', async () => {
      const remotePeer = {
        id: remotePeerId,
        url: 'https://127.0.0.1/consensus/continuity2017/peers/' +
          encodeURIComponent(remotePeerId)
      };
      let err;
      try {
        await _client.notifyPeer({remotePeer, ledgerNodeId: 'bar'});
      } catch(e) {
        err = e;
      }
      should.exist(err);
      err.name.should.equal('NotFoundError');
    });

    it('throws a NetworkError on connection refused', async () => {
      const remotePeer = {
        id: remotePeerId,
        url: 'https://127.0.0.1/consensus/continuity2017/peers/' +
          encodeURIComponent(remotePeerId)
      };
      let err;
      try {
        await _client.notifyPeer({ledgerNodeId, remotePeer});
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

    it('should notify peer', async () => {
      const remotePeer = {
        id: peerId,
        url: _localPeers.getLocalPeerUrl(peerId)
      };
      let err;
      try {
        // notifyPeer does not return anything
        await _client.notifyPeer({ledgerNodeId, remotePeer});
      } catch(e) {
        err = e;
      }
      should.not.exist(err);
      // FIXME look for the actual notification here
    });

  });

  describe('getEvents', () => {
    const remotePeerId =
      'did:key:z6MkikAFvQGvzvunQcSma5jvUY41FkLJxrBomQPKPSWWX7kU';
    it('properly handles ECONNREFUSED', async () => {
      let error;
      try {
        const remotePeer = {
          id: remotePeerId,
          url: 'https://127.0.0.1:3333/consensus/continuity2017/peers/' +
            encodeURIComponent(remotePeerId)
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
          id: remotePeerId,
          url: config.server.baseUri + '/consensus/continuity2017/peers/' +
            encodeURIComponent(remotePeerId)
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
