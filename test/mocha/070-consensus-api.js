/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const _ = require('lodash');
const consensusApi =
  require('bedrock-ledger-consensus-continuity/lib/consensus');

const mockData = require('./mock.data');

const {consensusInput} = mockData;

/* eslint-disable no-unused-vars */
describe('Consensus API findConsensus', () => {
  it.only('ledger history Figure 1.2', async () => {
    let result, err;
    const input = consensusInput['fig-1-2'];

    const supportY1 = ['y1', 'y3', 'y4'];
    const supportY2 = ['y1', 'y2', 'y3', 'y4'];

    const expectedSupport = {
      y1: ['y1'],
      y2: ['y2'],
      y3: ['y3'],
      y4: ['y4'],
      '1-1': supportY1,
      '1-2': supportY1,
      '1-3': supportY2,
      '2-1': supportY2,
      '2-2': supportY1,
      '2-3': supportY1,
      '3-1': supportY1,
      '3-2': supportY2,
      '3-3': supportY2,
      '3-4': supportY2,
      '4-1': supportY1,
      '4-2': supportY2,
      '4-3': supportY2,
      '4-4': supportY2
    };

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }

    assertNoError(err);
    should.exist(result);
    should.exist(result.eventHashes);
    should.exist(result.eventHashes.mergeEventHashes);
    should.exist(result.eventHashes.parentHashes);
    should.exist(result.eventHashes.order);
    result.consensusProofHashes.should.have.same.members(supportY2);
    input.history.events.forEach(({eventHash, _supporting}) => {
      if(eventHash === '4-5') { // decision
        should.not.exist(_supporting);
      } else {
        _supporting.map(({eventHash}) => eventHash)
          .should.have.same.members(expectedSupport[eventHash]);
      }
    });
  });
});
