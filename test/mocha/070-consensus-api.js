/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const consensusApi =
  require('bedrock-ledger-consensus-continuity/lib/consensus');

const mockData = require('./mock.data');

const {consensusInput} = mockData;

/* eslint-disable no-unused-vars */
describe('Consensus API findConsensus', () => {
  it.only('ledger history Figure 1.2', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-2'];

    const supportY1 = ['y1', 'y3', 'y4'];
    const supportY2 = ['y1', 'y2', 'y3', 'y4'];

    const expectedState = {
      y1: {
        support: ['y1']
      },
      y2: {
        support: ['y2']
      },
      y3: {
        support: ['y3']
      },
      y4: {
        support: ['y4']
      },
      '1-1': {
        support: supportY1
      },
      '1-2': {
        support: supportY1
      },
      '1-3': {
        support: supportY2,
        proposal: '1-3'
      },
      '2-1': {
        support: supportY2
      },
      '2-2': {
        support: supportY1,
        proposal: '2-2'
      },
      '2-3': {
        support: supportY1,
        proposal: '2-2'
      },
      '3-1': {
        support: supportY1,
      },
      '3-2': {
        support: supportY2
      },
      '3-3': {
        support: supportY2,
        proposal: '3-3'
      },
      '3-4': {
        support: supportY2
      },
      '4-1': {
        support: supportY1
      },
      '4-2': {
        support: supportY2
      },
      '4-3': {
        support: supportY2,
        proposal: '4-3'
      },
      '4-4': {
        support: supportY2
      },
    };

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }
    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(true);
    should.exist(result.eventHashes);
    should.exist(result.eventHashes.mergeEventHashes);
    should.exist(result.eventHashes.parentHashes);
    should.exist(result.eventHashes.order);
    result.consensusProofHashes.should.have.same.members(supportY2);
    _validateState({input, expectedState});
  });
  it('ledger history Figure 1.4', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-4'];

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }
    debugger;
    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
  });
  it('ledger history Figure 1.5', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-5'];

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }

    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
  });
  it('ledger history Figure 1.6', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-6'];

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }

    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
  });
  it('ledger history Figure 1.7', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-7'];

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }
    debugger;
    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
  });
  it.only('ledger history Figure 1.8', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-8'];

    // Support
    const supportY1 = ['y1', 'yb'];
    const supportY2 = ['y1', 'y2', 'y3', 'yb'];

    const expectedState = {
      y1: {
        support: ['y1']
      },
      y2: {
        support: ['y2']
      },
      y3: {
        support: ['y3']
      },
      yb: {
        support: ['yb']
      },
      '1-1': {
        support: supportY1
      },
      '1-2': {
        support: supportY2,
        proposal: '1-2'
      },
      'b-1': {
        support: supportY1
      },
      'b1-1': {
        support: supportY1
      },
      'b2-1': {
        support: supportY2
      },
      'b2-2': {
        support: supportY2
      },
      '2-1': {
        support: supportY2
      },
      '2-2': {
        support: supportY1,
        proposal: '2-2'
      },
      '2-3': {
        support: supportY1,
        proposal: '2-2',
        detectedFork: ['b']
      },
      '2-4': {
        support: supportY2,
        detectedFork: ['b']
      },
      '2-5': {
        support: supportY2,
        proposal: '2-5',
        detectedFork: ['b']
      },
      '3-1': {
        support: supportY2
      },
      '3-2': {
        support: supportY2
      }
    };

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }
    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
    _validateState({input, expectedState});
    // debugger;
  });
  it.only('ledger history Figure 1.9', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-9'];

    // Support
    const supportY1 = ['y2', 'y3', 'yb'];
    const supportY2 = ['y1', 'y2', 'y3', 'yb'];

    const expectedState = {
      y1: {
        support: ['y1']
      },
      y2: {
        support: ['y2']
      },
      y3: {
        support: ['y3']
      },
      yb: {
        support: ['yb']
      },
      '1-1': {
        support: supportY2
      },
      'b-1': {
        support: supportY1
      },
      'b1-1': {
        support: supportY1
      },
      'b1-2': {
        support: supportY1,
        proposal: 'b1-2'
      },
      'b2-1': {
        support: supportY2
      },
      '2-1': {
        support: supportY1,
        proposal: '2-1',
        endorsed: true,
        endorsers: ['b', '2', '3'],
        endorsement: ['2-2']
      },
      '2-2': {
        support: supportY1,
        proposal: '2-1',
        endorses: ['2-1']
      },
      '3-1': {
        support: supportY1
      },
      '3-2': {
        support: supportY1,
        proposal: '3-2'
      }
    };

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }

    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
    _validateState({input, expectedState});
  });
  it.only('ledger history Figure 1.10', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-10'];

    // Support
    const supportY1 = ['y3', 'yb'];
    const supportY2 = ['y1', 'y2', 'y3', 'yb'];

    const expectedState = {
      y1: {
        support: ['y1']
      },
      y2: {
        support: ['y2']
      },
      y3: {
        support: ['y3']
      },
      yb: {
        support: ['yb']
      },
      '1-1': {
        support: supportY2
      },
      'b-1': {
        support: supportY1
      },
      'b1-1': {
        support: supportY1
      },
      'b2-1': {
        support: supportY2
      },
      '2-1': {
        support: supportY2
      },
      '2-2': {
        support: supportY1
      },
      '3-1': {
        support: supportY1
      }
    };

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }

    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
    _validateState({input, expectedState});
    // debugger;
  });
  it.only('ledger history Figure 1.11', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-11'];

    // Support
    const supportY1 = ['y2', 'y3', 'yb'];
    const supportY2 = ['y1', 'y2', 'y3', 'yb'];

    const expectedState = {
      y1: {
        support: ['y1']
      },
      y2: {
        support: ['y2']
      },
      y3: {
        support: ['y3']
      },
      yb: {
        support: ['yb']
      },
      '1-1': {
        support: supportY2
      },
      'b-1': {
        support: supportY1
      },
      'b1-1': {
        support: supportY1
      },
      'b1-2': {
        support: supportY1,
        proposal: 'b1-2'
      },
      'b2-1': {
        support: supportY2
      },
      '2-1': {
        support: supportY1,
        proposal: '2-1',
        endorsed: true,
        endorsers: ['b', '2', '3'],
        endorsement: ['2-2']
      },
      '2-2': {
        support: supportY1,
        proposal: '2-1',
        endorses: ['2-1']
      },
      '3-1': {
        support: supportY1
      },
      '3-2': {
        support: supportY1,
        proposal: '3-2'
      }
    };

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }

    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
    debugger;
  });
  it('ledger history Figure 1.12', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-12'];

    try {
      result = consensusApi.findConsensus(input);
    } catch(e) {
      err = e;
    }

    assertNoError(err);
    should.exist(result);
    result.consensus.should.equal(false);
  });
});

function _validateState({input, expectedState}) {
  const {events} = input.history;
  events.forEach(event => {
    const {
      eventHash, _supporting, _proposal, _votes, _electorEndorsement,
      _endorsers, _proposalEndorsed, _proposalEndorsement, _endorsesProposal
    } = event;
    const expectedEventState = expectedState[eventHash];
    // validate support
    if(expectedEventState && expectedEventState.support) {
      const support = _supporting.map(({eventHash}) => eventHash);
      support.should.have.same.members(expectedEventState.support);
    }
    // validate proposals
    if(expectedEventState && expectedEventState.proposal) {
      _proposal.eventHash.should.equal(expectedEventState.proposal);
    }
    // validate endorsed event
    if(expectedEventState && expectedEventState.endorsed) {
      // should be endorsed
      _proposalEndorsed.should.equal(true);
      // should specify expected endorsers
      expectedEventState.endorsers.forEach(endorser => {
        _endorsers.has(endorser).should.equal(true);
      });
      // should specify the endorsing event
      const electorEndorsements =
        _electorEndorsement.map(({eventHash}) => eventHash);
      electorEndorsements.should.have.same.members(
        expectedEventState.endorsement);
      const proposalEndorsements =
        _proposalEndorsement.map(({eventHash}) => eventHash);
      proposalEndorsements.should.have.same.members(
        expectedEventState.endorsement);
    }
    // validate endorsing event
    if(expectedEventState && expectedEventState.endorses) {
      const endorsedProposals =
        _endorsesProposal.map(({eventHash}) => eventHash);
      endorsedProposals.should.have.same.members(expectedEventState.endorses);
    }
    // validate detected forks
    if(expectedEventState && expectedEventState.detectedFork) {
      expectedEventState.detectedFork.forEach(nodeId => {
        _votes[nodeId].should.equal(false);
      });
    }
  });
}
