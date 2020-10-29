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
  it.only('Figure 1.2', async () => {
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
  it.only('Figure 1.4', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-4'];

    // Support
    const supportY1 = ['yb'];
    const supportY2 = ['y1', 'yb'];
    const supportY3 = ['y2', 'yb'];
    const supportY4 = ['y1', 'y2', 'yb'];

    const expectedState = {
      y1: {
        support: ['y1']
      },
      y2: {
        support: ['y2']
      },
      yb: {
        support: ['yb']
      },
      '1-1': {
        support: supportY2
      },
      '1-2': {
        support: supportY2
      },
      '1-3': {
        support: supportY2
      },
      'b-1': {
        support: supportY1
      },
      'b1-1': {
        support: supportY1
      },
      'b2-1': {
        support: supportY1
      },
      'b3-1': {
        support: supportY1
      },
      'b3-2': {
        support: supportY1
      },
      '2-1': {
        support: supportY3
      },
      '2-2': {
        support: supportY4,
        detectedFork: ['b']
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
  it.only('Figure 1.5', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-5'];

    // Support
    const supportY1 = ['y1', 'y2', 'y3', 'y4', 'y5', 'ypi'];

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
      '2-1': {
        support: supportY1
      },
      '3-1': {
        support: supportY1
      },
      '4-1': {
        support: supportY1
      },
      '5-1': {
        support: supportY1
      },
      'pi-1': {
        support: supportY1,
        proposal: 'pi-1',
        endorsed: true,
        // FIXME: Can we assert the set of endorsers in a test, if continuity
        //        short circuits after it finds the first 2f + 1?
        endorsers: ['1', /* '2', */ '3', '4', '5', 'pi'],
        endorsersTotal: 5,
        endorsement: ['pi-2']
      },
      'pi-2': {
        support: supportY1,
        proposal: 'pi-1',
        endorses: ['pi-1']
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
  it.only('Figure 1.6', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-6'];

    // Support
    const supportY1 = ['y1', 'y2', 'y3', 'y4', 'y5', 'ypi'];

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
      '2-1': {
        support: supportY1
      },
      '2-2': {
        support: supportY1,
        proposal: '2-1'
      },
      '3-1': {
        support: supportY1
      },
      '4-1': {
        support: supportY1
      },
      '5-1': {
        support: supportY1
      },
      '6-1': {
        support: supportY1,
        proposal: '6-1'
      },
      'pi-1': {
        support: supportY1,
        proposal: 'pi-1',
        endorsed: true,
        // FIXME: Can we assert the set of endorsers in a test, if continuity
        //        short circuits after it finds the first 2f + 1?
        endorsers: ['1', /* '2', */ '3', '4', '5', 'pi'],
        endorsersTotal: 5,
        endorsement: ['pi-2']
      },
      'pi-2': {
        support: supportY1,
        proposal: 'pi-1',
        endorses: ['pi-1']
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
  it('Figure 1.7', async () => { // FIXME: Not finished
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
  it.only('Figure 1.8', async () => {
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
  });
  it.only('Figure 1.9', async () => {
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
  it.only('Figure 1.10', async () => {
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
  });
  it.only('Figure 1.11', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-11'];

    // Support
    const supportY1 = ['y1', 'y2', 'yb'];
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
        support: supportY1,
      },
      '2-2': {
        support: supportY1,
        proposal: '2-2',
        // endorsed: true, // FIXME: Shouldn't this have endorsed true?
        endorsers: ['1', '2', '3'],
        endorsement: ['2-3']
      },
      '2-3': {
        support: supportY2, // FIXME: Should this be supporting Y1?
        proposal: '2-3', // FIXME: Should this be supporting event "2-2"?
        endorses: ['2-2']
      },
      '3-1': {
        support: supportY2
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
  it.only('Figure 1.12', async () => {
    let result, err;
    const {input} = consensusInput['fig-1-12'];

    // Support
    const supportY1 = ['y1', 'y3', 'yb'];
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
      '2-1': {
        support: supportY2
      },
      '2-2': {
        support: supportY1,
        proposal: '2-2'
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
    _validateSupport({expectedEventState, _supporting});
    // validate proposals
    _validateProposals({expectedEventState, _proposal});
    // validate endorsed event
    _validateProposalEndorsed({expectedEventState, _proposalEndorsed});
    // validate proposal endorsers
    _validateProposalEndorsers({expectedEventState, _endorsers});
    // validate total endorsers
    _validateTotalEndorsers({expectedEventState, _endorsers});
    // validate endorsements
    _validateEndorsements(
      {expectedEventState, _electorEndorsement, _proposalEndorsement});
    // validate endorsing event
    _validateEndorsingEvent({expectedEventState, _endorsesProposal});
    // validate detected forks
    _validateDetectedForks({expectedEventState, _votes});
  });
}

function _validateSupport({expectedEventState, _supporting}) {
  if(expectedEventState && expectedEventState.support) {
    const support = _supporting.map(({eventHash}) => eventHash);
    support.should.have.same.members(expectedEventState.support);
  }
}

function _validateProposals({expectedEventState, _proposal}) {
  if(expectedEventState && expectedEventState.proposal) {
    _proposal.eventHash.should.equal(expectedEventState.proposal);
  }
}

function _validateProposalEndorsed({expectedEventState, _proposalEndorsed}) {
  if(expectedEventState && expectedEventState.endorsed) {
    // should be endorsed
    _proposalEndorsed.should.equal(true);
  }
}

function _validateProposalEndorsers({expectedEventState, _endorsers}) {
  if(expectedEventState && expectedEventState.endorsers) {
    // should specify expected endorsers
    expectedEventState.endorsers.forEach(endorser => {
      _endorsers.has(endorser).should.equal(true);
    });
  }
}

function _validateTotalEndorsers({expectedEventState, _endorsers}) {
  if(expectedEventState && expectedEventState.endorsersTotal) {
    // should specify total number of endorsers
    _endorsers.size.should.equal(expectedEventState.endorsersTotal);
  }
}

function _validateEndorsements(
  {expectedEventState, _electorEndorsement, _proposalEndorsement}) {
  if(expectedEventState && expectedEventState.endorsement) {
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
}

function _validateEndorsingEvent({expectedEventState, _endorsesProposal}) {
  if(expectedEventState && expectedEventState.endorses) {
    const endorsedProposals =
      _endorsesProposal.map(({eventHash}) => eventHash);
    endorsedProposals.should.have.same.members(expectedEventState.endorses);
  }
}

function _validateDetectedForks({expectedEventState, _votes}) {
  if(expectedEventState && expectedEventState.detectedFork) {
    expectedEventState.detectedFork.forEach(nodeId => {
      _votes[nodeId].should.equal(false);
    });
  }
}
