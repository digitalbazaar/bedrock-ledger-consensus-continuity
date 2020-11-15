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
  describe('Figure 1.2', async () => {
    const {graph} = consensusInput['fig-1-2'];

    const nodes = ['1', '2', '3', '4'];

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

    const extendedTests = new Map();

    function _node1ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _node3ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _node4ExtendedTest({result}) {
      result.consensus.should.equal(true);
      should.exist(result.eventHashes);
      should.exist(result.eventHashes.mergeEventHashes);
      should.exist(result.eventHashes.parentHashes);
      should.exist(result.eventHashes.order);
      result.consensusProofHashes.should.have.same.members(supportY2);
    }

    extendedTests.set('1', _node1ExtendedTest);
    extendedTests.set('2', _node2ExtendedTest);
    extendedTests.set('3', _node3ExtendedTest);
    extendedTests.set('4', _node4ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.4', async () => {
    const {graph} = consensusInput['fig-1-4'];

    const nodes = ['2'];

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
        detectFork: ['b']
      }
    };

    const extendedTests = new Map();

    function _node1ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('1', _node1ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.5', async () => {
    const {graph} = consensusInput['fig-1-5'];

    const nodes = ['pi'];

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
        endorsersTotal: 5, // 2f + 1
        endorsement: ['pi-2']
      },
      'pi-2': {
        support: supportY1,
        proposal: 'pi-1',
        endorses: ['pi-1']
      }
    };

    const extendedTests = new Map();

    function _nodePiExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('pi', _nodePiExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});

  });
  describe('Figure 1.6', async () => {
    const {graph} = consensusInput['fig-1-6'];

    const nodes = ['2', 'pi'];

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
        endorsersTotal: 5, // 2f + 1
        endorsement: ['pi-2']
      },
      'pi-2': {
        support: supportY1,
        proposal: 'pi-1',
        endorses: ['pi-1']
      }
    };

    const extendedTests = new Map();

    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _nodePiExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('2', _node2ExtendedTest);
    extendedTests.set('pi', _nodePiExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.7', async () => {
    const {graph} = consensusInput['fig-1-7'];
    const nodes = ['1', '2'];

    // Support
    const supportY1 = ['y1', 'y2', 'yh', 'yb', 'ygamma'];
    const supportY2 = ['y1', 'y2', 'yh', 'yb', 'yalpha', 'ybeta', 'ygamma'];

    const expectedState = {
      y1: {
        support: ['y1']
      },
      y2: {
        support: ['y2']
      },
      yh: {
        support: ['yh']
      },
      yb: {
        support: ['yb']
      },
      yalpha: {
        support: ['yalpha']
      },
      ybeta: {
        support: ['ybeta']
      },
      ygamma: {
        support: ['ygamma']
      },
      '1-1': {
        _exclude: {
          // Note: Exclude this state from tests involving Node 2 as it cannot
          // be deduced by its perspective of the DAG. In other words, Node 2
          // does not contain enough of the DAG in its history to make these
          // assertions.
          2: new Set(['endorsed', 'proposal', 'endorsersTotal', 'endorsement'])
        },
        support: supportY1,
        proposal: '1-1',
        endorsed: true,
        endorsersTotal: 5, // 2f + 1
        endorsement: ['1-2']
      },
      '1-2': {
        support: supportY1,
        proposal: '1-1',
        endorses: ['1-1']
      },
      'h-1': {
        support: supportY1
      },
      'h-2': {
        support: supportY2,
        detectFork: ['b']
      },
      'b-1': {
        support: supportY1
      },
      'b1-1': {
        support: supportY1
      },
      'b1-2': {
        support: supportY1
      },
      'b2-1': {
        support: supportY2
      },
      'b2-2': {
        support: supportY2
      },
      '2-1': {
        support: supportY2,
        proposal: '2-1',
        endorsed: true,
        endorsersTotal: 5, // 2f + 1
        endorsement: ['2-2']
      },
      '2-2': {
        support: supportY2,
        proposal: '2-1',
        endorses: ['2-1'],
        detectFork: ['b']
      }
    };

    const extendedTests = new Map();

    function _node1ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('1', _node1ExtendedTest);
    extendedTests.set('2', _node2ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.8', async () => {
    const {graph} = consensusInput['fig-1-8'];

    const nodes = ['2'];

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
        detectFork: ['b']
      },
      '2-4': {
        support: supportY2,
        detectFork: ['b']
      },
      '2-5': {
        support: supportY2,
        proposal: '2-5',
        detectFork: ['b']
      },
      '3-1': {
        support: supportY2
      },
      '3-2': {
        support: supportY2
      }
    };

    const extendedTests = new Map();

    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('2', _node2ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.9', async () => {
    const {graph} = consensusInput['fig-1-9'];

    const nodes = ['1', '2'];

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
        endorsersTotal: 3, // 2f + 1
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

    const extendedTests = new Map();

    function _node1ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('1', _node1ExtendedTest);
    extendedTests.set('2', _node2ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.10', async () => {
    const {graph} = consensusInput['fig-1-10'];

    const nodes = ['1', '2'];

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

    const extendedTests = new Map();

    function _node1ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('1', _node1ExtendedTest);
    extendedTests.set('2', _node2ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.11', async () => {
    const {graph} = consensusInput['fig-1-11'];

    const nodes = ['2'];

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
        endorsed: true,
        endorsersTotal: 3, // 2f + 1
        endorsement: ['2-3']
      },
      '2-3': {
        support: supportY1,
        proposal: '2-2',
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

    const extendedTests = new Map();

    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('2', _node2ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
  describe('Figure 1.12', async () => {
    const {graph} = consensusInput['fig-1-12'];

    const nodes = ['1', '2'];

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

    const extendedTests = new Map();

    function _node1ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }
    function _node2ExtendedTest({result}) {
      result.consensus.should.equal(false);
    }

    extendedTests.set('1', _node1ExtendedTest);
    extendedTests.set('2', _node2ExtendedTest);

    _validateNodesState({nodes, graph, expectedState, extendedTests});
  });
});

function _validateNodesState({
  nodes, graph, expectedState, extendedTests = new Map()
} = {}) {
  for(const nodeId of nodes) {
    describe(`Node: ${nodeId}`, () => {
      it('should validate state', async () => {
        let result, err;
        const input = {
          ledgerNodeId: nodeId,
          history: graph.getHistory({nodeId}),
          electors: graph.getElectors(),
          recoveryElectors: [],
          mode: 'first'
        };
        try {
          result = consensusApi.findConsensus(input);
        } catch(e) {
          err = e;
        }
        assertNoError(err);
        should.exist(result);
        _validateState({input, expectedState, nodeId});

        const _extendedTest = extendedTests.get(nodeId);
        if(_extendedTest) {
          _extendedTest({result, input});
        }
      });
    });
  }
}

function _validateState({input, expectedState, nodeId}) {
  const {events} = input.history;
  events.forEach(event => {
    const {
      eventHash,
      _c: {
        endorsers: _endorsers,
        endorsesProposal: _endorsesProposal,
        proposal: _proposal,
        proposalEndorsed: _proposalEndorsed,
        proposalEndorsement: _proposalEndorsement,
        supporting: _supporting,
        votes: _votes
      }
    } = event;
    const expectedEventState = expectedState[eventHash];
    let exclude = new Set();
    if(_hasExclusion({expectedEventState, nodeId})) {
      exclude = expectedEventState._exclude[nodeId];
    }

    // validate support
    _validateSupport({exclude, expectedEventState, _supporting});
    // validate proposals
    _validateProposals({exclude, expectedEventState, _proposal});
    // validate endorsed event
    _validateProposalEndorsed({exclude, expectedEventState, _proposalEndorsed});
    // validate total endorsers
    _validateTotalEndorsers({exclude, expectedEventState, _endorsers});
    // validate endorsements
    _validateEndorsements(
      {exclude, expectedEventState, _proposalEndorsement});
    // validate endorsing event
    _validateEndorsingEvent({exclude, expectedEventState, _endorsesProposal});
    // validate detected forks
    _validateDetectedForks({exclude, expectedEventState, _votes});
  });
}

function _validateSupport({expectedEventState, _supporting, exclude}) {
  const skip = exclude.has('support');
  if(expectedEventState && expectedEventState.support && !skip) {
    should.exist(_supporting);
    const support = _supporting.map(({eventHash}) => eventHash);
    support.should.have.same.members(expectedEventState.support);
  }
}

function _validateProposals({expectedEventState, _proposal, exclude}) {
  const skip = exclude.has('proposal');
  if(expectedEventState && expectedEventState.proposal && !skip) {
    _proposal.eventHash.should.equal(expectedEventState.proposal);
  }
}

function _validateProposalEndorsed(
  {expectedEventState, _proposalEndorsed, exclude}) {
  const skip = exclude.has('endorsed');
  if(expectedEventState && expectedEventState.endorsed && !skip) {
    // should be endorsed
    _proposalEndorsed.should.equal(true);
  }
}

function _validateTotalEndorsers({expectedEventState, _endorsers, exclude}) {
  const skip = exclude.has('endorsersTotal');
  if(expectedEventState && expectedEventState.endorsersTotal && !skip) {
    // should specify total number of endorsers
    should.exist(_endorsers);
    _endorsers.size.should.equal(expectedEventState.endorsersTotal);
  }
}

function _validateEndorsements(
  {expectedEventState, _proposalEndorsement, exclude}) {
  const skip = exclude.has('endorsement');
  if(expectedEventState && expectedEventState.endorsement && !skip) {
    // should specify the endorsing event
    const proposalEndorsements =
      _proposalEndorsement.map(({eventHash}) => eventHash);
    proposalEndorsements.should.have.same.members(
      expectedEventState.endorsement);
  }
}

function _validateEndorsingEvent(
  {expectedEventState, _endorsesProposal, exclude}) {
  const skip = exclude.has('endorses');
  if(expectedEventState && expectedEventState.endorses && !skip) {
    should.exist(_endorsesProposal);
    const endorsedProposals =
      _endorsesProposal.map(({eventHash}) => eventHash);
    endorsedProposals.should.have.same.members(expectedEventState.endorses);
  }
}

function _validateDetectedForks({expectedEventState, _votes, exclude}) {
  const skip = exclude.has('detectFork');
  if(expectedEventState && expectedEventState.detectFork && !skip) {
    expectedEventState.detectFork.forEach(nodeId => {
      _votes.get(nodeId).should.equal(false);
    });
  }
}

function _hasExclusion({expectedEventState, nodeId}) {
  return expectedEventState && expectedEventState._exclude &&
    expectedEventState._exclude[nodeId];
}
