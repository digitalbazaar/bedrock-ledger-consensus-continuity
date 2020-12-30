/*!
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const continuityApi =
  require('bedrock-ledger-consensus-continuity/lib/continuity');
const Graph = require('./tools/Graph');

const mockData = require('./mock.data');

const {consensusInput} = mockData;

/* eslint-disable no-unused-vars */
describe('Continuity API findConsensus', () => {
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
        _incremental: {
          support: {
            seenBy: new Set(['1']),
          }
        },
        support: supportY1
      },
      '1-2': {
        _incremental: {
          support: {
            seenBy: new Set(['1']),
          }
        },
        support: supportY1
      },
      '1-3': {
        _incremental: {
          support: {
            seenBy: new Set(['1']),
          }
        },
        support: supportY2,
        proposal: '1-3'
      },
      '2-1': {
        _incremental: {
          support: {
            seenBy: new Set(['2']),
          }
        },
        support: supportY2
      },
      '2-2': {
        _incremental: {
          support: {
            seenBy: new Set(['2']),
          }
        },
        support: supportY1,
        proposal: '2-2'
      },
      '2-3': {
        _incremental: {
          support: {
            seenBy: new Set(['2']),
          }
        },
        support: supportY1,
        proposal: '2-2'
      },
      '3-1': {
        _incremental: {
          support: {
            seenBy: new Set(['3']),
          }
        },
        support: supportY1,
      },
      '3-2': {
        _incremental: {
          support: {
            seenBy: new Set(['3']),
          }
        },
        support: supportY2
      },
      '3-3': {
        _incremental: {
          support: {
            seenBy: new Set(['3']),
          }
        },
        support: supportY2,
        proposal: '3-3'
      },
      '3-4': {
        _incremental: {
          support: {
            seenBy: new Set(['3']),
          }
        },
        support: supportY2
      },
      '4-1': {
        _incremental: {
          support: {
            seenBy: new Set(['4']),
          }
        },
        support: supportY1
      },
      '4-2': {
        _incremental: {
          support: {
            seenBy: new Set(['4']),
          }
        },
        support: supportY2
      },
      '4-3': {
        _incremental: {
          support: {
            seenBy: new Set(['4']),
          }
        },
        support: supportY2,
        proposal: '4-3'
      },
      '4-4': {
        _incremental: {
          support: {
            seenBy: new Set(['4']),
          }
        },
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
      should.exist(result.eventHashes.blockOrdering);
      should.exist(result.eventHashes.gossipOrdering);
      result.consensusProofHashes.should.have.same.members(supportY2);
    }

    extendedTests.set('1', _node1ExtendedTest);
    extendedTests.set('2', _node2ExtendedTest);
    extendedTests.set('3', _node3ExtendedTest);
    extendedTests.set('4', _node4ExtendedTest);

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
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

    _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests});
    /*
    _runIncrementallyBuiltDAGTest({nodes, graph, expectedState, extendedTests});
     */
  });
});

function _runPreBuiltDAGTest({nodes, graph, expectedState, extendedTests}) {
  describe(`Pre-Built DAG`, async () => {
    for(const nodeId of nodes) {
      describe(`Node: ${nodeId}`, () => {
        it('should validate state', () => {
          _validate({nodeId, graph, expectedState, extendedTests});
        });
      });
    }
  });
}

function _runIncrementallyBuiltDAGTest(
  {nodes, graph, expectedState, extendedTests}) {
  describe(`Incrementally Built DAG`, async () => {
    const g = new Graph();
    const {events} = graph.transactionLog;
    for(const {id, options} of graph.transactionLog.nodes) {
      g.addNode(id, options);
    }
    describe(`Nodes`, () => {
      it(`should validate state`, async () => {
        // preserve state outside of validation runs
        const state = {};
        for(const e of events) {
          const {eventHash} = e;
          const expectedEventState = expectedState[eventHash] || {};

          g.mergeEvent(e);

          for(const nodeId of nodes) {
            const opts = {incremental: true, expectedEventState, nodeId};
            const shouldRun = _shouldRunIncrementalTest(opts);
            if(shouldRun) {
              // console.log(
              //   `Node ${nodeId} @ ${eventHash}`,
              //   `Seen By:`,
              //   opts.expectedEventState._incremental.support.seenBy);
              await _validate({
                nodeId, graph: g, expectedState, incremental: {eventHash},
                state
              });
            }
            await _validate({nodeId, graph: g, expectedState, state});
          }
        }
      });
    });
  });
}

function _validate({
  nodeId, graph, expectedState, incremental, state = {},
  extendedTests = new Map()
}) {
  return new Promise(resolve => {
    let result, err;
    const input = {
      ledgerNodeId: nodeId,
      history: graph.getHistory({nodeId}),
      witnesses: graph.getWitnesses(),
      state
    };
    try {
      result = continuityApi.findConsensus(input);
    } catch(e) {
      err = e;
    }
    assertNoError(err);
    should.exist(result);
    _validateExpectedState({input, expectedState, nodeId, incremental});

    const _extendedTest = extendedTests.get(nodeId);
    if(_extendedTest) {
      _extendedTest({result, input});
    }

    resolve();
  });
}

function _validateExpectedState({input, expectedState, nodeId, incremental}) {
  const {events} = input.history;
  if(incremental) {
    events.map(({eventHash}) => eventHash)
      .includes(incremental.eventHash).should.be.true;
  }
  if(events.length === 0) {
    return;
  }
  if(events.length === 1) {
    const [eventHash] = events.map(({eventHash}) => eventHash);
    should.exist(eventHash);
    input.history.localBranchHead.eventHash.should.equal(eventHash);
    input.history.localBranchHead.generation.should.equal(1);
    return;
  }
  events.forEach(event => {
    const {
      eventHash,
      _c: {
        endorsers: _endorsers,
        endorsesProposal: _endorsesProposal,
        proposal: _proposal,
        proposalEndorsed: _proposalEndorsed,
        proposalEndorsement: _proposalEndorsement,
        support: _support,
        mostRecentWitnessAncestors: _mostRecentWitnessAncestors
      }
    } = event;
    const expectedEventState = expectedState[eventHash];
    let exclude = new Set();
    if(_hasExclusion({expectedEventState, nodeId})) {
      exclude = expectedEventState._exclude[nodeId];
    }
    if(incremental) {
      if(_shouldRunIncrementalTest({incremental, expectedEventState, nodeId})) {
        return _validateSupport({exclude, expectedEventState, _support});
      }
      return;
    }
    // validate support
    _validateSupport({exclude, expectedEventState, _support});
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
    _validateDetectedForks(
      {exclude, expectedEventState, _mostRecentWitnessAncestors});
  });
}

function _validateSupport({expectedEventState, _support, exclude}) {
  const skip = !expectedEventState || exclude.has('support');
  if(skip) {
    return;
  }
  if(expectedEventState.support) {
    should.exist(_support);
    const support = _support.map(({eventHash}) => eventHash);
    support.should.have.same.members(expectedEventState.support);
  }
}

function _validateProposals({expectedEventState, _proposal, exclude}) {
  const skip = !expectedEventState || exclude.has('proposal');
  if(skip) {
    return;
  }
  if(expectedEventState.proposal) {
    _proposal.eventHash.should.equal(expectedEventState.proposal);
  }
}

function _validateProposalEndorsed(
  {expectedEventState, _proposalEndorsed, exclude}) {
  const skip = !expectedEventState || exclude.has('endorsed');
  if(skip) {
    return;
  }
  if(expectedEventState.endorsed) {
    // should be endorsed
    _proposalEndorsed.should.equal(true);
  }
}

function _validateTotalEndorsers({expectedEventState, _endorsers, exclude}) {
  const skip = !expectedEventState || exclude.has('endorsersTotal');
  if(skip) {
    return;
  }
  if(expectedEventState.endorsersTotal) {
    // should specify total number of endorsers
    should.exist(_endorsers);
    _endorsers.size.should.equal(expectedEventState.endorsersTotal);
  }
}

function _validateEndorsements(
  {expectedEventState, _proposalEndorsement, exclude}) {
  const skip = !expectedEventState || exclude.has('endorsement');
  if(skip) {
    return;
  }
  if(expectedEventState.endorsement) {
    // should specify the endorsing event
    const proposalEndorsements =
      _proposalEndorsement.map(({eventHash}) => eventHash);
    proposalEndorsements.should.have.same.members(
      expectedEventState.endorsement);
  }
}

function _validateEndorsingEvent(
  {expectedEventState, _endorsesProposal, exclude}) {
  const skip = !expectedEventState || exclude.has('endorses');
  if(skip) {
    return;
  }
  if(expectedEventState.endorses) {
    should.exist(_endorsesProposal);
    const endorsedProposals =
      _endorsesProposal.map(({eventHash}) => eventHash);
    endorsedProposals.should.have.same.members(expectedEventState.endorses);
  }
}

function _validateDetectedForks(
  {expectedEventState, _mostRecentWitnessAncestors, exclude}) {
  const skip = !expectedEventState || exclude.has('detectFork');
  if(skip) {
    return;
  }
  if(expectedEventState.detectFork) {
    expectedEventState.detectFork.forEach(nodeId => {
      _mostRecentWitnessAncestors.get(nodeId).should.equal(false);
    });
  }
}

function _hasExclusion({expectedEventState, nodeId}) {
  return expectedEventState && expectedEventState._exclude &&
    expectedEventState._exclude[nodeId];
}

function _shouldRunIncrementalTest({incremental, expectedEventState, nodeId}) {
  if(!incremental) {
    return false;
  }

  return (
    expectedEventState &&
    expectedEventState._incremental &&
    expectedEventState._incremental.support.seenBy.size > 0 &&
    expectedEventState._incremental.support.seenBy.has(nodeId)
  );
}
