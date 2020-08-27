'use strict';

const input = {
  ledgerNodeId: '2',
  history: {
    events: [
      {
        _children: [],
        _parents: [],
        eventHash: 'yb',
        event: {
          parentHash: [
            '185ad8f3-98f6-4690-9834-5353dc5e3f1d'
          ],
          treeHash: '185ad8f3-98f6-4690-9834-5353dc5e3f1d',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'b'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'y1',
        event: {
          parentHash: [
            'c1942ee7-c345-46a3-ab29-aff026a8e5cb'
          ],
          treeHash: 'c1942ee7-c345-46a3-ab29-aff026a8e5cb',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '1'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'b1-1',
        event: {
          parentHash: [
            'yb'
          ],
          treeHash: '92fc678e-1df2-44fd-9745-1fe31b0d6d81',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'b1'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '1-1',
        event: {
          parentHash: [
            'y1',
            'yb'
          ],
          treeHash: 'y1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '1'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'b3-1',
        event: {
          parentHash: [
            'yb'
          ],
          treeHash: '68533f2a-95ad-42a9-94ba-5edad1ba5a83',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'b3'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '1-2',
        event: {
          parentHash: [
            '1-1',
            'b1-1'
          ],
          treeHash: '1-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '1'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'b3-2',
        event: {
          parentHash: [
            'b3-1'
          ],
          treeHash: 'b3-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'b3'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'y2',
        event: {
          parentHash: [
            'b47f76e0-5e50-4d4d-8c0f-438b7c59787e'
          ],
          treeHash: 'b47f76e0-5e50-4d4d-8c0f-438b7c59787e',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '2'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '1-3',
        event: {
          parentHash: [
            '1-2'
          ],
          treeHash: '1-2',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '1'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '2-1',
        event: {
          parentHash: [
            'y2',
            'b3-2'
          ],
          treeHash: 'y2',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '2'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '2-2',
        event: {
          parentHash: [
            '2-1',
            '1-3'
          ],
          treeHash: '2-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '2'
          }
        }
      }
    ],
    eventMap: {},
    localBranchHead: {
      eventHash: '2-2',
      generation: 3
    }
  },
  electors: [
    {
      id: '1'
    },
    {
      id: 'b'
    },
    {
      id: '2'
    }
  ],
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
