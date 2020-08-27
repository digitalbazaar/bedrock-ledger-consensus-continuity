'use strict';

const input = {
  ledgerNodeId: '2',
  history: {
    events: [
      {
        _children: [],
        _parents: [],
        eventHash: 'y2',
        event: {
          parentHash: [
            '421fb564-6983-4dc8-aed1-067f883d0f7c'
          ],
          treeHash: '421fb564-6983-4dc8-aed1-067f883d0f7c',
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
        eventHash: 'y1',
        event: {
          parentHash: [
            'ed5f412b-e997-4006-9f1e-0b585a4a5b56'
          ],
          treeHash: 'ed5f412b-e997-4006-9f1e-0b585a4a5b56',
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
        eventHash: 'y3',
        event: {
          parentHash: [
            '529e5df3-d378-4f56-87cf-d73668972faa'
          ],
          treeHash: '529e5df3-d378-4f56-87cf-d73668972faa',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '3'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'yb',
        event: {
          parentHash: [
            '29fae875-10a3-46c7-b671-b1b523eac151'
          ],
          treeHash: '29fae875-10a3-46c7-b671-b1b523eac151',
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
        eventHash: 'b-1',
        event: {
          parentHash: [
            'yb',
            'y3',
            'y1',
            'y2'
          ],
          treeHash: 'yb',
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
        eventHash: '3-1',
        event: {
          parentHash: [
            'y3',
            'yb',
            'y1',
            'y2'
          ],
          treeHash: 'y3',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '3'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'b1-1',
        event: {
          parentHash: [
            'b-1'
          ],
          treeHash: 'b-1',
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
        eventHash: '2-1',
        event: {
          parentHash: [
            'y2',
            'b1-1',
            '3-1'
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
        eventHash: '3-2',
        event: {
          parentHash: [
            '3-1',
            '2-1'
          ],
          treeHash: '3-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '3'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'b1-2',
        event: {
          parentHash: [
            'b1-1',
            '2-1'
          ],
          treeHash: 'b1-1',
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
        eventHash: '2-2',
        event: {
          parentHash: [
            '2-1',
            'b1-2',
            '3-2'
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
    },
    {
      id: '3'
    },
    {
      id: 'alpha'
    }
  ],
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
