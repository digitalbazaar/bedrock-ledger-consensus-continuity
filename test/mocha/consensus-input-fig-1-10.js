'use strict';

const input = {
  ledgerNodeId: '2',
  history: {
    events: [
      {
        _children: [],
        _parents: [],
        eventHash: 'y3',
        event: {
          parentHash: [
            '0b846199-aad3-4fd3-95de-a0559780c129'
          ],
          treeHash: '0b846199-aad3-4fd3-95de-a0559780c129',
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
            '8e1521e4-d93b-4e38-b72c-5d5864df9000'
          ],
          treeHash: '8e1521e4-d93b-4e38-b72c-5d5864df9000',
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
            'y3'
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
        eventHash: 'y1',
        event: {
          parentHash: [
            'fcd56596-f328-464c-a18e-aa856fec9a68'
          ],
          treeHash: 'fcd56596-f328-464c-a18e-aa856fec9a68',
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
        eventHash: 'y2',
        event: {
          parentHash: [
            '8ea924a6-d5b0-48f4-9b2d-c9e434b21abf'
          ],
          treeHash: '8ea924a6-d5b0-48f4-9b2d-c9e434b21abf',
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
        eventHash: '3-1',
        event: {
          parentHash: [
            'y3',
            'yb'
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
            'y1',
            'yb',
            'y3'
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
            'b1-1',
            '3-1'
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
}
;

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
