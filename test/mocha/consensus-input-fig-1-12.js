'use strict';

const input = {
  ledgerNodeId: '2',
  history: {
    events: [
      {
        _children: [],
        _parents: [],
        eventHash: 'y1',
        event: {
          parentHash: [
            '2e5baa2a-08af-45c5-a545-745e820b6b5d'
          ],
          treeHash: '2e5baa2a-08af-45c5-a545-745e820b6b5d',
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
            '59cff9a2-f1f3-405e-808c-fe78b0610396'
          ],
          treeHash: '59cff9a2-f1f3-405e-808c-fe78b0610396',
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
            'de82182f-06ea-4cc3-b1ad-89ea8fffff19'
          ],
          treeHash: 'de82182f-06ea-4cc3-b1ad-89ea8fffff19',
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
            'y1'
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
        eventHash: 'y2',
        event: {
          parentHash: [
            '9247c0fb-b8c2-4002-91e7-e6637636db9c'
          ],
          treeHash: '9247c0fb-b8c2-4002-91e7-e6637636db9c',
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
            'y1',
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
        eventHash: '1-1',
        event: {
          parentHash: [
            'y1',
            'yb',
            'y3'
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
            '1-1',
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
    }
  ],
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
