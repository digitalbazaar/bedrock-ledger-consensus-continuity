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
            '2f1fd8c8-5d19-4a20-a7e2-3ff89766cbd8'
          ],
          treeHash: '2f1fd8c8-5d19-4a20-a7e2-3ff89766cbd8',
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
        eventHash: 'yb',
        event: {
          parentHash: [
            '6716c697-fe18-4521-8c35-a63f1adeb184'
          ],
          treeHash: '6716c697-fe18-4521-8c35-a63f1adeb184',
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
            '7473189c-9f69-420b-ac2b-cefd463a4850'
          ],
          treeHash: '7473189c-9f69-420b-ac2b-cefd463a4850',
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
        eventHash: 'y3',
        event: {
          parentHash: [
            '82cf6d7c-b7a6-4b5e-a35d-3cd51bcbae6b'
          ],
          treeHash: '82cf6d7c-b7a6-4b5e-a35d-3cd51bcbae6b',
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
        eventHash: '3-1',
        event: {
          parentHash: [
            'y3',
            'y2',
            'yb',
            'y1'
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
        eventHash: 'b-1',
        event: {
          parentHash: [
            'yb',
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
        eventHash: '1-1',
        event: {
          parentHash: [
            'y1',
            'yb',
            'y2'
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
            'yb',
            'y1'
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
            'b-1',
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
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'b2-1',
        event: {
          parentHash: [
            'b-1',
            '3-1'
          ],
          treeHash: 'b-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'b2'
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
            '2-2'
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
        eventHash: '1-2',
        event: {
          parentHash: [
            '1-1',
            'b2-1',
            '2-2'
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
        eventHash: '2-3',
        event: {
          parentHash: [
            '2-2',
            '1-2',
            '3-2'
          ],
          treeHash: '2-2',
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
      eventHash: '2-3',
      generation: 4
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
}
;

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
