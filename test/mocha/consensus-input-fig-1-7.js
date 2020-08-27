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
            'aa50329d-7481-4ec8-99f7-1cb25c1b201f'
          ],
          treeHash: 'aa50329d-7481-4ec8-99f7-1cb25c1b201f',
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
            'yb'
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
        eventHash: 'yalpha',
        event: {
          parentHash: [
            '623dc8f8-0195-4647-a479-9912f63c72ce'
          ],
          treeHash: '623dc8f8-0195-4647-a479-9912f63c72ce',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'alpha'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'b2-1',
        event: {
          parentHash: [
            'b-1'
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
        eventHash: 'b1-2',
        event: {
          parentHash: [
            'b1-1'
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
        eventHash: 'y1',
        event: {
          parentHash: [
            '593aadc5-7605-4fa0-8b7c-023dd691bfc1'
          ],
          treeHash: '593aadc5-7605-4fa0-8b7c-023dd691bfc1',
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
        eventHash: 'b2-2',
        event: {
          parentHash: [
            'b2-1',
            'yalpha'
          ],
          treeHash: 'b2-1',
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
        eventHash: 'y2',
        event: {
          parentHash: [
            '8230632a-319f-49e4-9c0c-1bd70b902a02'
          ],
          treeHash: '8230632a-319f-49e4-9c0c-1bd70b902a02',
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
        eventHash: '1-1',
        event: {
          parentHash: [
            'y1',
            'b1-2'
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
        eventHash: 'yh',
        event: {
          parentHash: [
            '6defe6e6-02d1-4e54-9b51-b152e82b6fa5'
          ],
          treeHash: '6defe6e6-02d1-4e54-9b51-b152e82b6fa5',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'h'
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
            'b2-2'
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
        eventHash: 'h-1',
        event: {
          parentHash: [
            'yh',
            '1-1'
          ],
          treeHash: 'yh',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'h'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'h-2',
        event: {
          parentHash: [
            'h-1',
            '2-1'
          ],
          treeHash: 'h-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'h'
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
            'h-2'
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
      id: 'h'
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
}
;

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
