'use strict';

const input = {
  ledgerNodeId: '4',
  history: {
    events: [
      {
        _children: [],
        _parents: [],
        eventHash: 'y4',
        event: {
          parentHash: [
            'b43c7da1-48d2-4a94-9a62-fb0fad7d04d4'
          ],
          treeHash: 'b43c7da1-48d2-4a94-9a62-fb0fad7d04d4',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '4'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'y3',
        event: {
          parentHash: [
            'a605077d-b2f4-4a82-9200-5f81d1946b10'
          ],
          treeHash: 'a605077d-b2f4-4a82-9200-5f81d1946b10',
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
        eventHash: 'y1',
        event: {
          parentHash: [
            '4dc6d153-1917-4844-9339-8215ea9ba986'
          ],
          treeHash: '4dc6d153-1917-4844-9339-8215ea9ba986',
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
            'da8b3bf1-3877-4b80-a260-f533d76c8698'
          ],
          treeHash: 'da8b3bf1-3877-4b80-a260-f533d76c8698',
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
        eventHash: '2-1',
        event: {
          parentHash: [
            'y2',
            'y1',
            'y3',
            'y4'
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
        eventHash: '3-1',
        event: {
          parentHash: [
            'y3',
            'y1',
            'y4'
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
        eventHash: '4-1',
        event: {
          parentHash: [
            'y4',
            'y1',
            'y3'
          ],
          treeHash: 'y4',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '4'
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
        eventHash: '4-2',
        event: {
          parentHash: [
            '4-1',
            '2-1'
          ],
          treeHash: '4-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '4'
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
            'y3',
            'y4'
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
        eventHash: '4-3',
        event: {
          parentHash: [
            '4-2',
            '3-2'
          ],
          treeHash: '4-2',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '4'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '1-2',
        event: {
          parentHash: [
            '1-1'
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
        eventHash: '2-2',
        event: {
          parentHash: [
            '2-1',
            '1-1',
            '4-1'
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
        eventHash: '3-3',
        event: {
          parentHash: [
            '3-2',
            '1-2',
            '4-3'
          ],
          treeHash: '3-2',
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
        eventHash: '2-3',
        event: {
          parentHash: [
            '2-2',
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
      },
      {
        _children: [],
        _parents: [],
        eventHash: '4-4',
        event: {
          parentHash: [
            '4-3',
            '3-3'
          ],
          treeHash: '4-3',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '4'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '1-3',
        event: {
          parentHash: [
            '1-2',
            '2-3',
            '3-3'
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
        eventHash: '3-4',
        event: {
          parentHash: [
            '3-3',
            '1-3',
            '4-4'
          ],
          treeHash: '3-3',
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
        eventHash: '4-5',
        event: {
          parentHash: [
            '4-4',
            '3-4'
          ],
          treeHash: '4-4',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '4'
          }
        }
      }
    ],
    eventMap: {},
    localBranchHead: {
      eventHash: '4-5',
      generation: 6
    }
  },
  electors: [
    {
      id: '1'
    },
    {
      id: '2'
    },
    {
      id: '3'
    },
    {
      id: '4'
    }
  ],
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
