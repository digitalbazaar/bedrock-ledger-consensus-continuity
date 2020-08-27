'use strict';

const input = {
  ledgerNodeId: 'pi',
  history: {
    events: [
      {
        _children: [],
        _parents: [],
        eventHash: 'ypi',
        event: {
          parentHash: [
            'd617de47-348d-4f18-b78d-41f242b61d4c'
          ],
          treeHash: 'd617de47-348d-4f18-b78d-41f242b61d4c',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'pi'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'pi-1',
        event: {
          parentHash: [
            'ypi'
          ],
          treeHash: 'ypi',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'pi'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'y3',
        event: {
          parentHash: [
            'e7b95d21-5793-44ca-a523-8cc0d9f191dd'
          ],
          treeHash: 'e7b95d21-5793-44ca-a523-8cc0d9f191dd',
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
        eventHash: 'y5',
        event: {
          parentHash: [
            '03192c58-ee15-44ca-a80c-462b256e037b'
          ],
          treeHash: '03192c58-ee15-44ca-a80c-462b256e037b',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '5'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'y4',
        event: {
          parentHash: [
            '46d16203-cfd8-410b-aefb-ca56d5ea8340'
          ],
          treeHash: '46d16203-cfd8-410b-aefb-ca56d5ea8340',
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
        eventHash: '3-1',
        event: {
          parentHash: [
            'y3',
            'pi-1'
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
        eventHash: 'y2',
        event: {
          parentHash: [
            '58b24569-c259-49a1-9285-cf4187bfc077'
          ],
          treeHash: '58b24569-c259-49a1-9285-cf4187bfc077',
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
            '7470044c-2d3c-4935-aee7-03264551d905'
          ],
          treeHash: '7470044c-2d3c-4935-aee7-03264551d905',
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
        eventHash: '5-1',
        event: {
          parentHash: [
            'y5',
            'pi-1'
          ],
          treeHash: 'y5',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '5'
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
            'pi-1'
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
        eventHash: '2-1',
        event: {
          parentHash: [
            'y2',
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
        eventHash: '1-1',
        event: {
          parentHash: [
            'y1',
            'pi-1'
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
        eventHash: 'pi-2',
        event: {
          parentHash: [
            'pi-1',
            '1-1',
            '2-1',
            '4-1',
            '5-1'
          ],
          treeHash: 'pi-1',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: 'pi'
          }
        }
      }
    ],
    eventMap: {},
    localBranchHead: {
      eventHash: 'pi-2',
      generation: 3
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
      id: 'pi'
    },
    {
      id: '4'
    },
    {
      id: '5'
    }
  ],
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
