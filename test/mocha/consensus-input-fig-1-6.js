'use strict';

const input = {
  ledgerNodeId: '6',
  history: {
    events: [
      {
        _children: [],
        _parents: [],
        eventHash: 'ypi',
        event: {
          parentHash: [
            '540d367e-d228-4fae-88c0-4d8526910703'
          ],
          treeHash: '540d367e-d228-4fae-88c0-4d8526910703',
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
            '39d711c8-cb19-4e0a-9446-927df3d120f6'
          ],
          treeHash: '39d711c8-cb19-4e0a-9446-927df3d120f6',
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
            'a781db00-22a8-4686-91e5-587ee83b1673'
          ],
          treeHash: 'a781db00-22a8-4686-91e5-587ee83b1673',
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
            '46900219-cb4a-4295-b9d5-d0ee43d1af07'
          ],
          treeHash: '46900219-cb4a-4295-b9d5-d0ee43d1af07',
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
            '753138e9-cce5-410b-a3dd-7780572ecde9'
          ],
          treeHash: '753138e9-cce5-410b-a3dd-7780572ecde9',
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
            '8c6cc692-3852-47a7-862d-e5ca7eacc4b5'
          ],
          treeHash: '8c6cc692-3852-47a7-862d-e5ca7eacc4b5',
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
      },
      {
        _children: [],
        _parents: [],
        eventHash: 'y6',
        event: {
          parentHash: [
            'f09f32e9-3847-4237-82fc-089486d64995'
          ],
          treeHash: 'f09f32e9-3847-4237-82fc-089486d64995',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '6'
          }
        }
      },
      {
        _children: [],
        _parents: [],
        eventHash: '6-1',
        event: {
          parentHash: [
            'y6',
            'pi-2'
          ],
          treeHash: 'y6',
          type: 'ContinuityMergeEvent'
        },
        meta: {
          continuity2017: {
            creator: '6'
          }
        }
      }
    ],
    eventMap: {},
    localBranchHead: {
      eventHash: '6-1',
      generation: 2
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
    },
    {
      id: '6'
    }
  ],
  recoveryElectors: [],
  mode: 'first'
};

input.history.events.forEach(e => input.history.eventMap[e.eventHash] = e);
module.exports = input;
