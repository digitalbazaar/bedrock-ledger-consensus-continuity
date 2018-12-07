/*!
 * Copyright (c) 2018 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const api = {};
module.exports = api;

/**
 * Create event JSON data to be used with visualization tools.
 *
 * @return event visualization JSON data
 */
api.visualizationData = ({
  nodeId, build, history, branches, proof, nodes
}) => {
  const allXs = proof.consensus.map(p => p.x.eventHash);
  const allYs = proof.consensus.map(p => p.y.eventHash);
  const yCandidates = proof.yCandidates.map(c => c.eventHash);

  /*
  console.log('BUILD', build);
  console.log('HISTORY', history);
  console.log('BRANCHES', branches);
  console.log('PROOF', proof);
  console.log('X', allXs);
  console.log('Y', allYs);
  console.log('YCandidates', yCandidates);
  */

  // viz data
  const data = {nodes: [], links: [], constraints: []};

  // map from hash to viz node index
  const indexMap = {};
  history.events.forEach((e, i) => indexMap[e.eventHash] = i);

  // align roots
  const roots = [];
  const rootConstraint = {
    type: 'alignment',
    axis: 'y',
    offsets: []
  };
  data.constraints.push(rootConstraint);
  function _addRoot(e, i) {
    roots.push(e);
    rootConstraint.offsets.push({node: i, offset: 0});
  }

  // map of creator to root viz node index
  const creatorIndexMap = {};
  // find roots (no parents), index, and add root constraints
  history.events.forEach((e, i) => {
    if(e._parents.length === 0) {
      _addRoot(e, i);
      creatorIndexMap[e.meta.continuity2017.creator] = i;
    }
  });

  // map from creator to node name
  const creatorNameMap = {};
  Object.keys(nodes).forEach(name => {
    creatorNameMap[nodes[name].creatorId] = name;
  });

  // process all events
  history.events.forEach((e, i) => {
    data.nodes.push({
      //name: e.eventHash,
      name: build.copyMergeHashesIndex[e.eventHash] || 'XXX',
      width: 60,
      height: 50,
      eventHash: e.eventHash,
      isX: allXs.includes(e.eventHash),
      isY: allYs.includes(e.eventHash),
      isYCandidate: yCandidates.includes(e.eventHash),
      creatorName: creatorNameMap[e.meta.continuity2017.creator]
    });
    e._parents.forEach(pe => {
      const pi = indexMap[pe.eventHash];
      data.links.push({
        source: pi,
        target: i
      });
      data.constraints.push({
        axis: 'y',
        left: pi,
        right: i,
        gap: 50
      });
      data.constraints.push({
        axis: 'x',
        left: creatorIndexMap[e.meta.continuity2017.creator],
        right: i,
        gap: 0,
        equality: true
      });
    });
  });

  return data;
};
