/*!
 * Copyright (c) 2018-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const fs = require('fs').promises;
const path = require('path');

const api = {};
module.exports = api;

function eventsToIds(events) {
  if(!events) {
    return [];
  }
  return events.map(e => e.eventHash);
}

/**
 * Create event JSON data to be used with visualization tools.
 *
 * @return event visualization JSON data
 */
api.visualizationData = ({
  /* eslint-disable-next-line no-unused-vars */
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
    if(e._c.parents.length === 0) {
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
  //debugger;
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
    e._c.parents.forEach(pe => {
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

api.saveVisualizationDataD3 = async ({
  tag, historyId, nodeId, build, history, branches, proof, nodes
}) => {
  const filename = `./data/${tag}-${historyId}-${nodeId}.json`;

  const data = api.visualizationData({
    nodeId, build, history, branches, proof, nodes
  });

  /*
  console.log('BUILD', build);
  console.log('HISTORY', history);
  console.log('BRANCHES', branches);
  console.log('PROOF', proof);
  console.log('X', allXs);
  console.log('Y', allYs);
  console.log('YCandidates', yCandidates);
  */

  await fs.writeFile(filename, JSON.stringify(data, null, 2));
  console.log(`[viz] wrote data for d3: ${filename}`);

  //debugger;
};

const {inspect} = require('util');
/* eslint-disable-next-line no-unused-vars */
function _dbg(msg, json) {
  console.log(msg, inspect(json, {depth: 16, colors: true}));
}

/**
 * Create event JSON data to be used with visualization tools.
 *
 * Works with "input" style data.
 *
 * @return event visualization JSON data
 */
api.testInputData = ({
  historyId,
  nodeId,
  history
}) => {
  console.log('[viz] generating viz data', {historyId, nodeId});
  //_dbg('HISTORY', history);
  //const allXs = proof.consensus.map(p => p.x.eventHash);
  //const allYs = proof.consensus.map(p => p.y.eventHash);
  //const yCandidates = proof.yCandidates.map(c => c.eventHash);

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
  const indexMap = new Map();
  history.events.forEach((e, i) => {
    indexMap.set(e.eventHash, i);
  });
  // add other creators
  /*
  history.events.forEach((e, i) => {
    indexMap.set(e.eventHash, i);
    const creator = e.meta.continuity2017.creator;
    if(!indexMap.has(creator)) {
      indexMap.set(creator, indexMap.size);
    }
  });
  */
  //_dbg('INDEXMAP', indexMap);

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
  const creatorIndexMap = new Map();
  // find roots (no parents), index, and add root constraints
  history.events.forEach((e, i) => {
    //if(e._c.parents.length === 0) {
    // check for parentHashes not listed as eventHashes
    if(!e.event.parentHash.every(p => indexMap.has(p))) {
      _addRoot(e, i);
      creatorIndexMap.set(e.meta.continuity2017.creator, i);
    }
  });
  //console.log('CM1', historyId, creatorIndexMap);

  // map from creator to node name
  const creatorNameMap = {};
  //Object.keys(nodes).forEach(name => {
  //  creatorNameMap[nodes[name].creatorId] = name;
  //});
  // FIXME: current 'input' tests have simple creator names
  // find all creator names
  history.events.forEach(e => {
    const creator = e.meta.continuity2017.creator;
    creatorNameMap[creator] = creator;
  });
  //console.log('CM2', historyId, creatorIndexMap);

  // map from event hash to name
  function _eventNameForHash(eventHash) {
    // FIXME: current 'input' tests have simple event names
    return eventHash;
  }

  // process all events
  //debugger;
  history.events.forEach((e, i) => {
    data.nodes.push({
      //name: e.eventHash,
      //name: build.copyMergeHashesIndex[e.eventHash] || 'XXX',
      name: _eventNameForHash(e.eventHash),
      // FIXME
      width: 60,
      height: 50,
      eventHash: e.eventHash,
      //isX: allXs.includes(e.eventHash),
      isX: false,
      //isY: allYs.includes(e.eventHash),
      isY: false,
      //isYCandidate: yCandidates.includes(e.eventHash),
      isYCandidate: false,
      creatorName: creatorNameMap[e.meta.continuity2017.creator]
    });
    //e._c.parents.forEach(pe => {
    //console.log('XXX', historyId, creatorIndexMap);
    e.event.parentHash.forEach(pe => {
      if(!indexMap.has(pe)) {
        return;
      }
      //const pi = indexMap.get(pe.eventHash);
      const pi = indexMap.get(pe);
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
      /*
      if(!creatorIndexMap.has(e.meta.continuity2017.creator)) {
        throw new Error(`missing creator "${e.meta.continuity2017.creator}"`);
      }
      data.constraints.push({
        axis: 'x',
        left: creatorIndexMap.get(e.meta.continuity2017.creator),
        right: i,
        gap: 0,
        equality: true
      });
      */
      if(creatorIndexMap.has(e.meta.continuity2017.creator)) {
        data.constraints.push({
          axis: 'x',
          left: creatorIndexMap.get(e.meta.continuity2017.creator),
          right: i,
          gap: 0,
          equality: true
        });
      } else {
        console.log('missing creator', {
          historyId,
          nodeId,
          creator: e.meta.continuity2017.creator,
          eventHash: e.eventHash,
        });
      }
    });
  });

  return data;
};

api.saveTestInputDataForD3 = async ({
  directory, tag, historyId, nodeId, history
}) => {
  const filename = path.join(
    directory, `${tag}--${historyId}--${nodeId}--input.json`);

  const data = api.testInputData({historyId, nodeId, history});

  /*
  console.log('BUILD', build);
  console.log('HISTORY', history);
  console.log('BRANCHES', branches);
  console.log('PROOF', proof);
  console.log('X', allXs);
  console.log('Y', allYs);
  console.log('YCandidates', yCandidates);
  */

  await fs.writeFile(filename, JSON.stringify(data, null, 2));
  console.log(`[viz] wrote test input data for d3: ${filename}`);

  //debugger;

  return {
    filename
  };
};

/**
 * Create event JSON data to be used with visualization tools.
 *
 * Outputs data for timeline UI.
 *
 * @param history - input to findConsensus
 * @param consensus - output from findConsensus
 * @param display - hints for display
 *
 * @return event visualization JSON data
 */
api.testOutputDataForTimeline = ({
  //nodeId, build, history, branches, proof, nodes
  /* eslint-disable-next-line no-unused-vars */
  historyId, nodeId, history, consensus, display
}) => {
  //const allXs = proof.consensus.map(p => p.x.eventHash);
  //const allYs = proof.consensus.map(p => p.y.eventHash);
  //const yCandidates = proof.yCandidates.map(c => c.eventHash);

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
  const data = {
    historyId,
    nodeId,
    nodes: []
  };
  if(display) {
    data.display = display;
  }

  // map from creator to node name
  //const creatorNameMap = {};
  //Object.keys(nodes).forEach(name => {
  //  creatorNameMap[nodes[name].creatorId] = name;
  //});

  // process all events
  //debugger;
  history.events.forEach(e => {
    const _mostRecentWitnessAncestors = {};
    if(e._c.mostRecentWitnessAncestors) {
      for(const [key, value] of e._c.mostRecentWitnessAncestors) {
        _mostRecentWitnessAncestors[key] =
          typeof value === 'object' ? value.eventHash : value;
      }
    }
    data.nodes.push({
      id: e.eventHash,
      //name: build.copyMergeHashesIndex[e.eventHash] || null,
      name: e.eventHash,
      //eventHash: e.eventHash,
      //isX: allXs.includes(e.eventHash),
      //isY: allYs.includes(e.eventHash),
      //isYCandidate: yCandidates.includes(e.eventHash),
      //creatorName: creatorNameMap[e.meta.continuity2017.creator],
      creatorName: e.meta.continuity2017.creator,
      parents: eventsToIds(e._c.parents),
      support: eventsToIds(e._c.support),
      proposalEndorsement: eventsToIds(e._c.proposalEndorsement),
      endorsesProposal: eventsToIds(e._c.endorsesProposal),
      mostRecentWitnessAncestors: _mostRecentWitnessAncestors,
      decision: e._c.decision,
      proposal: e._c.proposal ? [e._c.proposal.eventHash] : [],
      treeParent: e._c.treeParent ? [e._c.treeParent.eventHash] : []
    });
  });

  return data;
};

api.saveTestOutputDataForTimeline = async ({
  directory, tag, historyId, nodeId, history, consensus, display
}) => {
  const filename = path.join(
    directory, `${tag}--${historyId}--${nodeId}--output.json`);

  const data = api.testOutputDataForTimeline({
    historyId, nodeId, history, consensus, display
  });

  /*
  console.log('BUILD', build);
  console.log('HISTORY', history);
  console.log('BRANCHES', branches);
  console.log('PROOF', proof);
  console.log('X', allXs);
  console.log('Y', allYs);
  console.log('YCandidates', yCandidates);
  */

  await fs.writeFile(filename, JSON.stringify(data, null, 2));
  console.log(`[viz] wrote test input data for timeline: ${filename}`);

  //debugger;

  return {
    filename
  };
};

/**
 * Save a *index.js file with result data.
 *
 * Each info object:
 *   "label": string, optional
 *   "url": url/file with data, optional
 *   "data": plain JSON data
 *
 * @param jsName - window[jsName] to use
 * @param info - array of objects with result data
 */
api.saveIndexJS = async ({
  directory, tag, jsName, info
}) => {
  /*
  const filename = `./data/${tag}-input-index.json`;

  await fs.writeFile(filename, JSON.stringify({
    tag,
    filenames
  }, null, 2));
  */
  const filename = path.join(
    directory, `${tag}--index.js`);

  await fs.writeFile(filename,
    `window.${jsName} = ${JSON.stringify(info, null, 2)};`);
  console.log(`[viz] wrote index JS: ${filename}`);
};

/**
 * Create event JSON data to be used with visualization tools.
 *
 * @return event visualization JSON data
 */
api.visualizationDataTimeline = ({
  /* eslint-disable-next-line no-unused-vars */
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
  const data = {nodes: []};

  // map from creator to node name
  const creatorNameMap = {};
  Object.keys(nodes).forEach(name => {
    creatorNameMap[nodes[name].creatorId] = name;
  });

  // process all events
  //debugger;
  history.events.forEach(e => {
    data.nodes.push({
      id: e.eventHash,
      name: build.copyMergeHashesIndex[e.eventHash] || null,
      //eventHash: e.eventHash,
      isX: allXs.includes(e.eventHash),
      isY: allYs.includes(e.eventHash),
      isYCandidate: yCandidates.includes(e.eventHash),
      creatorName: creatorNameMap[e.meta.continuity2017.creator],
      parents: eventsToIds(e._c.parents),
      support: eventsToIds(e._c.support),
      proposalEndorsement: eventsToIds(e._c.proposalEndorsement),
      endorsesProposal: eventsToIds(e._c.endorsesProposal)
    });
  });

  return data;
};

api.saveVisualizationDataTimeline = async ({
  tag, historyId, nodeId, build, history, branches, proof, nodes
}) => {
  const filename = `./data/${tag}-${historyId}-${nodeId}-tl.json`;

  const data = api.visualizationDataTimeline({
    nodeId, build, history, branches, proof, nodes
  });

  /*
  console.log('BUILD', build);
  console.log('HISTORY', history);
  console.log('BRANCHES', branches);
  console.log('PROOF', proof);
  console.log('X', allXs);
  console.log('Y', allYs);
  console.log('YCandidates', yCandidates);
  */

  await fs.writeFile(filename, JSON.stringify(data, null, 2));
  console.log(`[viz] wrote timeline data: ${filename}`);

  //debugger;
};

api.saveVisualizationData = async (...args) => {
  await Promise.all([
    api.saveVisualizationDataD3(...args),
    api.saveVisualizationDataTimeline(...args)
  ]);
};
