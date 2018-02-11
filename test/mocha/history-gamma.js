/*!
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */

module.exports = (api, consensusApi, eventTemplate, nodes) => ({
  // add a regular event and merge on every node
  regularEvent: callback => api.addEventMultiNode(
    {consensusApi, eventTemplate, nodes}, callback),
  cpa: ['regularEvent', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp1: ['cpa', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'alpha',
    nodes,
    to: 'beta'
  }, callback)],
  cpb: ['regularEvent', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
  cp2: ['cpb', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'delta',
    nodes,
    to: 'gamma'
  }, callback)],
  // snapshot gamma before copy
  ss1: ['cp1', 'cp2', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp3: ['ss1', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'delta'],
    nodes,
    to: 'gamma'
  }, callback)],
  cp4: ['ss1', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'gamma'],
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss2: ['cp3', 'cp4', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp5: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp6: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  cp7: ['cp5', 'cp6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'alpha'
  }, callback)],
  cp8: ['cp5', 'cp6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'delta'
  }, callback)],
  cp9: ['cp7', 'cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'beta'
  }, callback)],
  cp10: ['cp7', 'cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'gamma'
  }, callback)],
  cp11: ['cp9', 'cp10', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'alpha'
  }, callback)],
  cp12: ['cp9', 'cp10', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'delta'
  }, callback)],
  cp13: ['cp11', 'cp12', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'beta'
  }, callback)],
  cp14: ['cp11', 'cp12', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'gamma'
  }, callback)],
  cp15: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'alpha'
  }, callback)],
  cp16: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'delta'
  }, callback)],
  cp17: ['cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'beta'
  }, callback)],
  cp18: ['cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'gamma'
  }, callback)],
  cp19: ['cp17', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp20: ['cp18', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
});
