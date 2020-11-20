/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
const {callbackify} = require('util');

module.exports = ({api, consensusApi, eventTemplate, nodes, opTemplate}) => ({
  // add a regular event and merge on every node
  regularEvent: callback => callbackify(api.addEventMultiNode)(
    {consensusApi, eventTemplate, nodes, opTemplate}, callback),
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
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp8: ['cp5', 'cp6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
  cp9: ['cp7', 'cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'alpha',
    nodes,
    to: 'beta'
  }, callback)],
  cp10: ['cp7', 'cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'delta',
    nodes,
    to: 'gamma'
  }, callback)],
  // snapshot gamma before copy
  ss3: ['cp9', 'cp10', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp11: ['ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp12: ['ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss4: ['cp11', 'cp12', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp13: ['ss4', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp14: ['ss4', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  cp15: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp16: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
  cp17: ['cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'alpha',
    nodes,
    to: 'beta'
  }, callback)],
  cp18: ['cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'delta',
    nodes,
    to: 'gamma'
  }, callback)],
  // snapshot gamma before copy
  ss5: ['cp17', 'cp18', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp19: ['ss5', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp20: ['ss5', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss6: ['cp19', 'cp20', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp21: ['ss6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp22: ['ss6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  cp23: ['cp21', 'cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp24: ['cp21', 'cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
  cp25: ['cp23', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'alpha',
    nodes,
    to: 'beta'
  }, callback)],
  cp26: ['cp24', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'delta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp27: ['cp25', 'cp26', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta'
  }, callback)],
  cp28: ['cp27', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp29: ['cp27', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp30: ['cp28', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
});
