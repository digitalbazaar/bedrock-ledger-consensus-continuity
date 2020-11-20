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
  ss2: ['cp1', 'cp2', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp3: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'delta'],
    nodes,
    to: 'gamma'
  }, callback)],
  cp4: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'gamma'],
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss3: ['cp3', 'cp4', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp5: ['ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp6: ['ss3', (results, callback) => api.copyAndMerge({
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
  ss10: ['cp9', 'cp10', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp11: ['ss10', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp12: ['ss10', (results, callback) => api.copyAndMerge({
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
  cp15: ['cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'delta'
  }, callback)],
  cp16: ['cp15', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'delta'],
    nodes,
    to: 'alpha'
  }, callback)],
  cp17: ['cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'beta'
  }, callback)],
  // snapshot beta before copy
  ss17: ['cp17', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.beta}, callback)],
  cp18: ['cp17', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'delta'
  }, callback)],
  // snapshot delta before copy
  ss18: ['cp18', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.delta}, callback)],
  cp19: ['ss17', 'ss18', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'delta'],
    nodes,
    to: 'alpha',
    useSnapshot: true
  }, callback)],
  // snapshot alpha before copy
  ss19: ['cp19', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.alpha}, callback)],
  cp20: ['ss18', 'ss19', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'gamma',
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss20: ['cp20', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp21: ['ss20', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  // snapshot beta before copy
  ss21: ['cp21', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.beta}, callback)],
  cp22: ['ss20', 'ss21', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'alpha',
    useSnapshot: true
  }, callback)],
  cp23: ['ss20', 'ss21', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'delta',
    useSnapshot: true
  }, callback)],
  cp24: ['cp23', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: 'delta',
    nodes,
    to: 'gamma'
  }, callback)],
});
