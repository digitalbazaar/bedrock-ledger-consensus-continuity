/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
const {callbackify} = require('util');

module.exports = ({api, consensusApi, eventTemplate, nodes, opTemplate}) => ({
  // add a regular event and merge on every node
  regularEvent: callback => callbackify(api.addEventMultiNode)(
    {consensusApi, eventTemplate, nodes, opTemplate}, callback),
  cpa: ['regularEvent', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp1: ['cpa', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'alpha',
    nodes,
    to: 'beta'
  }, callback)],
  cpb: ['regularEvent', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
  cp2: ['cpb', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'delta',
    nodes,
    to: 'gamma'
  }, callback)],
  // snapshot gamma before copy
  ss2: ['cp1', 'cp2', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp3: ['ss2', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['beta', 'delta'],
    nodes,
    to: 'gamma'
  }, callback)],
  cp4: ['ss2', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['alpha', 'gamma'],
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss3: ['cp3', 'cp4', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp5: ['ss3', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp6: ['ss3', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  cp7: ['cp6', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp8: ['cp5', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
  cp9: ['cp7', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'alpha',
    nodes,
    to: 'beta'
  }, callback)],
  cp10: ['cp8', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'delta',
    nodes,
    to: 'gamma'
  }, callback)],
  // snapshot gamma before copy
  ss10: ['cp9', 'cp10', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp11: ['ss10', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  cp12: ['ss10', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss11: ['cp11', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  // snapshot beta before copy
  ss12: ['cp12', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.beta}, callback)],
  cp13: ['ss12', 'ss11', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'alpha',
    useSnapshot: true
  }, callback)],
  cp14: ['ss11', 'ss12', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'delta',
    useSnapshot: true
  }, callback)],
  cp15: ['cp13', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['alpha', 'gamma'],
    nodes,
    to: 'beta'
  }, callback)],
  cp16: ['cp14', 'cp15', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['beta', 'delta'],
    nodes,
    to: 'gamma'
  }, callback)],
  cp17: ['cp16', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'beta'
  }, callback)],
  cp18: ['cp17', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'gamma'
  }, callback)],
  // snapshot beta before copy
  ss17: ['cp17', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.beta}, callback)],
  // snapshot gamma before copy
  ss18: ['cp18', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp19: ['ss17', 'ss18', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'alpha'
  }, callback)],
  cp20: ['ss17', 'ss18', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['beta', 'gamma'],
    nodes,
    to: 'delta'
  }, callback)],
  // snapshot alpha before copy
  ss19: ['cp19', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.alpha}, callback)],
  // snapshot delta before copy
  ss20: ['cp20', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.delta}, callback)],
  cp21: ['ss19', 'ss20', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'beta',
    useSnapshot: true
  }, callback)],
  cp22: ['ss19', 'ss20', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: ['alpha', 'delta'],
    nodes,
    to: 'gamma',
    useSnapshot: true
  }, callback)],
  cp23: ['cp21', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'beta',
    nodes,
    to: 'alpha'
  }, callback)],
  cp24: ['cp22', (results, callback) => callbackify(api.copyAndMerge)({
    consensusApi,
    from: 'gamma',
    nodes,
    to: 'delta'
  }, callback)],
});
