/*!
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */

module.exports = (api, consensusApi, eventTemplate, nodes) => ({
  // add a regular event and merge on every node
  regularEvent: callback => api.addEventMultiNode(
    {consensusApi, eventTemplate, nodes}, callback),
  cpa: ['regularEvent', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp1: ['cpa', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.alpha,
    to: nodes.beta
  }, callback)],
  cpb: ['regularEvent', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
  cp2: ['cpb', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.delta,
    to: nodes.gamma
  }, callback)],
  // snapshot gamma before copy
  ss1: ['cp1', 'cp2', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp3: ['ss1', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.delta],
    to: nodes.gamma
  }, callback)],
  cp4: ['ss1', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.gamma],
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss2: ['cp3', 'cp4', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp5: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp6: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  cp7: ['cp6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp8: ['cp5', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
  cp9: ['cp7', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.alpha,
    to: nodes.beta
  }, callback)],
  cp10: ['cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.delta,
    to: nodes.gamma
  }, callback)],
  // snapshot gamma before copy
  ss3: ['cp10', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp11: ['cp9', 'ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp12: ['cp9', 'ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss4: ['cp11', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp13: ['ss4', 'cp12', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp14: ['cp12', 'ss4', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  cp15: ['ss4', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.alpha,
    useSnapshot: true
  }, callback)],
  cp16: ['cp12', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.delta
  }, callback)],
  cp17: ['cp14', 'cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.beta
  }, callback)],
  cp18: ['cp13', 'cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.gamma
  }, callback)],
  ss5: ['cp18', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp19: ['cp17', 'ss5', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp20: ['cp17', 'ss5', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  cp21: ['cp15', 'cp20', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp22: ['cp16', 'cp19', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
  cp23: ['cp21', 'cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.delta,
    to: nodes.alpha
  }, callback)],
  cp24: ['cp20', 'cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.delta,
    to: nodes.beta
  }, callback)],
  cp25: ['cp19', 'cp21', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.alpha,
    to: nodes.gamma
  }, callback)],
  cp26: ['cp21', 'cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.alpha,
    to: nodes.delta
  }, callback)],
});
