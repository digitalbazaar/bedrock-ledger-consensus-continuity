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
  cp7: ['cp5', 'cp6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp8: ['cp5', 'cp6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
  cp9: ['cp7', 'cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.alpha,
    to: nodes.beta
  }, callback)],
  cp10: ['cp7', 'cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.delta,
    to: nodes.gamma
  }, callback)],
  // snapshot gamma before copy
  ss3: ['cp9', 'cp10', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp11: ['ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp12: ['ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss4: ['cp11', 'cp12', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp13: ['ss4', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp14: ['ss4', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  cp15: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp16: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
  cp17: ['cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.alpha,
    to: nodes.beta
  }, callback)],
  cp18: ['cp15', 'cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.delta,
    to: nodes.gamma
  }, callback)],
  // snapshot gamma before copy
  ss5: ['cp17', 'cp18', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp19: ['ss5', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp20: ['ss5', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss6: ['cp19', 'cp20', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp21: ['ss6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp22: ['ss6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  cp23: ['cp21', 'cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp24: ['cp21', 'cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
});
