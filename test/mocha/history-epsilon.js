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
  ss2: ['cp1', 'cp2', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp3: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.delta],
    to: nodes.gamma
  }, callback)],
  cp4: ['ss2', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.gamma],
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss3: ['cp3', 'cp4', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp5: ['ss3', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp6: ['ss3', (results, callback) => api.copyAndMerge({
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
  ss10: ['cp10', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp11: ['cp9', 'ss10', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  cp12: ['cp9', 'ss10', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  // snapshot gamma before copy
  ss11: ['cp11', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  // snapshot beta before copy
  ss12: ['cp12', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.beta}, callback)],
  cp13: ['ss12', 'ss11', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.gamma],
    to: nodes.alpha,
    useSnapshot: true
  }, callback)],
  cp14: ['ss11', 'ss12', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.gamma],
    to: nodes.delta,
    useSnapshot: true
  }, callback)],
  cp15: ['cp13', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.gamma],
    to: nodes.beta
  }, callback)],
  cp16: ['cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.delta],
    to: nodes.gamma
  }, callback)],
  cp17: ['cp16', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.beta
  }, callback)],
  cp18: ['cp15', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.gamma
  }, callback)],
  // snapshot beta before copy
  ss17: ['cp17', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.beta}, callback)],
  // snapshot gamma before copy
  ss18: ['cp18', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.gamma}, callback)],
  cp19: ['ss17', 'ss18', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.gamma],
    to: nodes.alpha
  }, callback)],
  cp20: ['ss17', 'ss18', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.gamma],
    to: nodes.delta
  }, callback)],
  // snapshot alpha before copy
  ss19: ['cp19', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.alpha}, callback)],
  // snapshot delta before copy
  ss20: ['cp20', (results, callback) => api.snapshotEvents(
    {ledgerNode: nodes.delta}, callback)],
  cp21: ['ss19', 'ss20', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.beta,
    useSnapshot: true
  }, callback)],
  cp22: ['ss19', 'ss20', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.gamma,
    useSnapshot: true
  }, callback)],
  cp23: ['cp21', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp24: ['cp22', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
});
