/*!
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');

module.exports = (api, consensusApi, eventTemplate, nodes) => ({
  // add a regular event and merge on every node
  regularEvent: callback => async.each(nodes, (n, callback) =>
    api.addEventAndMerge(
      {consensusApi, eventTemplate, ledgerNode: n}, callback), callback),
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
    from: [nodes.beta, nodes.gamma],
    to: nodes.alpha
  }, callback)],
  cp8: ['cp5', 'cp6', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.gamma],
    to: nodes.delta
  }, callback)],
  cp9: ['cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.beta
  }, callback)],
  cp10: ['cp8', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.gamma
  }, callback)],
  cp11: ['cp9', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.beta,
    to: nodes.alpha
  }, callback)],
  cp12: ['cp10', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: nodes.gamma,
    to: nodes.delta
  }, callback)],
  cp13: ['cp11', 'cp12', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.beta
  }, callback)],
  cp14: ['cp11', 'cp12', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.alpha, nodes.delta],
    to: nodes.gamma
  }, callback)],
  cp15: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.gamma],
    to: nodes.alpha
  }, callback)],
  cp16: ['cp13', 'cp14', (results, callback) => api.copyAndMerge({
    consensusApi,
    from: [nodes.beta, nodes.gamma],
    to: nodes.delta
  }, callback)],
});
