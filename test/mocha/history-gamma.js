/*!
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */

module.exports = async ({
  api, consensusApi, eventTemplate, nodes, opTemplate
}) => {
  const results = {};

  function _check({name, deps}) {
    // check not overwriting name
    if(name in results) {
      throw new Error(`Name already exists: ${name}`);
    }
    // check deps exist
    for(const d of deps) {
      if(!(d in results)) {
        throw new Error(`Missing test dependency: ${d}`);
      }
    }
  }
  async function cm({name, deps, from, to, useSnapshot = false}) {
    _check({name, deps});
    results[name] = await api.copyAndMerge({
      consensusApi,
      from,
      nodes,
      to,
      useSnapshot
    });
  }
  async function ss({name, deps, node}) {
    _check({name, deps});
    results[name] = await api.snapshotEvents({ledgerNode: nodes[node]});
  }

  // add a regular event and merge on every node
  results.regularEvent = await api.addEventMultiNode({
    consensusApi, eventTemplate, nodes, opTemplate
  });
  await cm({
    name: 'cpa',
    deps: ['regularEvent'],
    from: 'beta',
    to: 'alpha'
  });
  await cm({
    name: 'cp1',
    deps: ['cpa'],
    from: 'alpha',
    to: 'beta'
  });
  await cm({
    name: 'cpb',
    deps: ['regularEvent'],
    from: 'gamma',
    to: 'delta'
  });
  await cm({
    name: 'cp2',
    deps: ['cpb'],
    from: 'delta',
    to: 'gamma'
  });
  // snapshot gamma before copy
  await ss({
    name: 'ss1',
    deps: ['cp1', 'cp2'],
    node: 'gamma'
  });
  await cm({
    name: 'cp3',
    deps: ['ss1'],
    from: ['beta', 'delta'],
    to: 'gamma'
  });
  await cm({
    name: 'cp4',
    deps: ['ss1'],
    from: ['alpha', 'gamma'],
    to: 'beta',
    useSnapshot: true
  });
  // snapshot gamma before copy
  await ss({
    name: 'ss2',
    deps: ['cp3', 'cp4'],
    node: 'gamma'
  });
  await cm({
    name: 'cp5',
    deps: ['ss2'],
    from: 'beta',
    to: 'gamma'
  });
  await cm({
    name: 'cp6',
    deps: ['ss2'],
    from: 'gamma',
    to: 'beta',
    useSnapshot: true
  });
  await cm({
    name: 'cp7',
    deps: ['cp5', 'cp6'],
    from: ['beta', 'gamma'],
    to: 'alpha'
  });
  await cm({
    name: 'cp8',
    deps: ['cp5', 'cp6'],
    from: ['beta', 'gamma'],
    to: 'delta'
  });
  await cm({
    name: 'cp9',
    deps: ['cp7', 'cp8'],
    from: ['alpha', 'delta'],
    to: 'beta'
  });
  await cm({
    name: 'cp10',
    deps: ['cp7', 'cp8'],
    from: ['alpha', 'delta'],
    to: 'gamma'
  });
  await cm({
    name: 'cp11',
    deps: ['cp9', 'cp10'],
    from: ['beta', 'gamma'],
    to: 'alpha'
  });
  await cm({
    name: 'cp12',
    deps: ['cp9', 'cp10'],
    from: ['beta', 'gamma'],
    to: 'delta'
  });
  await cm({
    name: 'cp13',
    deps: ['cp11', 'cp12'],
    from: ['alpha', 'delta'],
    to: 'beta'
  });
  await cm({
    name: 'cp14',
    deps: ['cp11', 'cp12'],
    from: ['alpha', 'delta'],
    to: 'gamma'
  });
  await cm({
    name: 'cp15',
    deps: ['cp13', 'cp14'],
    from: ['beta', 'gamma'],
    to: 'alpha'
  });
  await cm({
    name: 'cp16',
    deps: ['cp13', 'cp14'],
    from: ['beta', 'gamma'],
    to: 'delta'
  });
  await cm({
    name: 'cp17',
    deps: ['cp15', 'cp16'],
    from: ['alpha', 'delta'],
    to: 'beta'
  });
  await cm({
    name: 'cp18',
    deps: ['cp15', 'cp16'],
    from: ['alpha', 'delta'],
    to: 'gamma'
  });
  await cm({
    name: 'cp19',
    deps: ['cp17'],
    from: 'beta',
    to: 'alpha'
  });
  await cm({
    name: 'cp20',
    deps: ['cp18'],
    from: 'gamma',
    to: 'delta'
  });
  return results;
};
