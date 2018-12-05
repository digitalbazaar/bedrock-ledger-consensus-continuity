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
    name: 'ss2',
    deps: ['cp1', 'cp2'],
    node: 'gamma'
  });
  await cm({
    name: 'cp3',
    deps: ['ss2'],
    from: ['beta', 'delta'],
    to: 'gamma'
  });
  await cm({
    name: 'cp4',
    deps: ['ss2'],
    from: ['alpha', 'gamma'],
    to: 'beta',
    useSnapshot: true
  });
  // snapshot gamma before copy
  await ss({
    name: 'ss3',
    deps: ['cp3', 'cp4'],
    node: 'gamma'
  });
  await cm({
    name: 'cp5',
    deps: ['ss3'],
    from: 'beta',
    to: 'gamma'
  });
  await cm({
    name: 'cp6',
    deps: ['ss3'],
    from: 'gamma',
    to: 'beta',
    useSnapshot: true
  });
  await cm({
    name: 'cp7',
    deps: ['cp5', 'cp6'],
    from: 'beta',
    to: 'alpha'
  });
  await cm({
    name: 'cp8',
    deps: ['cp5', 'cp6'],
    from: 'gamma',
    to: 'delta'
  });
  await cm({
    name: 'cp9',
    deps: ['cp7', 'cp8'],
    from: 'alpha',
    to: 'beta'
  });
  await cm({
    name: 'cp10',
    deps: ['cp7', 'cp8'],
    from: 'delta',
    to: 'gamma'
  });
  // snapshot gamma before copy
  await ss({
    name: 'ss10',
    deps: ['cp9', 'cp10'],
    node: 'gamma'
  });
  await cm({
    name: 'cp11',
    deps: ['ss10'],
    from: 'beta',
    to: 'gamma'
  });
  await cm({
    name: 'cp12',
    deps: ['ss10'],
    from: 'gamma',
    to: 'beta',
    useSnapshot: true
  });
  // snapshot gamma before copy
  await ss({
    name: 'ss4',
    deps: ['cp11', 'cp12'],
    node: 'gamma'
  });
  await cm({
    name: 'cp13',
    deps: ['ss4'],
    from: 'beta',
    to: 'gamma'
  });
  await cm({
    name: 'cp14',
    deps: ['ss4'],
    from: 'gamma',
    to: 'beta',
    useSnapshot: true
  });
  await cm({
    name: 'cp15',
    deps: ['cp14'],
    from: 'beta',
    to: 'delta'
  });
  await cm({
    name: 'cp16',
    deps: ['cp15'],
    from: ['beta', 'delta'],
    to: 'alpha'
  });
  await cm({
    name: 'cp17',
    deps: ['cp16'],
    from: ['alpha', 'delta'],
    to: 'beta'
  });
  // snapshot beta before copy
  await ss({
    name: 'ss17',
    deps: ['cp17'],
    node: 'beta'
  });
  await cm({
    name: 'cp18',
    deps: ['cp17'],
    from: 'beta',
    to: 'delta'
  });
  // snapshot delta before copy
  await ss({
    name: 'ss18',
    deps: ['cp18'],
    node: 'delta'
  });
  await cm({
    name: 'cp19',
    deps: ['ss17', 'ss18'],
    from: ['beta', 'delta'],
    to: 'alpha',
    useSnapshot: true
  });
  // snapshot alpha before copy
  await ss({
    name: 'ss19',
    deps: ['cp19'],
    node: 'alpha'
  });
  await cm({
    name: 'cp20',
    deps: ['ss18', 'ss19'],
    from: ['alpha', 'delta'],
    to: 'gamma',
    useSnapshot: true
  });
  // snapshot gamma before copy
  await ss({
    name: 'ss20',
    deps: ['cp20'],
    node: 'gamma'
  });
  await cm({
    name: 'cp21',
    deps: ['ss20'],
    from: 'gamma',
    to: 'beta',
    useSnapshot: true
  });
  // snapshot beta before copy
  await ss({
    name: 'ss21',
    deps: ['cp21'],
    node: 'beta'
  });
  await cm({
    name: 'cp22',
    deps: ['ss20', 'ss21'],
    from: ['beta', 'gamma'],
    to: 'alpha',
    useSnapshot: true
  });
  await cm({
    name: 'cp23',
    deps: ['ss20', 'ss21'],
    from: ['beta', 'gamma'],
    to: 'delta',
    useSnapshot: true
  });
  await cm({
    name: 'cp24',
    deps: ['cp23'],
    from: 'delta',
    to: 'gamma'
  });
  return results;
};
