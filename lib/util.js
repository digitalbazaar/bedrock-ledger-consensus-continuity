/*!
 * Copyright (c) 2017-2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const brLedgerNode = require('bedrock-ledger-node');
const {canonize, NQuads} = require('rdf-canonize');
const jsonCanonicalize = require('canonicalize');
const crypto = require('crypto');
const multibase = require('multibase');
const multihash = require('multihashes');
const pAll = require('p-all');
const pImmediate = require('p-immediate');

exports.generateRecordId = ({ledgerNode, operation}) => {
  let recordId;
  if(operation.type === 'CreateWebLedgerRecord') {
    recordId = operation.record.id;
  }
  if(operation.type === 'UpdateWebLedgerRecord') {
    recordId = operation.recordPatch.target;
  }
  return ledgerNode.storage.driver.hash(recordId);
};

exports.rdfCanonizeAndHash = brLedgerNode.consensus._rdfCanonizeAndHash;

exports.hasher = async data => {
  if(!data) {
    throw new TypeError('The `data` parameter must be a JSON-LD document.');
  }
  // TODO: add tests that compare
  // exports.canonizeMergeEvent === exports.rdfCanonizeAndHash,
  // exports.canonizeOperationEvent === exports.rdfCanonizeAndHash
  if(data.type === 'ContinuityMergeEvent') {
    const {hash} = await exports.canonizeMergeEvent(data);
    return hash;
  }
  if(data.type === 'WebLedgerOperationEvent') {
    const {hash} = await exports.canonizeOperationEvent(data);
    return hash;
  }
  // if(data.type === 'CreateWebLedgerRecord') {
  //   const {hash} = await exports.canonizeCreateRecordOperation(data);
  //   return hash;
  // }
  const {hash} = await exports.rdfCanonizeAndHash(data);
  return hash;
};

exports.canonizeMergeEvent = async event => {
  const dataset = _mergeEventToDataset(event);
  const canonized = await canonize(dataset, {
    algorithm: 'URDNA2015',
    format: 'application/n-quads'
  });
  return _hashCanonized(canonized);
};

exports.canonizeOperationEvent = async event => {
  const {basisBlockHeight} = event;
  let canonized =
    '_:c14n0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ' +
      '<https://w3id.org/webledger#WebLedgerOperationEvent> .\n' +
    '_:c14n0 <https://w3id.org/webledger#basisBlockHeight> ' +
      `"${basisBlockHeight}"^^<http://www.w3.org/2001/XMLSchema#integer> .\n`;

  let operationHashes;
  if(Array.isArray(event.operationHash)) {
    operationHashes = event.operationHash.slice();
    operationHashes.sort();
  } else {
    operationHashes = [event.operationHash];
  }
  const opQuads = operationHashes.map(h =>
    `_:c14n0 <https://w3id.org/webledger#operationHash> "${h}" .\n`);
  canonized += opQuads.join('');

  let parentHashes;
  if(Array.isArray(event.parentHash)) {
    parentHashes = event.parentHash.slice();
    parentHashes.sort();
  } else {
    parentHashes = [event.parentHash];
  }
  const parentQuads = parentHashes.map(h =>
    `_:c14n0 <https://w3id.org/webledger#parentHash> "${h}" .\n`);
  canonized += parentQuads.join('');

  if(event.treeHash) {
    canonized +=
      `_:c14n0 <https://w3id.org/webledger#treeHash> "${event.treeHash}" .\n`;
  }

  return _hashCanonized(canonized);
};

exports.canonizeCreateRecordOperation = async operation => {
  const recordQuad = NQuads.serializeQuad({
    subject: {termType: 'BlankNode', value: '_:c14n0'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/webledger#record'
    },
    object: {
      termType: 'Literal',
      value: jsonCanonicalize(operation.record),
      datatype: {
        termType: 'NamedNode',
        value: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#JSON'
      }
    },
    graph: {termType: 'DefaultGraph', value: ''}
  });

  const canonized =
    '_:c14n0 <http://purl.org/dc/terms/creator> ' +
      `<${operation.creator}> .\n` +
    '_:c14n0 <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ' +
      '<https://w3id.org/webledger#CreateWebLedgerRecord> .\n' +
    recordQuad;
  return _hashCanonized(canonized);
};

function _hashCanonized(canonized) {
  const canonizedBuffer = Buffer.from(canonized, 'utf8');
  const canonizedBytes = canonizedBuffer.length;
  const hash = crypto.createHash('sha256').update(canonizedBuffer).digest();
  const mh = multihash.encode(hash, 'sha2-256');
  const mb = multibase.encode('base58btc', mh).toString();
  return {canonizedBytes, hash: mb};
}

function _mergeEventToDataset(event) {
  const dataset = [{
    subject: {termType: 'BlankNode', value: '_:b0'},
    predicate: {
      termType: 'NamedNode',
      value: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    },
    object: {
      termType: 'NamedNode',
      value: 'https://w3id.org/webledger#ContinuityMergeEvent'
    },
    graph: {termType: 'DefaultGraph', value: ''}
  }, {
    subject: {termType: 'BlankNode', value: '_:b0'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/security#proof'
    },
    object: {termType: 'BlankNode', value: '_:b1'},
    graph: {termType: 'DefaultGraph', value: ''}
  }];

  dataset.push({
    subject: {termType: 'BlankNode', value: '_:b0'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/webledger#basisBlockHeight'
    },
    object: {
      termType: 'Literal',
      value: '' + event.basisBlockHeight,
      datatype: {
        termType: 'NamedNode',
        value: 'http://www.w3.org/2001/XMLSchema#integer'
      }
    },
    graph: {termType: 'DefaultGraph', value: ''}
  });

  dataset.push({
    subject: {termType: 'BlankNode', value: '_:b0'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/webledger#mergeHeight'
    },
    object: {
      termType: 'Literal',
      value: '' + event.mergeHeight,
      datatype: {
        termType: 'NamedNode',
        value: 'http://www.w3.org/2001/XMLSchema#integer'
      }
    },
    graph: {termType: 'DefaultGraph', value: ''}
  });

  const parentHashes = Array.isArray(event.parentHash) ?
    event.parentHash : [event.parentHash];
  dataset.push(...parentHashes.map(h => ({
    subject: {termType: 'BlankNode', value: '_:b0'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/webledger#parentHash'
    },
    object: {
      termType: 'Literal',
      value: h,
      datatype: {
        termType: 'NamedNode',
        value: 'http://www.w3.org/2001/XMLSchema#string'
      }
    },
    graph: {termType: 'DefaultGraph', value: ''}
  })));

  if(event.treeHash) {
    dataset.push({
      subject: {termType: 'BlankNode', value: '_:b0'},
      predicate: {
        termType: 'NamedNode',
        value: 'https://w3id.org/webledger#treeHash'
      },
      object: {
        termType: 'Literal',
        value: event.treeHash,
        datatype: {
          termType: 'NamedNode',
          value: 'http://www.w3.org/2001/XMLSchema#string'
        }
      },
      graph: {termType: 'DefaultGraph', value: ''}
    });
  }

  // proof quads
  dataset.push(...[{
    subject: {termType: 'BlankNode', value: '_:b2'},
    predicate: {
      termType: 'NamedNode',
      value: 'http://www.w3.org/1999/02/22-rdf-syntax-ns#type'
    },
    object: {
      termType: 'NamedNode',
      value: 'https://w3id.org/security#Ed25519Signature2020'
    },
    graph: {termType: 'BlankNode', value: '_:b1'}
  }, {
    subject: {termType: 'BlankNode', value: '_:b2'},
    predicate: {
      termType: 'NamedNode',
      value: 'http://purl.org/dc/terms/created'
    },
    object: {
      termType: 'Literal',
      value: event.proof.created,
      datatype: {
        termType: 'NamedNode',
        value: 'http://www.w3.org/2001/XMLSchema#dateTime'
      }
    },
    graph: {termType: 'BlankNode', value: '_:b1'}
  }, {
    subject: {termType: 'BlankNode', value: '_:b2'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/security#proofValue'
    },
    object: {
      termType: 'Literal',
      value: event.proof.proofValue,
      datatype: {
        termType: 'NamedNode',
        value: 'https://w3id.org/security#multibase'
      }
    },
    graph: {termType: 'BlankNode', value: '_:b1'}
  }, {
    subject: {termType: 'BlankNode', value: '_:b2'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/security#proofPurpose'
    },
    object: {
      termType: 'NamedNode',
      value: 'https://w3id.org/security#assertionMethod'
    },
    graph: {termType: 'BlankNode', value: '_:b1'}
  }, {
    subject: {termType: 'BlankNode', value: '_:b2'},
    predicate: {
      termType: 'NamedNode',
      value: 'https://w3id.org/security#verificationMethod'
    },
    object: {
      termType: 'NamedNode',
      value: event.proof.verificationMethod
    },
    graph: {termType: 'BlankNode', value: '_:b1'}
  }]);

  return dataset;
}

/**
 * Lexicographically sorts an array of operation records by
 * `meta.operationHash`. The given array of operations is mutated.
 *
 * @param operations the array of operations to sort by operation hash.
 */
exports.sortOperations = operations => {
  operations.sort(_compareOperationHashes);
};

exports.processChunked = async function(
  {tasks, fn, concurrency, chunkSize, args = []}) {
  const finishedTasks = [];
  let batch = [];

  for(const task of tasks) {
    batch.push(() => fn(task, ...args));
    if(batch.length === chunkSize) {
      const ops = await pAll(batch, {concurrency});
      finishedTasks.push(...ops);
      batch = [];
      await pImmediate();
    }
  }

  if(batch.length > 0) {
    const ops = await pAll(batch, {concurrency});
    finishedTasks.push(...ops);
  }

  return finishedTasks;
};

function _compareOperationHashes(a, b) {
  return (a.meta.operationHash < b.meta.operationHash ? -1 :
    (a.meta.operationHash > b.meta.operationHash ? 1 : 0));
}
