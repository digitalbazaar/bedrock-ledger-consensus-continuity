/*
 * Copyright (c) 2017-2018 Digital Bazaar, Inc. All rights reserved.
 */
console.error('FIXME: update to newer helper tools');
process.exit(1);

const async = require('async');
const bedrock = require('bedrock');
//const brDidClient = require('bedrock-did-client');
const config = require('bedrock').config;
const database = require('bedrock-mongodb');
const fs = require('fs');
//require('bedrock-ledger-consensus-continuity');
require('bedrock-ledger-storage-mongodb');

var logger = bedrock.loggers.get('app').child('viz');

config.mongodb.name = 'bedrock_ledger_continuity_test';

/*
bedrock.events.on('bedrock.init', () => {
  const jsonld = bedrock.jsonld;
  const mockData = require('./mocha/mock.data');

  const oldLoader = jsonld.documentLoader;

  jsonld.documentLoader = function(url, callback) {
    if(Object.keys(mockData.ldDocuments).includes(url)) {
      return callback(null, {
        contextUrl: null,
        document: mockData.ldDocuments[url],
        documentUrl: url
      });
    }
    oldLoader(url, callback);
  };
  // override jsonld.documentLoader in brDidClient so this document loader
  // can be used for did: and https: URLs
  brDidClient.jsonld.documentLoader = jsonld.documentLoader;
});
*/

bedrock.events.on('bedrock-cli.init', () => {
  console.log('CLI INIT');
  bedrock.program
    .option('--viz-node <ID>', 'Visualization node ID')
    .option('--viz-dump <filename>', 'Visualization data filename')
    .option('--viz-raw', 'Console log raw events')
    .option('--viz-tail', 'Only process events with consensus')
    .option('--viz-stats', 'Show node stats')
    .option('--viz-merge', 'Show merge events only')
});

bedrock.events.on('bedrock-cli.ready', () => {
  if(bedrock.program.vizNode) {
    logger.info('Visualization dump', {
      node: bedrock.program.vizNode,
      dump: bedrock.program.vizDump || '<stdout>',
      tail: !!bedrock.program.vizTail,
      stats: !!bedrock.program.vizStats
    });
  }
});

// open some collections once the database is ready
bedrock.events.on('bedrock-mongodb.ready', function(callback) {
  if(!bedrock.program.vizNode) {
    return callback();
  }
  let collectionId = bedrock.program.vizNode;
  if(!collectionId.endsWith('-event')) {
    collectionId = collectionId + '-event';
  }

  async.auto({
    collection: (callback) => {
      database.openCollections([collectionId], (err) => {
        if(err) {
          return callback(err);
        }
        callback(null, database.collections[collectionId]);
      });
    },
    count: ['collection', (results, callback) => {
      results.collection.count((err, result) => {
        if(err) {
          return callback(err);
        }
        logger.info('count', {count: result});
        callback(null, result);
      });
    }],
    countConsensus: ['collection', (results, callback) => {
      const filter = {'meta.consensus': null};
      results.collection.count(filter, (err, result) => {
        if(err) {
          return callback(err);
        }
        logger.info('count consensus', {countConsensus: result});
        callback(null, result);
      });
    }],
    events: ['collection', (results, callback) => {
      if(bedrock.program.vizStats) {
        return callback();
      }
      const filter = {};
      if(bedrock.program.vizTail) {
        filter['meta.consensus'] = null
      }
      results.collection.find(filter).toArray((err, result) => {
        if(err) {
          return callback(err);
        }
        //logger.info('events', {events: result});
        callback(null, result);
      });
    }],
    dump: ['events', (results, callback) => {
      if(bedrock.program.vizRaw) {
        const data = JSON.stringify(results.events, null, 2);
        //console.log(data);
        results.events.forEach(e => {
          if(!('meta' in e)) {
            console.log('NOMETA', e);
          }
          if(!('continuity2017' in e.meta)) {
            console.log('NOMETA', e);
          }
        });
        return callback();
      }
      const data = {};

      // find node ids
      const nodeIds = new Set(results.events.filter(e => {
        return 'continuity2017' in e.meta &&
          'creator' in e.meta.continuity2017;
      }).map(e => e.meta.continuity2017.creator));

      logger.info('nodes', {size: nodeIds.size, ids: [...nodeIds]})

      // setup viz groups
      data.groups = [...nodeIds.values()].map(n => ({
        id: n,
        label: n
      }));
      data.groups.push({
        id: 'UNKNOWN',
        label: 'UNKNOWN'
      });

      //console.log('VG', data.groups.length, data.groups);

      // find event ids
      const eventIds = new Set(results.events.map(e => e.meta.eventHash));

      // find event node map
      const eventHashMap = {};
      results.events.forEach(e => eventHashMap[e.meta.eventHash] = e);

      // setup viz nodes and group map
      /*
      data.nodes = results.events.map(e => ({
        id: e.meta.eventHash,
        group: e.meta['continuity2017'].creator
      }));
      */
      // map of event id to node structure
      const nodeMap = {};
      data.nodes = results.events.map(e => {
        const n = {
          id: e.meta.eventHash,
          consensus: !!e.meta.consensus,
          merge: bedrock.jsonld.hasValue(
            e.event, 'type', 'ContinuityMergeEvent')
        };
        if('continuity2017' in e.meta && 'creator' in e.meta.continuity2017) {
          n.group = e.meta['continuity2017'].creator;
        } else if(e.event.parentHash && e.event.parentHash.length === 1) {
          const pEvent = eventHashMap[e.event.parentHash[0]];
          if(pEvent) {
            n.group = pEvent.meta['continuity2017'].creator;
          } else {
            n.group = 'UNKNOWN';
          }
        } else {
          logger.error('No group', {event: e});
          n.group = 'UNKNOWN';
        }
        nodeMap[e.meta.eventHash] = n;
        return n;
      });
      if(bedrock.program.vizMerge) {
        data.nodes = data.nodes.filter(n => n.merge);
      }

      //console.log('VN', data.nodes.length, data.nodes);
      logger.debug('Nodes', {nodes: data.nodes});

      // setup viz links
      data.links = results.events.filter(e => {
        return e.event.parentHash;
      }).map(e => {
        return e.event.parentHash.map(p => {
          if(bedrock.program.vizTail && !eventIds.has(p)) {
            return null;
          }
          const link = {
            source: p,
            target: e.meta.eventHash
          };
          if(nodeMap[p].group === nodeMap[e.meta.eventHash].group) {
            link.value = 10;
          } else {
            link.value = 1;
          }
          return link;
        }).filter(link => link);
      }).reduce((acc, cur) => acc.concat(cur), []);
      if(bedrock.program.vizMerge) {
        data.links = data.links.filter(link => {
          return nodeMap[link.source].merge && nodeMap[link.target].merge;
        });
      }

      // link check
      data.links.forEach(link => {
        if(!eventIds.has(link.source)) {
          const e = results.events.filter(e => e.meta.eventHash === link.source);
          console.log('link source not found', link, e);
        }
        if(!eventIds.has(link.target)) {
          const e = results.events.filter(e => e.meta.eventHash === link.target);
          console.log('link target not found', link, e);
        }
      });
      //console.log('VL', data.links.length, data.links);
      //process.exit();

      if(bedrock.program.vizDump) {
        const dump = JSON.stringify(data, null, 2);
        if(bedrock.program.vizDump === '-') {
          console.log(dump);
          callback();
        } else {
          fs.writeFile(bedrock.program.vizDump, dump, (err) => {
            callback(err);
          });
        }
      }
    }]
  }, (err, results) => {
    if(err) {
      return callback(err);
    }
    if(bedrock.program.vizStats) {
      console.log({
        id: collectionId,
        count: results.count,
        countConsensus: results.countConsensus
      });
    }
    // FIXME
    // ungraceful exit
    process.exit();
  });
});

//require('bedrock-test');
bedrock.start();
