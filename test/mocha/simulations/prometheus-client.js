/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {httpClient} = require('@digitalbazaar/http-client');
const http = require('http');

const agent = new http.Agent({keepAlive: true});

const api = {};
module.exports = api;

api.send = send;

const PROMETHEUS_METRIC_NAMES = new Map([
  ['totalTicks', 'total_ticks'],
  ['consensusDuration', 'avg_consensus_duration'],
  ['totalMergeEvents', 'avg_total_merge_events'],
  ['totalMergeEventsCreated', 'avg_total_merge_events_created'],
  ['gossipSessions', 'total_gossip_sessions'],
]);

async function send({report}) {
  // implement
  const metrics = createMetrics({report});
  const job = 'continuity_simulation';
  const headers = {'content-type': 'text/plain'};
  await httpClient.post(`http://104.131.25.239:9091/metrics/job/${job}`, {
    agent, body: metrics, headers
  });
}

function createMetrics({report}) {
  const labelKeys = ['id', 'name', 'run', 'creator', 'timestamp'];
  const label = labelKeys.map(key => `${key}="${report[key]}"`).join(', ');

  const metrics = ['totalTicks', 'average', 'gossipSessions'];
  let prometheusData = '';

  const name = report.name.replace(/\-|\./g, '_');
  const {run, timestamp} = report;
  const suffix = `${run}_${timestamp}_${name}`;

  metrics.forEach(key => {
    if(key === 'totalTicks') {
      const metricName = PROMETHEUS_METRIC_NAMES.get(key);
      prometheusData += `${metricName}_${suffix}{${label}} ${report[key]}\n`;
    } else if(key === 'average') {
      Object.keys(report[key]).forEach(k => {
        const metricName = PROMETHEUS_METRIC_NAMES.get(k);
        prometheusData +=
          `${metricName}_${suffix}{${label}} ${report[key][k]}\n`;
      });
    }
  });

  console.log({prometheusData});

  return prometheusData;
}
