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

const PROMETHEUS_NAMESPACE = 'simulation';
const PROMETHEUS_JOB = 'continuity_simulation';

/* eslint-disable max-len */
const PROMETHEUS_METRIC_NAMES = new Map([
  ['nodeCount', `${PROMETHEUS_NAMESPACE}_node_count`],
  ['witnessCount', `${PROMETHEUS_NAMESPACE}_witness_count`],
  ['totalTimeSlices', `${PROMETHEUS_NAMESPACE}_time_slices_total`],
  ['consensusDuration', `${PROMETHEUS_NAMESPACE}_avg_consensus_duration_milliseconds`],
  ['totalMergeEvents', `${PROMETHEUS_NAMESPACE}_avg_merge_events_total`],
  ['totalMergeEventsCreated', `${PROMETHEUS_NAMESPACE}_avg_merge_events_created_total`],
  ['gossipSessions', `${PROMETHEUS_NAMESPACE}_gossip_sessions_total`],
]);
/* eslint-enable max-len */

async function send({report}) {
  // implement
  const metrics = createMetrics({report});
  const job = PROMETHEUS_JOB;
  const headers = {'content-type': 'text/plain'};
  await httpClient.post(`http://104.131.25.239:9091/metrics/job/${job}`, {
    agent, body: metrics, headers
  });
}

function createMetrics({report}) {
  const labelKeys = [
    'id', 'name', 'run', 'creator', 'timestamp', 'witnessCount', 'nodeCount'
  ];
  const label = labelKeys.map(key => {
    if(key === 'witnessCount' || key === 'nodeCount') {
      return `${key}="${report[key].toString().padStart(3, 0)}"`;
    }
    return `${key}="${report[key]}"`;
  }).join(', ');

  let prometheusData = '';

  const name = report.name.replace(/\-|\./g, '_');
  const {run, timestamp} = report;
  const suffix = `${run}_${timestamp}_${name}`;

  prometheusData += _generateTimeSlicesMetric({report, label, suffix});
  prometheusData += _generateAveragesMetric({report, label, suffix});

  // TODO: Capture gossip session data
  return prometheusData;
}

function _generateTimeSlicesMetric({report, label, suffix}) {
  const reportKey = 'totalTimeSlices';
  let metric = '';

  const metricName = PROMETHEUS_METRIC_NAMES.get(reportKey);
  metric += `${metricName}_${suffix}{${label}} ${report[reportKey]}\n`;

  return metric;
}

function _generateAveragesMetric({report, label, suffix}) {
  const reportKey = 'average';
  let metric = '';

  const averagesReport = report[reportKey];
  for(const [k, v] of Object.entries(averagesReport)) {
    const metricName = PROMETHEUS_METRIC_NAMES.get(k);
    metric += `${metricName}_${suffix}{${label}} ${v}\n`;
  }

  return metric;
}
