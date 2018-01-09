/*
 * Copyright (c) 2017 Digital Bazaar, Inc. All rights reserved.
 */
const async = require('async');
const helpers = require('./helpers');

const api = {};
module.exports = api;

// add events on alpha and beta, gossip from beta to alpha
api.alpha = (
  {consensusApi, eventTemplate, nodes, peers, previousResult}, callback) => {
  async.auto({
    betaAddEvent1: callback => helpers.addEventAndMerge(
      {consensusApi, eventTemplate, ledgerNode: nodes.beta}, callback),
    // add event on beta
    alphaAddEvent1: ['betaAddEvent1', (results, callback) =>
      helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.alpha}, callback)],
    // beta gossips with alpha
    betaGossip1: ['alphaAddEvent1', (results, callback) =>
      consensusApi._worker._gossipWith(
        {ledgerNode: nodes.beta, peerId: peers.alpha}, (err, result) => {
          assertNoError(err);
          // alpha knows about the merge event added to alpha during this cycle
          result.peerHistory.creatorHeads[peers.alpha]
            .should.equal(results.alphaAddEvent1.mergeHash);
          // one new merge event event available from alpha
          result.peerHistory.history.should.have.length(1);
          result.peerHistory.history.should.have.same.members(
            [results.alphaAddEvent1.mergeHash]);
          // beta wants to send the regular and merge event added this cycle
          result.partitionHistory.history.should.have.length(2);
          result.partitionHistory.history.should.have.same.members([
            ...results.betaAddEvent1.allHashes
          ]);
          if(previousResult) {
            // alpha only knowas about beta merge event added and gossiped
            // last cycle
            result.peerHistory.creatorHeads[peers.beta]
              .should.equal(previousResult.betaAddEvent1.mergeHash);
          }
          callback();
        })],
  }, callback);
};

// add events on alpha and beta, gossip from beta to alpha
api.beta = (
  {consensusApi, eventTemplate, nodes, peers, previousResult}, callback) => {
  async.auto({
    gammaAddEvent1: callback => helpers.addEventAndMerge(
      {consensusApi, eventTemplate, ledgerNode: nodes.gamma}, callback),
    // gamma gossips with beta
    gammaGossip1: ['gammaAddEvent1', (results, callback) =>
      consensusApi._worker._gossipWith(
        {ledgerNode: nodes.gamma, peerId: peers.beta}, callback)],
    // beta adds and event and merges which includes events from gamma
    betaAddEvent1: ['gammaGossip1', (results, callback) =>
      helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.beta}, callback)],
    // add event on beta
    alphaAddEvent1: ['betaAddEvent1', (results, callback) =>
      helpers.addEventAndMerge(
        {consensusApi, eventTemplate, ledgerNode: nodes.alpha}, callback)],
    // beta gossips with alpha
    betaGossip1: ['alphaAddEvent1', (results, callback) =>
      consensusApi._worker._gossipWith(
        {ledgerNode: nodes.beta, peerId: peers.alpha}, (err, result) => {
          assertNoError(err);
          // console.log('ALPHA', peers.alpha);
          // console.log('AAAAAA', nodes.alpha.storage.events.collection.s.name);
          // alpha knows about the merge event added to alpha during this cycle
          result.peerHistory.creatorHeads[peers.alpha]
            .should.equal(results.alphaAddEvent1.mergeHash);
          // one new merge event event available from alpha
          result.peerHistory.history.should.have.length(1);
          result.peerHistory.history.should.have.same.members(
            [results.alphaAddEvent1.mergeHash]);

          if(previousResult) {
            // alpha only knowas about beta merge event added and gossiped
            // last cycle
            result.peerHistory.creatorHeads[peers.beta]
              .should.equal(previousResult.betaAddEvent1.mergeHash);
            result.peerHistory.creatorHeads[peers.gamma]
              .should.equal(previousResult.gammaAddEvent1.mergeHash);
            // console.log('YYYYYYYYY', JSON.stringify(previousResult.gammaAddEvent1.allHashes, null, 2));
          }

          // beta wants to send the regular and merge event added this cycle
          // console.log('XXXXXXXXX', JSON.stringify(result.partitionHistory.history, null, 2));
          // console.log('BETA', peers.beta);
          result.partitionHistory.history.should.have.length(4);
          // NOTE: checking for exact order of these events
          result.partitionHistory.history.should.deep.equal([
            results.gammaAddEvent1.regularHashes[0],
            results.gammaAddEvent1.mergeHash,
            results.betaAddEvent1.regularHashes[0],
            results.betaAddEvent1.mergeHash
          ]);

          callback();
        })],
  }, callback);
};
