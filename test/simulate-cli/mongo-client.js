/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {MongoClient} = require('mongodb');

const mongoUrl = process.env.MONGODB_URL;

module.exports.send = async function({payload}) {
  if(!mongoUrl) {
    console.log(
      '"process.env.MONGODB_URL" not defined, skipping send to mongo.');
    return;
  }
  const {client, collection} = await _initMongo();
  await collection.insert(payload);
  await _closeMongo({client});
};

async function _closeMongo({client}) {
  await client.close();
}

// eslint-disable-next-line no-unused-vars
async function _initMongo() {
  const client = new MongoClient(mongoUrl, {useUnifiedTopology: true});
  await client.connect();
  const db = client.db();
  const collection = db.collection('hackathon_reports');
  // TODO: indexes?
  return {client, collection, db};
}
