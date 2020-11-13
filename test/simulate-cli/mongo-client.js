/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';

const {MongoClient} = require('mongodb');

const mongoUrl = process.env.MONGODB_URL;

if(!mongoUrl) {
  // throw new Error('You must setup "process.env.MONGODB_URL".');
}

module.exports.send = async function({payload}) {
  const {collection, db} = await _initMongo2();
  await collection.insert(payload);
  await _closeMongo({db});
};

async function _closeMongo({db}) {
  await db.close();
}

async function _initMongo2() {
  const {db} = await new Promise((resolve, reject) => {
    MongoClient.connect(mongoUrl, {}, (err, db) => {
      if(err) {
        return reject(err);
      }
      resolve({db});
    });
  });
  const collection = db.collection('hackathon_reports');
  // TODO: indexes?
  return {collection, db};
}

// eslint-disable-next-line no-unused-vars
async function _initMongo3() {
  const client = new MongoClient(mongoUrl, {useUnifiedTopology: true});
  await client.connect();
  const db = client.db();
  const collection = db.collection('hackathon_reports');
  // TODO: indexes?
  return {client, collection, db};
}
