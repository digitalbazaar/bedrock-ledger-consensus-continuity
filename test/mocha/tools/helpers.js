/*
 * Copyright (c) 2020 Digital Bazaar, Inc. All rights reserved.
 */
'use strict';
const crypto = require('crypto');
const util = require('util');
const randomBytes = util.promisify(crypto.randomBytes);

const api = {};
module.exports = api;

api.strfy = a => JSON.stringify(a, null, 2);

api.generateId = generateId;
api.getTimer = getTimer;

async function generateId({encoding = 'base64', bytes = 8} = {}) {
  const buffer = await randomBytes(bytes);
  return buffer.toString(encoding);
}

function getTimer() {
  const NS_PER_SEC = 1000000000;
  const NS_PER_MS = 1000000;
  const time = process.hrtime();

  return {
    elapsed() {
      const [seconds, nanoseconds] = process.hrtime(time);
      return (seconds * NS_PER_SEC + nanoseconds) / NS_PER_MS;
    }
  };
}
